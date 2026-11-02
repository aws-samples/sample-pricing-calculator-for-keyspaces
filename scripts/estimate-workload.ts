#!/usr/bin/env npx ts-node
/**
 * estimate-workload.ts
 *
 * Prices a source-independent workload spec: tables from any database, monitoring data, or a proposed
 * data model (e.g. one SQL-to-Keyspaces modeling option). Converts each table to the Keyspaces workload
 * (keyspaces-pricing-rules.md, Part 3), then prices it with the same code as calculate.ts.
 * Output has the same shape as parse-cassandra.ts, so generate-pdf.ts and generate-cfn.ts --estimate accept it.
 *
 * Usage:
 *   npx ts-node --require tsconfig-paths/register --project tsconfig.scripts.json \
 *     scripts/estimate-workload.ts --spec workload.json [--explain]
 *   cat workload.json | ... scripts/estimate-workload.ts [--explain]
 *
 * Spec:
 *   {
 *     "source": "postgres",                     // free text, shown in the output
 *     "regions": ["us-east-1", {"name": "eu-west-1", "pitr": false}],  // every keyspace is replicated to every
 *                                               // Region; an object entry can set PITR for that Region
 *     "pitr": true,                             // optional; default: on when more than one Region
 *     "keyspaces": [{
 *       "name": "shop",
 *       "pitr": false,                          // optional per-keyspace override
 *       "tables": [{
 *         "name": "orders",
 *         "reads_per_second": 120, "writes_per_second": 40,
 *         "reads_per_second_by_region": {"eu-west-1": 30},  // optional: reads differ per Region
 *         "avg_row_bytes": 512,                 // or "columns": ["UUID", "VARCHAR(100)", "TEXT"] (summed); default 1024
 *         "rows_per_read": 1,                   // default 1
 *         "ttl_deletes_per_second": 0,          // default 0
 *         "rows": 250000000,                    // storage = rows x avg_row_bytes ...
 *         "stored_bytes": 0, "compression_ratio": 1, "storage_copies": 1,  // ... or stored size
 *         "request_copies": 1                   // divide reads/writes when metrics count replica operations
 *       }]
 *     }]
 *   }
 */

import fs from 'fs';

import {
  calculatePricingEstimate,
  explainPricingEstimate,
  aggregatePricingTotals,
  type DatacenterRef,
  type EstimateResults,
  type KeyspaceAggregate,
} from 'src/calculator/PricingFormulas';
import { estimateColumnBytes } from 'src/calculator/ParsingHelpers';

const regionsMap: Record<string, string> = require('src/calculator/data/regions.json');

const GIGABYTE = 1024 ** 3;
const DEFAULT_ROW_BYTES = 1024;

interface TableSpec {
  name: string;
  reads_per_second?: number;
  reads_per_second_by_region?: Record<string, number>;
  writes_per_second?: number;
  avg_row_bytes?: number;
  /** Column types (SQL or CQL); the row size is their estimated sum when avg_row_bytes isn't given. */
  columns?: string[];
  rows_per_read?: number;
  ttl_deletes_per_second?: number;
  rows?: number;
  stored_bytes?: number;
  compression_ratio?: number;
  storage_copies?: number;
  request_copies?: number;
}

interface KeyspaceSpec {
  name: string;
  pitr?: boolean;
  tables: TableSpec[];
}

type RegionSpec = string | { name: string; pitr?: boolean };

interface WorkloadSpec {
  source?: string;
  regions?: RegionSpec[];
  region?: string;
  pitr?: boolean;
  keyspaces: KeyspaceSpec[];
}

const fail = (msg: string): never => {
  console.error(msg);
  process.exit(1);
};

const n = (v: number): string =>
  Number.isInteger(v) ? v.toLocaleString('en-US') : v.toLocaleString('en-US', { maximumFractionDigits: 4 });

const positive = (v: number | undefined, fallback: number): number =>
  typeof v === 'number' && Number.isFinite(v) && v > 0 ? v : fallback;

const nonNegative = (v: number | undefined): number =>
  typeof v === 'number' && Number.isFinite(v) && v > 0 ? v : 0;

interface ConvertedTable {
  reads: number;
  writes: number;
  ttls: number;
  rowBytes: number;
  rowsPerRead: number;
  storageGb: number;
  lines: string[];
}

/** One source table → logical Keyspaces workload in one Region (keyspaces-pricing-rules.md, 3.2). */
function convertTable(keyspace: string, t: TableSpec, region: string): ConvertedTable {
  const id = `${keyspace}.${t.name}`;
  const lines: string[] = [];
  let rowBytes: number;
  if (typeof t.avg_row_bytes === 'number' && t.avg_row_bytes > 0) {
    rowBytes = t.avg_row_bytes;
  } else if (Array.isArray(t.columns) && t.columns.length > 0) {
    const sizes = t.columns.map((c) => estimateColumnBytes(String(c)));
    rowBytes = sizes.reduce((a, b) => a + b, 0);
    lines.push(`${id}: row size = ${t.columns.map((c, k) => `${c} ${sizes[k]}`).join(' + ')} = ${n(rowBytes)} B`);
  } else {
    rowBytes = DEFAULT_ROW_BYTES;
  }
  const rowsPerRead = positive(t.rows_per_read, 1);
  const requestCopies = positive(t.request_copies, 1);

  const rate = (label: string, raw: number): number => {
    if (raw === 0 && label.startsWith('TTL')) return 0;
    if (requestCopies === 1) {
      lines.push(`${id}: ${label} = ${n(raw)}`);
      return raw;
    }
    const v = raw / requestCopies;
    lines.push(`${id}: ${label} = ${n(raw)} observed ÷ ${n(requestCopies)} request copies = ${n(v)}`);
    return v;
  };
  const regionReads = t.reads_per_second_by_region?.[region];
  const reads = rate(
    regionReads !== undefined ? `reads/s in ${region}` : 'reads/s',
    nonNegative(regionReads !== undefined ? regionReads : t.reads_per_second)
  );
  const writes = rate('writes/s', nonNegative(t.writes_per_second));
  const ttls = rate('TTL deletes/s', nonNegative(t.ttl_deletes_per_second));

  let storageGb: number;
  if (typeof t.rows === 'number' && t.rows >= 0) {
    storageGb = (t.rows * rowBytes) / GIGABYTE;
    lines.push(`${id}: storage = ${n(t.rows)} rows × ${n(rowBytes)} B ÷ 1,073,741,824 = ${n(storageGb)} GB`);
  } else if (typeof t.stored_bytes === 'number' && t.stored_bytes >= 0) {
    const ratio = positive(t.compression_ratio, 1);
    const copies = positive(t.storage_copies, 1);
    storageGb = t.stored_bytes / ratio / copies / GIGABYTE;
    lines.push(
      `${id}: storage = ${n(t.stored_bytes)} B stored ÷ ${n(ratio)} compression ÷ ${n(copies)} copies ÷ 1,073,741,824 = ${n(storageGb)} GB`
    );
  } else {
    return fail(`Table ${id}: give "rows" (with avg_row_bytes) or "stored_bytes" to size storage.`);
  }
  if (rowsPerRead !== 1) lines.push(`${id}: rows per read = ${n(rowsPerRead)}`);
  return { reads, writes, ttls, rowBytes, rowsPerRead, storageGb, lines };
}

/** Source tables of one keyspace → one workload record per Region (keyspaces-pricing-rules.md, 3.3). */
function convertKeyspace(
  ks: KeyspaceSpec,
  region: string
): { agg: Omit<KeyspaceAggregate, 'use_backup'>; lines: string[] } {
  if (!ks.name) fail('Every keyspace needs a "name".');
  if (!Array.isArray(ks.tables) || ks.tables.length === 0) fail(`Keyspace ${ks.name}: "tables" is empty.`);
  const tables = ks.tables.map((t) => convertTable(ks.name, t, region));
  const lines = tables.flatMap((t) => t.lines);

  const sum = (f: (t: ConvertedTable) => number) => tables.reduce((a, t) => a + f(t), 0);
  const reads = sum((t) => t.reads);
  const writes = sum((t) => t.writes);
  const ttls = sum((t) => t.ttls);
  const storageGb = sum((t) => t.storageGb);
  const ops = reads + writes;
  const rowBytes = ops > 0 ? sum((t) => (t.reads + t.writes) * t.rowBytes) / ops : tables[0].rowBytes;
  const rowsPerRead = reads > 0 ? sum((t) => t.reads * t.rowsPerRead) / reads : 1;

  if (tables.length > 1) {
    const list = (f: (t: ConvertedTable) => number) => tables.map((t) => n(f(t))).join(' + ');
    lines.push(
      `${ks.name}: reads/s = ${list((t) => t.reads)} = ${n(reads)}`,
      `${ks.name}: writes/s = ${list((t) => t.writes)} = ${n(writes)}`,
      `${ks.name}: storage = ${list((t) => t.storageGb)} = ${n(storageGb)} GB`,
      `${ks.name}: row size = Σ(ops × row bytes) ÷ Σ ops = ${n(rowBytes)} B (weighted by reads + writes)`
    );
    if (tables.some((t) => t.rowsPerRead !== 1)) {
      lines.push(`${ks.name}: rows per read = Σ(reads × rows per read) ÷ Σ reads = ${n(rowsPerRead)}`);
    }
  }
  if (ttls > 0) lines.push(`${ks.name}: TTL deletes/s = ${n(ttls)}`);

  return {
    agg: {
      keyspace_name: ks.name,
      keyspace_type: 'user',
      replication_factor: 3,
      total_live_space_gb: storageGb,
      uncompressed_single_replica_gb: storageGb,
      avg_read_row_size_bytes: rowBytes,
      avg_write_row_size_bytes: rowBytes,
      reads_per_second: reads,
      writes_per_second: writes,
      ttls_per_second: ttls,
      avg_rows_per_read: rowsPerRead,
    },
    lines,
  };
}

function readSpec(argv: string[]): { spec: WorkloadSpec; explain: boolean } {
  let specPath: string | null = null;
  let explain = false;
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === '--spec') specPath = argv[++i];
    else if (argv[i] === '--explain') explain = true;
  }
  let raw = '';
  try {
    raw = specPath ? fs.readFileSync(specPath, 'utf8') : !process.stdin.isTTY ? fs.readFileSync(0, 'utf8') : '';
  } catch (e: unknown) {
    fail(`Failed to read spec: ${(e as Error).message}`);
  }
  if (!raw.trim()) fail('Usage: estimate-workload.ts --spec <workload.json> [--explain]   (or pipe the spec on stdin)');
  try {
    return { spec: JSON.parse(raw) as WorkloadSpec, explain };
  } catch (e: unknown) {
    return fail(`Spec is not valid JSON: ${(e as Error).message}`);
  }
}

function main() {
  const { spec, explain } = readSpec(process.argv.slice(2));
  if (!Array.isArray(spec.keyspaces) || spec.keyspaces.length === 0) fail('Spec needs a non-empty "keyspaces" list.');
  const regionEntries = (spec.regions?.length ? spec.regions : [spec.region ?? 'us-east-1']).map((r) =>
    typeof r === 'string' ? { name: r, pitr: undefined as boolean | undefined } : { name: r?.name, pitr: r?.pitr }
  );
  const regionCodes = [...new Set(regionEntries.map((r) => r.name))];
  for (const code of regionCodes) if (!code || !regionsMap[code]) fail(`Unknown AWS Region "${code}".`);
  const regionPitr = new Map(regionEntries.filter((r) => r.pitr !== undefined).map((r) => [r.name, r.pitr as boolean]));
  for (const ks of spec.keyspaces) {
    for (const t of ks.tables ?? []) {
      for (const r of Object.keys(t.reads_per_second_by_region ?? {})) {
        if (!regionCodes.includes(r)) fail(`Table ${ks.name}.${t.name}: reads_per_second_by_region names "${r}", which isn't in "regions".`);
      }
    }
  }

  const multiRegion = regionCodes.length > 1;
  // Converted per Region: reads (and PITR) can differ by Region; writes, TTL and storage are replicated.
  const converted = Object.fromEntries(
    regionCodes.map((code) => [code, spec.keyspaces.map((ks) => ({ spec: ks, ...convertKeyspace(ks, code) }))])
  );

  // One record per keyspace per Region; a replicated keyspace carries the same workload in every Region.
  const datacenters: DatacenterRef[] = regionCodes.map((name) => ({ name, nodeCount: 0 }));
  const regions: Record<string, string> = Object.fromEntries(regionCodes.map((c) => [c, regionsMap[c]]));
  const estimateResults: EstimateResults = {};
  for (const code of regionCodes) {
    estimateResults[code] = {};
    for (const c of converted[code]) {
      // PITR precedence: keyspace pitr > Region entry pitr > spec pitr > default (on when multi-Region).
      const pitr = c.spec.pitr ?? regionPitr.get(code) ?? spec.pitr ?? multiRegion;
      estimateResults[code][c.spec.name] = { ...c.agg, use_backup: pitr };
    }
  }

  const pricing = calculatePricingEstimate(datacenters, regions, estimateResults);
  if (!pricing) fail('Failed to calculate pricing — check the Regions.');
  const t = aggregatePricingTotals(pricing!);

  let formulas: Record<string, Record<string, string[]>> | undefined;
  if (explain) {
    formulas = explainPricingEstimate(datacenters, regions, estimateResults);
    for (const [code, byKs] of Object.entries(formulas)) {
      for (const c of converted[code]) {
        if (!byKs[c.spec.name]) continue;
        const header = `Conversion (${spec.source ?? 'workload spec'}):`;
        const copy = multiRegion
          ? [`Replicated to ${regionCodes.join(', ')}: the full workload is billed in ${code}.`]
          : [];
        byKs[c.spec.name] = [header, ...c.lines, ...copy, ...byKs[c.spec.name]];
      }
    }
  }

  const result = {
    region: { short: regionCodes[0], long: regions[regionCodes[0]] },
    source: spec.source ? `workload-spec:${spec.source}` : 'workload-spec',
    datacenters,
    regions_by_datacenter: Object.fromEntries(regionCodes.map((c) => [c, { short: c, long: regions[c] }])),
    pitr_by_datacenter: Object.fromEntries(
      regionCodes.map((c) => [c, Object.values(estimateResults[c]).some((a) => a.use_backup === true)])
    ),
    on_demand: {
      reads_strong: t.reads_on_demand,
      reads_eventual: t.reads_on_demand / 2,
      writes: t.writes_on_demand,
      ttl_deletes: t.ttl_deletes,
      storage: t.storage,
      backup: t.backup,
      total: pricing!.total_monthly_on_demand_cost,
    },
    provisioned: {
      reads_strong: t.reads_provisioned,
      reads_eventual: t.reads_provisioned / 2,
      writes: t.writes_provisioned,
      ttl_deletes: t.ttl_deletes,
      storage: t.storage,
      backup: t.backup,
      total: pricing!.total_monthly_provisioned_cost,
    },
    savings_plan_available:
      pricing!.total_monthly_provisioned_cost_savings !== pricing!.total_monthly_provisioned_cost ||
      pricing!.total_monthly_on_demand_cost_savings !== pricing!.total_monthly_on_demand_cost,
    on_demand_savings_plan: { total: pricing!.total_monthly_on_demand_cost_savings },
    provisioned_savings_plan: { total: pricing!.total_monthly_provisioned_cost_savings },
    per_datacenter: pricing!.total_datacenter_cost,
    ...(formulas ? { formulas } : {}),
    report_data: { datacenters, regions, estimateResults, pricing },
  };
  console.log(JSON.stringify(result, null, 2));
}

main();
