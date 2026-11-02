#!/usr/bin/env npx ts-node
/**
 * calculate.ts
 *
 * Keyspaces Pricing Calculator — manual inputs mode.
 * Delegates all pricing logic to PricingFormulas.ts.
 *
 * Usage:
 *   npx ts-node --require tsconfig-paths/register --project tsconfig.scripts.json \
 *     scripts/calculate.ts <region> <reads/s> <writes/s> <rowSizeBytes> <storageGB> [ttl/s] [pitr-legacy] \
 *     [--dc-count N] [--reads-override 'dc-1=100,dc-2=50'] [--pitr | --no-pitr] [--pitr-override 'dc-1=on,dc-2=off']
 *     [--rows-per-read N] [--explain]
 *
 * Reads and PITR can differ per Region (--reads-override, --pitr-override); writes, TTL and storage are replicated.
 *
 * --explain adds `formulas`: every pricing formula with this estimate's numbers substituted.
 *
 * With --dc-count N > 1: names dc-1..dc-N in the same region; inputs are per-datacenter workload.
 * Default PITR/backups: on for each DC when N > 1 unless --no-pitr; single-DC default off unless --pitr or legacy pitr arg.
 */

import {
  calculatePricingEstimate,
  explainPricingEstimate,
  calculateWriteUnitsPerOperation,
  calculateReadUnitsPerOperation,
  calculateTtlUnitsPerOperation,
  applyPerDcReadOverridesToEstimateResults,
  type DatacenterRef,
  type EstimateResults,
  type KeyspaceAggregate,
  type KeyspaceCostEntry,
  type PricingEstimateResult,
} from 'src/calculator/PricingFormulas';

import { parseReadsOverrideCsv, parsePitrOverrideCsv } from 'src/calculator/ParsingHelpers';

const regionsMap: Record<string, string> = require('src/calculator/data/regions.json');

interface ParsedCli {
  positional: string[];
  dcCount: number;
  readsOverride: string | null;
  pitrUser: boolean | null;
  rowsPerRead: number;
  explain: boolean;
  pitrOverride: string | null;
}

function parseCalculateArgs(argv: string[]): ParsedCli {
  const positional: string[] = [];
  let dcCount = 1;
  let readsOverride: string | null = null;
  let pitrUser: boolean | null = null;
  let rowsPerRead = 1;
  let explain = false;
  let pitrOverride: string | null = null;
  let i = 0;
  while (i < argv.length) {
    const a = argv[i];
    if (a === '--dc-count') {
      const n = parseInt(argv[++i], 10);
      dcCount = Number.isFinite(n) && n >= 1 ? n : 1;
    } else if (a === '--reads-override') {
      readsOverride = argv[++i];
    } else if (a === '--pitr') {
      pitrUser = true;
    } else if (a === '--no-pitr') {
      pitrUser = false;
    } else if (a === '--pitr-override') {
      pitrOverride = argv[++i];
    } else if (a === '--explain') {
      explain = true;
    } else if (a === '--rows-per-read') {
      const n = Number(argv[++i]);
      rowsPerRead = Number.isFinite(n) && n > 0 ? n : 1;
    } else if (!a.startsWith('-')) {
      positional.push(a);
    }
    i++;
  }
  return { positional, dcCount, readsOverride, pitrUser, rowsPerRead, explain, pitrOverride };
}

function sumDefaultKeyspace(pricing: PricingEstimateResult, datacenters: DatacenterRef[]): KeyspaceCostEntry | null {
  let acc: KeyspaceCostEntry | null = null;
  for (const d of datacenters) {
    const dc = pricing.total_datacenter_cost[d.name];
    if (!dc?.keyspaceCosts?.default) continue;
    const k = dc.keyspaceCosts['default'];
    if (!acc) {
      acc = { ...k };
      continue;
    }
    acc.reads_on_demand += k.reads_on_demand;
    acc.reads_on_demand_savings += k.reads_on_demand_savings;
    acc.reads_provisioned += k.reads_provisioned;
    acc.reads_provisioned_savings += k.reads_provisioned_savings;
    acc.writes_on_demand += k.writes_on_demand;
    acc.writes_on_demand_savings += k.writes_on_demand_savings;
    acc.writes_provisioned += k.writes_provisioned;
    acc.writes_provisioned_savings += k.writes_provisioned_savings;
    acc.storage += k.storage;
    acc.backup += k.backup;
    acc.ttlDeletes += k.ttlDeletes;
    acc.provisioned_total += k.provisioned_total;
    acc.on_demand_total += k.on_demand_total;
    acc.provisioned_total_savings += k.provisioned_total_savings;
    acc.on_demand_total_savings += k.on_demand_total_savings;
  }
  return acc;
}

// ─── Main ─────────────────────────────────────────────────────────────────────

function main() {
  const { positional, dcCount, readsOverride, pitrUser, rowsPerRead, explain, pitrOverride } = parseCalculateArgs(process.argv.slice(2));
  if (positional.length < 5) {
    console.error(
      'Usage: calculate.ts <region> <reads/s> <writes/s> <rowSizeBytes> <storageGB> [ttl/s] [pitr true|false] [--dc-count N] [--reads-override dc-1=1,dc-2=2] [--pitr | --no-pitr] [--rows-per-read N]'
    );
    process.exit(1);
  }

  const [regionArg, readsArg, writesArg, rowSizeArg, storageArg] = positional;
  const ttlArg = positional[5] ?? '0';
  const legacyPitrStr = positional[6];

  const longRegion = regionsMap[regionArg] ?? regionArg;
  const reads_per_second = Number(readsArg);
  const writes_per_second = Number(writesArg);
  const avg_row_size_bytes = Number(rowSizeArg);
  const storage_gb = Number(storageArg);
  const ttls_per_second = Number(ttlArg);

  let pitr_enabled: boolean;
  if (pitrUser !== null) {
    pitr_enabled = pitrUser;
  } else if (legacyPitrStr !== undefined) {
    pitr_enabled = legacyPitrStr === 'true';
  } else if (dcCount > 1) {
    pitr_enabled = true;
  } else {
    pitr_enabled = false;
  }

  const datacenters: DatacenterRef[] = [];
  const regions: Record<string, string> = {};
  const estimateResults: EstimateResults = {};

  for (let i = 1; i <= dcCount; i++) {
    const name = dcCount === 1 ? regionArg : `dc-${i}`;
    datacenters.push({ name, nodeCount: 0 });
    regions[name] = longRegion;
    const agg: KeyspaceAggregate = {
      keyspace_name: 'default',
      keyspace_type: 'user',
      replication_factor: 3,
      total_live_space_gb: storage_gb,
      uncompressed_single_replica_gb: storage_gb,
      avg_read_row_size_bytes: avg_row_size_bytes,
      avg_write_row_size_bytes: avg_row_size_bytes,
      reads_per_second,
      writes_per_second,
      ttls_per_second,
      use_backup: pitr_enabled,
      avg_rows_per_read: rowsPerRead,
    };
    estimateResults[name] = { default: agg };
  }

  if (readsOverride) {
    const ov = parseReadsOverrideCsv(readsOverride);
    if (ov.size > 0) applyPerDcReadOverridesToEstimateResults(estimateResults, ov);
  }
  // PITR can differ per Region: --pitr-override 'dc-1=on,dc-2=off' wins over the default above.
  if (pitrOverride) {
    for (const [dc, on] of parsePitrOverrideCsv(pitrOverride)) {
      if (!estimateResults[dc]) {
        console.error(`Warning: --pitr-override names unknown datacenter "${dc}" (expected ${Object.keys(estimateResults).join(', ')})`);
        continue;
      }
      estimateResults[dc].default.use_backup = on;
    }
  }

  const pricing: PricingEstimateResult | null = calculatePricingEstimate(datacenters, regions, estimateResults);
  if (!pricing) {
    console.error(`Region not found: ${longRegion}`);
    process.exit(1);
  }

  const kc = sumDefaultKeyspace(pricing, datacenters);
  if (!kc) {
    console.error('No pricing rows for default keyspace.');
    process.exit(1);
  }

  const odTotal = pricing.total_monthly_on_demand_cost;
  const provTotal = pricing.total_monthly_provisioned_cost;
  const odTotalSP = pricing.total_monthly_on_demand_cost_savings;
  const provTotalSP = pricing.total_monthly_provisioned_cost_savings;
  const savingsPlanAvailable = odTotalSP !== odTotal || provTotalSP !== provTotal;

  const result = {
    region: { short: regionArg, long: longRegion },
    dc_count: dcCount,
    inputs: {
      reads_per_second,
      writes_per_second,
      avg_row_size_bytes,
      storage_gb,
      ttls_per_second,
      pitr_enabled,
      pitr_by_datacenter: Object.fromEntries(
        Object.entries(estimateResults).map(([dc, ks]) => [dc, !!ks.default.use_backup])
      ),
      rows_per_read: rowsPerRead,
    },
    units_per_operation: {
      write: calculateWriteUnitsPerOperation(avg_row_size_bytes),
      read: calculateReadUnitsPerOperation(avg_row_size_bytes, rowsPerRead),
      ttl: calculateTtlUnitsPerOperation(avg_row_size_bytes),
    },
    on_demand: {
      reads_strong: kc.reads_on_demand,
      reads_eventual: kc.reads_on_demand / 2,
      writes: kc.writes_on_demand,
      ttl_deletes: kc.ttlDeletes,
      storage: kc.storage,
      backup: kc.backup,
      total: odTotal,
    },
    provisioned: {
      reads_strong: kc.reads_provisioned,
      reads_eventual: kc.reads_provisioned / 2,
      writes: kc.writes_provisioned,
      ttl_deletes: kc.ttlDeletes,
      storage: kc.storage,
      backup: kc.backup,
      total: provTotal,
    },
    savings_plan_available: savingsPlanAvailable,
    on_demand_savings_plan: savingsPlanAvailable
      ? {
          reads_strong: kc.reads_on_demand_savings,
          reads_eventual: kc.reads_on_demand_savings / 2,
          writes: kc.writes_on_demand_savings,
          ttl_deletes: kc.ttlDeletes,
          storage: kc.storage,
          backup: kc.backup,
          total: odTotalSP,
        }
      : null,
    provisioned_savings_plan: savingsPlanAvailable
      ? {
          reads_strong: kc.reads_provisioned_savings,
          reads_eventual: kc.reads_provisioned_savings / 2,
          writes: kc.writes_provisioned_savings,
          ttl_deletes: kc.ttlDeletes,
          storage: kc.storage,
          backup: kc.backup,
          total: provTotalSP,
        }
      : null,
    ...(explain ? { formulas: explainPricingEstimate(datacenters, regions, estimateResults) } : {}),
    report_data: {
      datacenters,
      regions,
      estimateResults,
      pricing,
    },
  };

  console.log(JSON.stringify(result, null, 2));
}

main();
