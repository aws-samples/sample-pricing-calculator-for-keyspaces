#!/usr/bin/env npx ts-node
/**
 * parse-cassandra.ts
 *
 * Parses Cassandra diagnostic files and outputs a Keyspaces pricing estimate
 * as JSON (same shape as calculate.js), suitable for piping to generate-pdf.js.
 *
 * Imports directly from ParsingHelpers.ts and PricingFormulas.ts — no duplication.
 *
 * Usage:
 *   npx ts-node --require tsconfig-paths/register --project tsconfig.scripts.json \
 *     scripts/parse-cassandra.ts \
 *     --region    us-east-1 \
 *     --tablestats tablestats.txt \
 *     [--status   status.txt] \
 *     [--info     node1-info.txt] \
 *     [--rowsize  rowsize.txt] \
 *     [--schema   schema.cql] \
 *     [--pitr | --no-pitr] \
 *     [--reads-override 'dc1=100,dc2=50']
 *
 * Multiple --info flags accepted (one per node).
 *
 * Per-datacenter bundles (one node sampled per DC; node counts come from status):
 *   --dir <root>              root holds status.txt (+ optional schema / prepared / rowsize / tco);
 *                             each sub-directory holding a tablestats capture is one DC bundle
 *                             (tablestats + info, optional rowsize / prepared / schema).
 *   --dc-dir <dc>=<path>      explicit DC bundle directory. Repeatable; replaces sub-directory detection.
 *   --dc-region <dc>=<region> price a DC in its own Region. Repeatable, or CSV 'dc1=us-east-1,dc2=eu-west-1'.
 *
 * One sampled DC (one DC folder, or a flat --dir with one tablestats + info) is a shared bundle:
 * it applies to every DC each keyspace replicates to (per DESCRIBE SCHEMA), reads included.
 * With several DC folders, each DC uses its own; a status DC without a folder falls back, per keyspace,
 * to the first sampled DC (status order) that has the keyspace (reported in fallback_datacenters).
 * PITR defaults on whenever more than one DC is priced.
 *
 * --rows-per-read N: rows returned per read for every keyspace (default 1); read units aggregate their bytes.
 * --explain: add `formulas` with the conversion and pricing formulas, using this estimate's numbers.
 * --views ks.view1,ks.view2 | all: materialized views carried over as Keyspaces tables (default: all); unlisted
 *   views aren't priced. Reads and storage come from the view's own tablestats.
 * --view-writes own | unlogged-batch: view writes from the view's own tablestats counters (default, straight
 *   writes) or from the base table's writes (view written with its base row in an UNLOGGED BATCH).
 * --pitr-override 'dc1=on,dc2=off': PITR per datacenter (reads already differ per DC via samples/--reads-override).
 *
 * Region per DC: --dc-region, else the DC name itself when it is a Region code (e.g. us-east-1),
 * else --region.
 */

import fs from 'fs';
import path from 'path';

import {
  parseNodetoolStatus,
  parseNodetoolInfo,
  parse_nodetool_tablestats,
  parse_cassandra_schema,
  parse_cassandra_schema_compatibility,
  parse_prepared_statements,
  parseRowSizeInfo,
  parseTCOInfo,
  scanCassandraFiles,
  mergeTopologyWithStatus,
  inferCassandraBundleMode,
  parseReadsOverrideCsv,
  parsePitrOverrideCsv,
  parseMaterializedViewBases,
  estimateCqlRowBytes,
  type CompatibilityInfo,
  type QueryPatternsInfo,
  type TcoData as ParsedTcoData,
  type InferReadsMode,
} from 'src/calculator/ParsingHelpers';

import {
  buildCassandraLocalSet,
  getKeyspaceCassandraAggregate,
  calculatePricingEstimate,
  explainPricingEstimate,
  aggregatePricingTotals,
  cloneKeyspaceAggregates,
  applyUseBackupToAggregates,
  applyPerDcReadOverridesToEstimateResults,
  filterEstimateResultsBySchemaReplication,
  applyInferReadsModeToEstimateResults,
  applyMaterializedViewPolicy,
  type MaterializedViewReport,
  type ViewWriteMode,
  type Samples,
  type DatacenterRef,
  type EstimateResults,
  type KeyspaceAggregate,
  type PricingEstimateResult,
} from 'src/calculator/PricingFormulas';

import { parseTables, parseMaterializedViews } from 'src/calculator/CloudFormationGenerator';

const TCO_CLUSTER_KEY = '_cluster';

const regionsMap: Record<string, string> = require('src/calculator/data/regions.json');

// ─── CLI arg parsing ──────────────────────────────────────────────────────────

interface Args {
  region: string;
  info: string[];
  status: string | null;
  tablestats: string | null;
  rowsize: string | null;
  schema: string | null;
  prepared: string | null;
  tco: string | null;
  /** null = apply mode defaults (shared multi-DC defaults to true) */
  pitrUser: boolean | null;
  dir: string | null;
  readsOverride: string | null;
  tablestatsPathCount: number;
  /** null = mode default: `same` for a shared bundle (one sample applies everywhere), else `zero`. */
  inferReads: InferReadsMode | null;
  dcDirs: Array<{ dc: string; dir: string }>;
  dcRegions: Map<string, string>;
  /** Rows returned per read for every keyspace (tablestats counts read requests, not rows). Default 1. */
  rowsPerRead: number;
  /** Add `formulas` (conversion + pricing, with this estimate's numbers) to the output. */
  explain: boolean;
  /** Per-DC PITR: 'dc1=on,dc2=off' (wins over --pitr / --no-pitr and the default). */
  pitrOverride: string | null;
  /** Materialized views carried over as tables: 'all' (default) or a ks.view list; unlisted views aren't priced. */
  views: Set<string> | 'all';
  /** View writes: 'own' (default, the view's tablestats counters) or 'unlogged-batch' (the base table's writes). */
  viewWrites: ViewWriteMode;
}

/** Parse `name=value` pairs from a single pair or a comma-separated list. */
function parseNameValueList(raw: string): Array<[string, string]> {
  const out: Array<[string, string]> = [];
  for (const part of (raw || '').split(',')) {
    const eq = part.indexOf('=');
    if (eq <= 0) continue;
    const name = part.slice(0, eq).trim();
    const value = part.slice(eq + 1).trim();
    if (name && value) out.push([name, value]);
  }
  return out;
}

function parseArgs(argv: string[]): Args {
  const args: Args = {
    region: 'us-east-1',
    info: [],
    status: null,
    tablestats: null,
    rowsize: null,
    schema: null,
    prepared: null,
    tco: null,
    pitrUser: null,
    dir: null,
    readsOverride: null,
    tablestatsPathCount: 1,
    inferReads: null,
    dcDirs: [],
    dcRegions: new Map(),
    rowsPerRead: 1,
    explain: false,
    pitrOverride: null,
    views: 'all',
    viewWrites: 'own',
  };
  let i = 0;
  while (i < argv.length) {
    switch (argv[i]) {
      case '--region':
        args.region = argv[++i];
        break;
      case '--info':
        args.info.push(argv[++i]);
        break;
      case '--status':
        args.status = argv[++i];
        break;
      case '--tablestats':
        args.tablestats = argv[++i];
        break;
      case '--rowsize':
        args.rowsize = argv[++i];
        break;
      case '--schema':
        args.schema = argv[++i];
        break;
      case '--prepared':
        args.prepared = argv[++i];
        break;
      case '--tco':
        args.tco = argv[++i];
        break;
      case '--dc-dir':
        for (const [dc, dir] of parseNameValueList(argv[++i])) args.dcDirs.push({ dc, dir });
        break;
      case '--views': {
        const v = (argv[++i] || '').trim();
        args.views = v.toLowerCase() === 'all' ? 'all' : new Set(v.split(',').map((x) => x.trim()).filter(Boolean));
        break;
      }
      case '--view-writes': {
        const v = (argv[++i] || '').trim().toLowerCase();
        if (v === 'own' || v === 'unlogged-batch') args.viewWrites = v;
        else console.error(`Warning: --view-writes must be "own" or "unlogged-batch"; using "${args.viewWrites}"`);
        break;
      }
      case '--pitr-override':
        args.pitrOverride = argv[++i];
        break;
      case '--explain':
        args.explain = true;
        break;
      case '--rows-per-read': {
        const n = Number(argv[++i]);
        args.rowsPerRead = Number.isFinite(n) && n > 0 ? n : 1;
        break;
      }
      case '--dc-region':
        for (const [dc, region] of parseNameValueList(argv[++i])) args.dcRegions.set(dc, region);
        break;
      case '--dir':
        args.dir = argv[++i];
        break;
      case '--pitr':
        args.pitrUser = true;
        break;
      case '--no-pitr':
        args.pitrUser = false;
        break;
      case '--reads-override':
        args.readsOverride = argv[++i];
        break;
      case '--infer-reads': {
        const v = (argv[++i] || '').toLowerCase();
        if (v === 'same' || v === 'zero' || v === 'measured') args.inferReads = v;
        break;
      }
      default:
        break;
    }
    i++;
  }
  return args;
}

// Read all files in a directory and classify them using ParsingHelpers detectors.
function resolveArgsFromDir(dir: string, args: Args): void {
  const entries = fs.readdirSync(dir).filter((f) => fs.statSync(path.join(dir, f)).isFile());
  const fileMap: Record<string, string> = {};
  for (const entry of entries) {
    try {
      fileMap[path.join(dir, entry)] = fs.readFileSync(path.join(dir, entry), 'utf8');
    } catch {
      /* skip unreadable */
    }
  }
  const scan = scanCassandraFiles(fileMap);
  args.tablestatsPathCount = Math.max(1, scan.tablestats.length);
  if (!args.tablestats && scan.tablestats.length > 0) args.tablestats = scan.tablestats[0];
  if (!args.status && scan.status) args.status = scan.status;
  if (args.info.length === 0) args.info.push(...scan.info);
  if (!args.rowsize && scan.rowsize) args.rowsize = scan.rowsize;
  if (!args.schema && scan.schema) args.schema = scan.schema;
  if (!args.prepared && scan.prepared) args.prepared = scan.prepared;
  if (!args.tco && scan.tco) args.tco = scan.tco;
}

/** One sampled node's captures for a single datacenter. */
interface DcBundle {
  /** Folder / --dc-dir name; resolved to the status DC name in main(). */
  name: string;
  dir: string;
  tablestats: string;
  info: string[];
  rowsize: string | null;
  prepared: string | null;
  schema: string | null;
}

function scanDcDir(name: string, dir: string): DcBundle | null {
  const fileMap: Record<string, string> = {};
  for (const entry of fs.readdirSync(dir)) {
    const p = path.join(dir, entry);
    try {
      if (fs.statSync(p).isFile()) fileMap[p] = fs.readFileSync(p, 'utf8');
    } catch {
      /* skip unreadable */
    }
  }
  const scan = scanCassandraFiles(fileMap);
  if (scan.tablestats.length === 0) return null;
  return {
    name,
    dir,
    tablestats: scan.tablestats[0],
    info: scan.info,
    rowsize: scan.rowsize,
    prepared: scan.prepared,
    schema: scan.schema,
  };
}

/** Explicit --dc-dir bundles, else sub-directories of --dir that hold a tablestats capture. */
function resolveDcBundles(args: Args): DcBundle[] {
  const bundles: DcBundle[] = [];
  if (args.dcDirs.length > 0) {
    for (const { dc, dir } of args.dcDirs) {
      const bundle = scanDcDir(dc, dir);
      if (!bundle) {
        console.error(`No nodetool tablestats capture found for datacenter ${dc} in ${dir}`);
        process.exit(1);
      }
      bundles.push(bundle);
    }
    return bundles;
  }
  if (!args.dir) return bundles;
  for (const entry of fs.readdirSync(args.dir).sort()) {
    const p = path.join(args.dir, entry);
    if (!fs.statSync(p).isDirectory()) continue;
    const bundle = scanDcDir(entry, p);
    if (bundle) bundles.push(bundle);
  }
  return bundles;
}

/** Merge prepared-statement captures from several nodes, dropping duplicate rows. */
function mergePreparedContent(paths: string[]): string | null {
  const seen = new Set<string>();
  const lines: string[] = [];
  for (const p of paths) {
    for (const line of fs.readFileSync(p, 'utf8').split('\n')) {
      const trimmed = line.trim();
      if (!trimmed || seen.has(trimmed)) continue;
      seen.add(trimmed);
      lines.push(trimmed);
    }
  }
  return lines.length > 0 ? lines.join('\n') : null;
}

// ─── Main ─────────────────────────────────────────────────────────────────────

function main() {
  const args = parseArgs(process.argv.slice(2));

  if (args.dir) resolveArgsFromDir(args.dir, args);

  let dcBundles = resolveDcBundles(args);
  // A single sampled datacenter (one tablestats + info) is a shared bundle: it applies to every
  // datacenter the keyspace replicates to, exactly like a flat --dir with one capture.
  if (dcBundles.length === 1 && !args.tablestats) {
    const only = dcBundles[0];
    args.tablestats = only.tablestats;
    if (args.info.length === 0) args.info.push(...only.info);
    args.rowsize ??= only.rowsize;
    args.prepared ??= only.prepared;
    args.schema ??= only.schema;
    args.tablestatsPathCount = 1;
    dcBundles = [];
  }
  const perDcBundles = dcBundles.length > 0;

  if (!args.tablestats && !perDcBundles) {
    console.error(
      'Usage: parse-cassandra.ts --tablestats <file> [--status <file>] [--info <file>] [--rowsize <file>] [--schema <file>] [--region us-east-1] [--pitr | --no-pitr] [--reads-override dc1=1,dc2=2]'
    );
    console.error('       parse-cassandra.ts --dir <directory> [--region us-east-1] [--pitr | --no-pitr] ...');
    process.exit(1);
  }

  // ── Parse files using ParsingHelpers.ts ──────────────────────────────────

  const tablestats = perDcBundles ? {} : parse_nodetool_tablestats(fs.readFileSync(args.tablestats, 'utf8'));

  const statusData = args.status
    ? parseNodetoolStatus(fs.readFileSync(args.status, 'utf8'))
    : new Map<string, number>();
  const sharedRowsize = args.rowsize ?? dcBundles.find((b) => b.rowsize)?.rowsize ?? null;
  const rowSizeData = sharedRowsize ? parseRowSizeInfo(fs.readFileSync(sharedRowsize, 'utf8')) : {};

  // Per-DC bundles: key each bundle by its status DC name (info "Data Center" field, else folder name).
  const bundleByDc = new Map<string, DcBundle>();
  for (const bundle of dcBundles) {
    const infoDc = bundle.info.length > 0 ? parseNodetoolInfo(fs.readFileSync(bundle.info[0], 'utf8')).dc : '';
    let dcKey = bundle.name;
    if (infoDc && (statusData.size === 0 || statusData.has(infoDc))) dcKey = infoDc;
    else if (statusData.size > 0 && !statusData.has(bundle.name)) {
      console.error(`Warning: datacenter bundle "${bundle.name}" does not match any nodetool status datacenter`);
    }
    if (bundle.info.length === 0) {
      console.error(`Warning: datacenter ${dcKey} has no nodetool info capture; its request rates use one month of uptime`);
    }
    bundleByDc.set(dcKey, bundle);
  }
  const allInfoFiles = perDcBundles ? dcBundles.flatMap((b) => b.info) : args.info;

  // Group info files by DC
  const nodesByDc = new Map<string, Array<{ id: string; uptime_seconds: number }>>();
  if (perDcBundles) {
    for (const [dcKey, bundle] of bundleByDc.entries()) {
      const nodes = bundle.info.map((infoFile) => {
        const { id, uptime_seconds } = parseNodetoolInfo(fs.readFileSync(infoFile, 'utf8'));
        return { id, uptime_seconds };
      });
      if (nodes.length > 0) nodesByDc.set(dcKey, nodes);
    }
  } else {
    for (const infoFile of args.info) {
      const { dc, id, uptime_seconds } = parseNodetoolInfo(fs.readFileSync(infoFile, 'utf8'));
      if (!nodesByDc.has(dc)) nodesByDc.set(dc, []);
      nodesByDc.get(dc)!.push({ id, uptime_seconds });
    }
  }
  if (perDcBundles && statusData.size === 0) {
    console.error('Warning: no nodetool status capture; each datacenter is priced as a single node');
  }

  // Fall back to status DCs or placeholder if no info files
  if (nodesByDc.size === 0) {
    const dcNames = statusData.size > 0 ? [...statusData.keys()] : ['datacenter1'];
    const SECONDS_PER_MONTH = (365 / 12) * 86400;
    for (const dc of dcNames) {
      nodesByDc.set(dc, [{ id: 'node0', uptime_seconds: SECONDS_PER_MONTH }]);
    }
  }
  if (statusData.size === 0) {
    for (const [dc, nodes] of nodesByDc.entries()) statusData.set(dc, nodes.length);
  }

  if (statusData.size > 0) {
    mergeTopologyWithStatus(statusData, nodesByDc);
  }

  const dcNamesOrdered = statusData.size > 0 ? [...statusData.keys()] : [...nodesByDc.keys()];
  const bundleMode = perDcBundles ? 'perDc' : inferCassandraBundleMode(statusData, args.tablestatsPathCount);
  // Status DCs without their own bundle borrow, per keyspace, from the first sampled DC (status order).
  const fallbackSources: Record<string, Record<string, string>> = {};

  const schemaRepDc = dcNamesOrdered[0] ?? [...nodesByDc.keys()][0];
  const schemaPath = args.schema ?? dcBundles.find((b) => b.schema)?.schema ?? null;
  const schemaContent = schemaPath ? fs.readFileSync(schemaPath, 'utf8') : null;
  const schema = schemaContent ? parse_cassandra_schema(schemaContent, schemaRepDc) : null;
  const compatibility: CompatibilityInfo | null = schemaContent ? parse_cassandra_schema_compatibility(schemaContent, rowSizeData) : null;
  // system.prepared_statements is node-local, so per-DC captures are merged.
  const preparedPaths = [args.prepared, ...dcBundles.map((b) => b.prepared)].filter((p): p is string => !!p);
  const preparedContent = perDcBundles
    ? mergePreparedContent(preparedPaths)
    : args.prepared ? fs.readFileSync(args.prepared, 'utf8') : null;
  const queryPatterns: QueryPatternsInfo | null = preparedContent ? parse_prepared_statements(preparedContent) : null;
  const preparedTtlTables = queryPatterns ? new Set(Object.keys(queryPatterns.ttl_tables)) : undefined;

  // Materialized views (4.6): carried over as tables (all by default, or the --views list). Writes come from the
  // view's own counters (default) or, with --view-writes unlogged-batch, from the base table. Applied to every
  // tablestats sample before aggregation.
  const viewBases = schemaContent ? parseMaterializedViewBases(schemaContent) : new Map<string, string>();
  // view ÷ base row bytes from the schema's column types (used when row-size samples don't cover both).
  const schemaRowRatios = new Map<string, number>();
  if (schemaContent && viewBases.size > 0) {
    const baseTables = parseTables(schemaContent);
    for (const v of parseMaterializedViews(schemaContent, baseTables)) {
      const fq = `${v.keyspace}.${v.name}`;
      const base = baseTables.find((t) => `${t.keyspace}.${t.name}` === viewBases.get(fq));
      const baseBytes = base ? estimateCqlRowBytes(base.columns.map((c) => c.type)) : 0;
      if (baseBytes > 0) schemaRowRatios.set(fq, estimateCqlRowBytes(v.columns.map((c) => c.type)) / baseBytes);
    }
  }
  if (args.views !== 'all') {
    for (const v of Array.from(args.views)) {
      if (!viewBases.has(v)) console.error(`Warning: --views names "${v}", which isn't a materialized view in the schema`);
    }
  }
  let materializedViews: Record<string, MaterializedViewReport> = {};
  const applyViews = (stats: typeof tablestats, rowSize: typeof rowSizeData) => {
    if (viewBases.size === 0) return;
    materializedViews = applyMaterializedViewPolicy(
      stats, viewBases, args.views, args.viewWrites, rowSize, preparedTtlTables, schemaRowRatios
    );
  };
  applyViews(tablestats, rowSizeData);

  let tcoData: Record<string, ParsedTcoData> | null = null;
  if (args.tco) {
    try {
      const parsedTco = parseTCOInfo(fs.readFileSync(args.tco, 'utf8'));
      tcoData = { [TCO_CLUSTER_KEY]: parsedTco };
    } catch (e: unknown) {
      console.error(`Warning: could not parse TCO file (${args.tco}): ${(e as Error).message}`);
    }
  }

  const dcsWithInfoFiles = new Set<string>();
  if (perDcBundles) {
    for (const dc of nodesByDc.keys()) if (bundleByDc.has(dc)) dcsWithInfoFiles.add(dc);
  } else {
    for (const infoFile of allInfoFiles) {
      const { dc } = parseNodetoolInfo(fs.readFileSync(infoFile, 'utf8'));
      dcsWithInfoFiles.add(dc);
    }
  }

  const nodeSchema = schema
    ? Object.fromEntries(Object.entries(schema).map(([ks, v]) => [ks, { datacenters: v.datacenters }]))
    : undefined;

  const pitrExplicit = args.pitrUser;
  let usePitr: boolean;
  if (pitrExplicit !== null) {
    usePitr = pitrExplicit;
  } else if (bundleMode === 'shared' || bundleByDc.size > 1) {
    // Multi-datacenter estimates default PITR on (same rule as calculate.ts --dc-count > 1).
    usePitr = true;
  } else {
    usePitr = false;
  }

  // ── Build Samples + aggregate ─────────────────────────────────────────────

  let estimateResults: EstimateResults = {};
  const dcNames: string[] = dcNamesOrdered;

  const repDcName =
    dcNamesOrdered.find((d) => nodesByDc.has(d) && (nodesByDc.get(d) ?? []).length > 0) ?? dcNamesOrdered[0];

  if (bundleMode === 'shared') {
    const repDc = repDcName;
    const repNodes = nodesByDc.get(repDc) ?? [{ id: 'node0', uptime_seconds: (365 / 12) * 86400 }];
    const firstNode = repNodes[0];
    const localSamples: Samples = {
      [repDc]: {
        [firstNode.id]: {
          tablestats_data: tablestats,
          schema: nodeSchema,
          info_data: { uptime_seconds: firstNode.uptime_seconds },
          row_size_data: rowSizeData,
        },
      },
    };
    const localStatus = new Map<string, number>([[repDc, statusData.get(repDc) ?? repNodes.length]]);
    const cassandraSet = buildCassandraLocalSet(localSamples, localStatus, { preparedTtlTables });
    const baseAgg = getKeyspaceCassandraAggregate(cassandraSet, repDc);
    for (const dc of dcNamesOrdered) {
      estimateResults[dc] = cloneKeyspaceAggregates(baseAgg);
    }
  } else if (perDcBundles) {
    // Each DC uses its own sampled node; statusData node counts scale it to the whole DC.
    // DCs listed in status without a bundle fall back to a sampled DC (see below).
    const samples: Samples = {};
    for (const [dc, bundle] of bundleByDc.entries()) {
      const dcTablestats = parse_nodetool_tablestats(fs.readFileSync(bundle.tablestats, 'utf8'));
      const dcRowSize = bundle.rowsize ? parseRowSizeInfo(fs.readFileSync(bundle.rowsize, 'utf8')) : rowSizeData;
      applyViews(dcTablestats, dcRowSize);
      const nodes = nodesByDc.get(dc) ?? [{ id: 'node0', uptime_seconds: (365 / 12) * 86400 }];
      samples[dc] = {};
      for (const node of nodes) {
        samples[dc][node.id] = {
          tablestats_data: dcTablestats,
          schema: nodeSchema,
          info_data: { uptime_seconds: node.uptime_seconds },
          row_size_data: dcRowSize,
        };
      }
    }

    const cassandraSet = buildCassandraLocalSet(samples, statusData, { preparedTtlTables });

    for (const dc of dcNames) {
      estimateResults[dc] = getKeyspaceCassandraAggregate(cassandraSet, dc);
    }

    // Fallback: copy each keyspace's aggregate from the first sampled DC that has it. Keyspaces that
    // don't replicate to the target DC are removed by filterEstimateResultsBySchemaReplication below.
    const sampledDcs = dcNamesOrdered.filter((dc) => bundleByDc.has(dc));
    for (const dc of dcNamesOrdered) {
      if (bundleByDc.has(dc)) continue;
      const borrowed: Record<string, KeyspaceAggregate> = {};
      for (const source of sampledDcs) {
        for (const [ks, agg] of Object.entries(estimateResults[source] ?? {})) {
          if (borrowed[ks]) continue;
          borrowed[ks] = { ...agg };
          (fallbackSources[dc] ??= {})[ks] = source;
        }
      }
      estimateResults[dc] = borrowed;
    }
  } else {
    const samples: Samples = {};
    for (const [dc, nodes] of nodesByDc.entries()) {
      samples[dc] = {};
      for (const node of nodes) {
        samples[dc][node.id] = {
          tablestats_data: tablestats,
          schema: nodeSchema,
          info_data: { uptime_seconds: node.uptime_seconds },
          row_size_data: rowSizeData,
        };
      }
    }

    const cassandraSet = buildCassandraLocalSet(samples, statusData, { preparedTtlTables });

    for (const dc of dcNames) {
      estimateResults[dc] = getKeyspaceCassandraAggregate(cassandraSet, dc);
    }
  }

  filterEstimateResultsBySchemaReplication(estimateResults, nodeSchema, dcNamesOrdered);

  // Remove DCs that have no user keyspaces after schema filtering (e.g. seed-only DCs).
  const prunedDcNames = dcNames.filter((dc) => {
    const aggs = estimateResults[dc];
    return aggs && Object.values(aggs).some((a) => a.keyspace_type === 'user');
  });
  if (prunedDcNames.length > 0 && prunedDcNames.length < dcNames.length) {
    dcNames.length = 0;
    prunedDcNames.forEach((dc) => dcNames.push(dc));
    for (const dc of Object.keys(estimateResults)) {
      if (!prunedDcNames.includes(dc)) delete estimateResults[dc];
    }
  }

  // Default `same`: a sample (shared or fallback) applies to every DC it covers, reads included.
  const inferReads: InferReadsMode = args.inferReads ?? 'same';
  applyInferReadsModeToEstimateResults(estimateResults, inferReads, {
    dcsWithInfoFiles,
    representativeDc: repDcName,
    bundleShared: bundleMode === 'shared',
  });

  for (const aggs of Object.values(estimateResults)) {
    applyUseBackupToAggregates(aggs, usePitr);
    for (const agg of Object.values(aggs)) agg.avg_rows_per_read = args.rowsPerRead;
  }
  // PITR can differ per Region/DC: --pitr-override 'dc1=on,dc2=off' wins over the default above.
  if (args.pitrOverride) {
    for (const [dc, on] of parsePitrOverrideCsv(args.pitrOverride)) {
      if (!estimateResults[dc]) {
        console.error(`Warning: --pitr-override names unknown datacenter "${dc}" (expected ${Object.keys(estimateResults).join(', ')})`);
        continue;
      }
      applyUseBackupToAggregates(estimateResults[dc], on);
    }
  }

  if (args.readsOverride) {
    const overrides = parseReadsOverrideCsv(args.readsOverride);
    if (overrides.size > 0) applyPerDcReadOverridesToEstimateResults(estimateResults, overrides);
  }

  // ── Price using calculatePricingEstimate from PricingFormulas.ts ──────────

  const longRegion = regionsMap[args.region] ?? args.region;
  // Region per DC: --dc-region, else a DC named after a Region code (e.g. us-east-1), else --region.
  const regionCodeFor = (dc: string): string =>
    args.dcRegions.get(dc) ?? (regionsMap[dc] ? dc : args.region);
  for (const dc of dcNames) {
    const code = regionCodeFor(dc);
    if (!regionsMap[code]) {
      console.error(`Unknown AWS Region "${code}" for datacenter ${dc}`);
      process.exit(1);
    }
  }

  // A keyspace can't be replicated to the same Region twice. Datacenters that map to one Region become one
  // Region record: reads add up (every DC's clients read from that Region); writes, TTL deletes and storage
  // are replicated, so they count once (the largest DC's value); PITR is on if any DC had it.
  const nodeCountFor = new Map(dcNames.map((dc) => [dc, statusData.get(dc) ?? 0]));
  const mergedDatacenters: Record<string, string[]> = {};
  const firstDcForRegion = new Map<string, string>();
  for (const dc of [...dcNames]) {
    const code = regionCodeFor(dc);
    const into = firstDcForRegion.get(code);
    if (!into) {
      firstDcForRegion.set(code, dc);
      continue;
    }
    const target = estimateResults[into];
    for (const [ks, agg] of Object.entries(estimateResults[dc] ?? {})) {
      const t = target[ks];
      if (!t) {
        target[ks] = { ...agg };
        continue;
      }
      const tOps = t.reads_per_second + t.writes_per_second;
      const aOps = agg.reads_per_second + agg.writes_per_second;
      const weighted = (x: number, y: number) => (tOps + aOps > 0 ? (x * tOps + y * aOps) / (tOps + aOps) : x);
      const readsTotal = t.reads_per_second + agg.reads_per_second;
      target[ks] = {
        ...t,
        reads_per_second: readsTotal,
        writes_per_second: Math.max(t.writes_per_second, agg.writes_per_second),
        ttls_per_second: Math.max(t.ttls_per_second, agg.ttls_per_second),
        uncompressed_single_replica_gb: Math.max(t.uncompressed_single_replica_gb, agg.uncompressed_single_replica_gb),
        total_live_space_gb: Math.max(t.total_live_space_gb ?? 0, agg.total_live_space_gb ?? 0),
        avg_read_row_size_bytes: weighted(t.avg_read_row_size_bytes, agg.avg_read_row_size_bytes),
        avg_write_row_size_bytes: weighted(t.avg_write_row_size_bytes, agg.avg_write_row_size_bytes),
        avg_rows_per_read:
          readsTotal > 0
            ? ((t.avg_rows_per_read ?? 1) * t.reads_per_second + (agg.avg_rows_per_read ?? 1) * agg.reads_per_second) / readsTotal
            : t.avg_rows_per_read,
        use_backup: t.use_backup === true || agg.use_backup === true,
      };
    }
    delete estimateResults[dc];
    nodeCountFor.set(into, (nodeCountFor.get(into) ?? 0) + (nodeCountFor.get(dc) ?? 0));
    (mergedDatacenters[into] ??= [into]).push(dc);
    dcNames.splice(dcNames.indexOf(dc), 1);
    console.error(
      `Warning: datacenters ${mergedDatacenters[into].join(', ')} all map to ${code}; a keyspace can't be replicated ` +
        'to the same Region twice, so they are priced as one Region (reads summed, writes/TTL/storage once).'
    );
  }
  const datacenters: DatacenterRef[] = dcNames.map((name) => ({ name, nodeCount: nodeCountFor.get(name) ?? 0 }));
  const regions: Record<string, string> = Object.fromEntries(
    dcNames.map((dc) => [dc, regionsMap[regionCodeFor(dc)] ?? longRegion])
  );
  const regionsByDatacenter = Object.fromEntries(
    dcNames.map((dc) => [dc, { short: regionCodeFor(dc), long: regions[dc] }])
  );

  const pricing: PricingEstimateResult | null = calculatePricingEstimate(datacenters, regions, estimateResults);
  if (!pricing) {
    console.error('Failed to calculate pricing — check region and input files.');
    process.exit(1);
  }

  // ── Build summary output (same shape as calculate.js) ────────────────────

  const {
    reads_on_demand: sumOdRead,
    writes_on_demand: sumOdWrite,
    ttl_deletes: sumTtl,
    storage: sumStorage,
    backup: sumBackup,
    reads_provisioned: sumProvRead,
    writes_provisioned: sumProvWrite,
  } = aggregatePricingTotals(pricing);

  let compatibilityReport: {
    has_issues: boolean;
    summary: {
      total_issues: number;
      schema: {
        total_issues: number;
        keyspaces_affected: number;
        tables_affected: number;
        functions: number;
        aggregates: number;
      } | null;
      query_patterns: {
        lwt_in_unlogged_batch: number;
        aggregations: number;
        ttl_tables: number;
      } | null;
    };
    details: {
      schema: CompatibilityInfo | null;
      query_patterns: QueryPatternsInfo | null;
    };
  } | null = null;
  if (compatibility || queryPatterns) {
    let schemaSummary: {
      total_issues: number;
      keyspaces_affected: number;
      tables_affected: number;
      functions: number;
      aggregates: number;
      short_column_names: number;
    } | null = null;
    if (compatibility) {
      let total_table_issues = 0;
      let tables_affected = 0;
      let short_column_names = 0;
      for (const tables of Object.values(compatibility.keyspaces)) {
        for (const issue of Object.values(tables)) {
          const count = issue.indexes.length + issue.triggers.length + issue.materializedViews.length + issue.shortColumnNames.length;
          short_column_names += issue.shortColumnNames.length;
          if (count > 0) {
            tables_affected++;
            total_table_issues += count;
          }
        }
      }
      schemaSummary = {
        total_issues: total_table_issues + compatibility.functions + compatibility.aggregates,
        keyspaces_affected: Object.keys(compatibility.keyspaces).length,
        tables_affected,
        functions: compatibility.functions,
        aggregates: compatibility.aggregates,
        short_column_names,
      };
    }
    const querySummary = queryPatterns
      ? {
          lwt_in_unlogged_batch: queryPatterns.lwt_in_unlogged_batch.length,
          aggregations: queryPatterns.aggregations.length,
          ttl_tables: Object.keys(queryPatterns.ttl_tables).length,
        }
      : null;
    const total_issues =
      (schemaSummary?.total_issues ?? 0) +
      (querySummary?.lwt_in_unlogged_batch ?? 0) +
      (querySummary?.aggregations ?? 0);
    compatibilityReport = {
      has_issues: total_issues > 0,
      summary: { total_issues, schema: schemaSummary, query_patterns: querySummary },
      details: { schema: compatibility, query_patterns: queryPatterns },
    };
  }

  // Conversion lines (Part 3 of keyspaces-pricing-rules.md) ahead of the pricing lines (Part 2).
  // The sampled per-node rate is the converted rate scaled back by RF / N, i.e. what tablestats measured.
  const explainCassandraEstimate = (): Record<string, Record<string, string[]>> => {
    const formulas = explainPricingEstimate(datacenters, regions, estimateResults);
    const n = (v: number) => (Number.isInteger(v) ? v.toLocaleString('en-US') : v.toLocaleString('en-US', { maximumFractionDigits: 4 }));
    for (const [dc, byKs] of Object.entries(formulas)) {
      for (const [ks, lines] of Object.entries(byKs)) {
        const agg = estimateResults[dc][ks];
        const copiedFrom =
          fallbackSources[dc]?.[ks] ?? (bundleMode === 'shared' && dc !== repDcName ? repDcName : null);
        let conversion: string[];
        if (mergedDatacenters[dc]) {
          conversion = [
            `Conversion: datacenters ${mergedDatacenters[dc].join(', ')} all map to ${regionsByDatacenter[dc].short}. ` +
              "A keyspace can't be replicated to the same Region twice, so they're one Region record: " +
              'reads/s = sum of the datacenters; writes/s, TTL deletes/s and storage = the largest datacenter.',
          ];
        } else if (copiedFrom) {
          conversion = [`Conversion: workload copied from datacenter ${copiedFrom} (no sample of its own); see ${copiedFrom}.`];
        } else {
          const nodes = statusData.get(dc) ?? 0;
          const rf = agg.replication_factor;
          const readDiv = rf - 1 > 0 ? rf - 1 : 1;
          conversion = [
            `Conversion: N = ${n(nodes)} nodes, RF = ${n(rf)}`,
            `Writes/s = ${n((agg.writes_per_second * rf) / (nodes || 1))} sampled per node × ${n(nodes)} ÷ ${n(rf)} = ${n(agg.writes_per_second)}`,
            `Reads/s = ${n((agg.reads_per_second * readDiv) / (nodes || 1))} sampled per node × ${n(nodes)} ÷ ${n(readDiv)} = ${n(agg.reads_per_second)}`,
            `Storage = ${n((agg.uncompressed_single_replica_gb * rf) / (nodes || 1))} GB uncompressed per node × ${n(nodes)} ÷ ${n(rf)} = ${n(agg.uncompressed_single_replica_gb)} GB`,
          ];
        }
        for (const [view, r] of Object.entries(materializedViews)) {
          if (!view.startsWith(`${ks}.`)) continue;
          conversion.push(
            r.priced
              ? `View ${view}: priced as a table: writes from ${r.writes_source === 'view tablestats' ? "the view's own tablestats (straight writes)" : `${r.base} writes (UNLOGGED BATCH with the base table)`}; ` +
                  `reads = the view's own; storage from ${r.storage_source}.`
              : `View ${view}: not priced (not in --views).`
          );
        }
        if (args.readsOverride || inferReads !== 'same') {
          conversion.push(`Note: reads adjusted by ${args.readsOverride ? '--reads-override' : `--infer-reads ${inferReads}`}.`);
        }
        byKs[ks] = [...conversion, ...lines];
      }
    }
    return formulas;
  };

  // Report fallbacks only for keyspaces that survived replication filtering and pruning.
  const fallbackDatacenters: Record<string, Record<string, string>> = {};
  for (const [dc, byKs] of Object.entries(fallbackSources)) {
    for (const [ks, source] of Object.entries(byKs)) {
      if (estimateResults[dc]?.[ks]) (fallbackDatacenters[dc] ??= {})[ks] = source;
    }
  }

  const result = {
    region: { short: args.region, long: longRegion },
    source: 'cassandra-diagnostic-files',
    bundle_mode: bundleMode,
    regions_by_datacenter: regionsByDatacenter,
    ...(Object.keys(mergedDatacenters).length > 0 ? { merged_datacenters: mergedDatacenters } : {}),
    ...(Object.keys(materializedViews).length > 0 ? { materialized_views: materializedViews } : {}),
    pitr_by_datacenter: Object.fromEntries(
      Object.entries(estimateResults).map(([dc, ks]) => [dc, Object.values(ks).some((a) => a.use_backup === true)])
    ),
    ...(Object.keys(fallbackDatacenters).length > 0 ? { fallback_datacenters: fallbackDatacenters } : {}),
    datacenters,
    on_demand: {
      reads_strong: sumOdRead,
      reads_eventual: sumOdRead / 2,
      writes: sumOdWrite,
      ttl_deletes: sumTtl,
      storage: sumStorage,
      backup: sumBackup,
      total: pricing.total_monthly_on_demand_cost,
    },
    provisioned: {
      reads_strong: sumProvRead,
      reads_eventual: sumProvRead / 2,
      writes: sumProvWrite,
      ttl_deletes: sumTtl,
      storage: sumStorage,
      backup: sumBackup,
      total: pricing.total_monthly_provisioned_cost,
    },
    savings_plan_available: pricing.total_monthly_provisioned_cost_savings !== pricing.total_monthly_provisioned_cost,
    on_demand_savings_plan: {
      total: pricing.total_monthly_on_demand_cost_savings,
    },
    provisioned_savings_plan: {
      total: pricing.total_monthly_provisioned_cost_savings,
    },
    per_datacenter: pricing.total_datacenter_cost,
    compatibility: compatibilityReport,
    ...(args.explain ? { formulas: explainCassandraEstimate() } : {}),
    report_data: {
      datacenters,
      regions,
      estimateResults,
      pricing,
      tco: tcoData,
      status_datacenter_count: statusData.size > 0 ? statusData.size : undefined,
    },
  };

  console.log(JSON.stringify(result, null, 2));
}

main();
