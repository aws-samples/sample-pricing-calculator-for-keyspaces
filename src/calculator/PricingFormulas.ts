import pricingDataJson from './data/mcs.json';
import { system_keyspaces, REPLICATION_FACTOR, SECONDS_PER_MONTH, GIGABYTE, DEFAULT_TARGET_UTILIZATION } from './Constants';
import { HOURS_PER_MONTH, resolveStatusDcToSchemaDc, type InferReadsMode } from './ParsingHelpers';
import savingsPlansMap from './PricingData';

// --- Type definitions ---

export interface RegionPricing {
  readRequestPrice: number;
  writeRequestPrice: number;
  writeRequestPricePerHour: number;
  readRequestPricePerHour: number;
  storagePricePerGB: number;
  pitrPricePerGB: number;
  ttlDeletesPrice: number;
}

interface TablestatsTableData {
  space_used?: number;
  compression_ratio?: number;
  read_count?: number;
  write_count?: number;
}

interface SchemaKeyspaceInfo {
  datacenters: Record<string, number>;
}

interface NodePayload {
  tablestats_data: Record<string, Record<string, TablestatsTableData>>;
  schema?: Record<string, SchemaKeyspaceInfo>;
  info_data: { uptime_seconds: number };
  row_size_data: Record<string, { average?: string; 'default-ttl'?: string }>;
}

export type Samples = Record<string, Record<string, NodePayload>>;

export interface TableMetrics {
  table_name: string;
  total_compressed_bytes: number;
  total_uncompressed_bytes: number;
  avg_row_size_bytes: number;
  writes_monthly: number;
  reads_monthly: number;
  has_ttl: boolean;
  sample_count: number;
}

interface DcTables {
  number_of_nodes: number;
  replication_factor: number;
  tables: Record<string, TableMetrics>;
}

interface KeyspaceInSet {
  type: 'system' | 'user';
  dcs: Record<string, DcTables>;
}

export interface CassandraLocalSet {
  data: {
    keyspaces: Record<string, KeyspaceInSet>;
  };
}

export interface KeyspaceAggregate {
  keyspace_name: string;
  keyspace_type: 'system' | 'user';
  replication_factor: number;
  total_live_space_gb?: number;
  uncompressed_single_replica_gb: number;
  avg_write_row_size_bytes: number;
  avg_read_row_size_bytes: number;
  writes_per_second: number;
  reads_per_second: number;
  ttls_per_second: number;
  use_backup?: boolean;
  /** Rows returned per read request (range/partition reads); read units aggregate their bytes. Default 1. */
  avg_rows_per_read?: number;
}

export type EstimateResults = Record<string, Record<string, KeyspaceAggregate>>;

export interface DatacenterRef {
  name: string;
  nodeCount?: number;
}

export interface KeyspaceCostEntry {
  name: string;
  storage: number;
  backup: number;
  reads_provisioned: number;
  writes_provisioned: number;
  reads_provisioned_savings: number;
  writes_provisioned_savings: number;
  reads_on_demand: number;
  writes_on_demand: number;
  reads_on_demand_savings: number;
  writes_on_demand_savings: number;
  ttlDeletes: number;
  provisioned_total: number;
  on_demand_total: number;
  provisioned_total_savings: number;
  on_demand_total_savings: number;
}

export interface DatacenterCost {
  region: string;
  keyspaceCosts: Record<string, KeyspaceCostEntry>;
  total_datacenter_provisioned_cost: number;
  total_datacenter_on_demand_cost: number;
  total_datacenter_provisioned_cost_savings: number;
  total_datacenter_on_demand_cost_savings: number;
}

export interface PricingEstimateResult {
  total_datacenter_cost: Record<string, DatacenterCost>;
  total_monthly_provisioned_cost: number;
  total_monthly_on_demand_cost: number;
  total_monthly_provisioned_cost_savings: number;
  total_monthly_on_demand_cost_savings: number;
}

export interface FormDataRegion {
  averageRowSizeInBytes?: number;
  averageReadRequestsPerSecond?: number;
  averageWriteRequestsPerSecond?: number;
  averageTtlDeletesPerSecond?: number;
  storageSizeInGb?: number;
  pointInTimeRecoveryForBackups?: boolean;
}

export interface MultiRegionOption {
  value: string;
}

export interface KeyspacesEstimateInput {
  datacenters: DatacenterRef[];
  regions: Record<string, string>;
  estimateResults: EstimateResults;
}

export interface ProvisionedPricing {
  strongConsistencyReads: number;
  strongConsistencyWrites: number;
  eventualConsistencyReads: number;
  eventualConsistencyWrites: number;
  strongConsistencyReadsSavings: number;
  strongConsistencyWritesSavings: number;
  eventualConsistencyReadsSavings: number;
  eventualConsistencyWritesSavings: number;
  strongConsistencyStorage: number;
  strongConsistencyBackup: number;
  eventualConsistencyStorage: number;
  eventualConsistencyBackup: number;
  strongConsistencyTtlDeletesPrice: number;
  eventualConsistencyTtlDeletesPrice: number;
}

export interface MapPricingResult {
  provisionedPricing: ProvisionedPricing;
  onDemandPricing: ProvisionedPricing;
}

// MCS JSON shape (minimal for region pricing lookup)
interface McsRegionProducts {
  'MCS-ReadUnits'?: { price: string };
  'MCS-WriteUnits'?: { price: string };
  'Provisioned Write Units'?: { price: string };
  'Provisioned Read Units'?: { price: string };
  'AmazonMCS - Indexed DataStore per GB-Mo'?: { price: string };
  'Point-In-Time-Restore PITR Backup Storage per GB-Mo'?: { price: string };
  'Time to Live'?: { price: string };
}

interface McsPricingJson {
  manifest?: { hawkFilePublicationDate?: string };
  regions?: Record<string, McsRegionProducts>;
}

const pricingData = pricingDataJson as McsPricingJson;

interface SavingsPlanRate {
  rate: number;
}

/** Publication date of the list prices in mcs.json (YYYY-MM-DD), for explanations and reports. */
export const PRICE_LIST_DATE: string = (pricingData.manifest?.hawkFilePublicationDate ?? '').slice(0, 10) || 'unknown';
type SavingsPlansMapType = Record<string, Record<string, SavingsPlanRate>>;
const savingsPlanMap = savingsPlansMap as SavingsPlansMapType;

/** Resolve pricing primitives for a region from pricing JSON. */
const getRegionPricing = (regionName: string): RegionPricing | null => {
  if (!pricingData?.regions?.[regionName]) {
    return null;
  }
  const r = pricingData.regions[regionName];
  return {
    readRequestPrice: Number(r['MCS-ReadUnits']?.price ?? 0),
    writeRequestPrice: Number(r['MCS-WriteUnits']?.price ?? 0),
    writeRequestPricePerHour: Number(r['Provisioned Write Units']?.price ?? 0),
    readRequestPricePerHour: Number(r['Provisioned Read Units']?.price ?? 0),
    storagePricePerGB: Number(r['AmazonMCS - Indexed DataStore per GB-Mo']?.price ?? 0),
    pitrPricePerGB: Number(r['Point-In-Time-Restore PITR Backup Storage per GB-Mo']?.price ?? 0),
    ttlDeletesPrice: Number(r['Time to Live']?.price ?? 0),
  };
};

/**
 * Build a normalized Cassandra dataset from raw samples and status data.
 */
export const buildCassandraLocalSet = (
  samples: Samples,
  statusData: Map<string, number>,
  opts?: { preparedTtlTables?: Set<string> }
): CassandraLocalSet => {
  const preparedTtl = opts?.preparedTtlTables;
  const result: CassandraLocalSet = {
    data: { keyspaces: {} },
  };

  for (const [dcName, dcData] of Object.entries(samples)) {
    const numberOfNodes = statusData.get(dcName);
    for (const [, nodeData] of Object.entries(dcData)) {
      const tablestatsData = nodeData.tablestats_data;
      const schema = nodeData.schema;
      const infoData = nodeData.info_data;
      const rowSizeData = nodeData.row_size_data;
      const uptimeSeconds = infoData.uptime_seconds;

      for (const [keyspaceName, keyspaceData] of Object.entries(tablestatsData)) {
        let replicationFactor = REPLICATION_FACTOR;
        if (schema?.[keyspaceName]) {
          const dcKeys = Object.keys(schema[keyspaceName].datacenters);
          const schemaDc = resolveStatusDcToSchemaDc(dcName, dcKeys);
          if (!schemaDc) {
            continue;
          }
          const rf = schema[keyspaceName].datacenters[schemaDc];
          if (rf === undefined || rf <= 0) {
            continue;
          }
          replicationFactor = rf;
        }

        if (!result.data.keyspaces[keyspaceName]) {
          result.data.keyspaces[keyspaceName] = {
            type: isSystemKeyspace(keyspaceName),
            dcs: {},
          };
        }

        if (!result.data.keyspaces[keyspaceName].dcs[dcName]) {
          result.data.keyspaces[keyspaceName].dcs[dcName] = {
            number_of_nodes: numberOfNodes ?? 0,
            replication_factor: replicationFactor,
            tables: {},
          };
        }

        for (const [tableName, tableData] of Object.entries(keyspaceData)) {
          const dcTables = result.data.keyspaces[keyspaceName].dcs[dcName].tables;
          if (!dcTables[tableName]) {
            const fullyQualifiedTableName = `${keyspaceName}.${tableName}`;
            let hasTtl = false;
            let averageBytes = 1024;
            const rowEntry = rowSizeData?.[fullyQualifiedTableName];
            if (rowEntry) {
              const avgNumber = rowEntry.average ?? '1';
              const parsedBytes = parseInt(avgNumber, 10);
              averageBytes = (isNaN(parsedBytes) || parsedBytes <= 0) ? 1 : parsedBytes;
              const ttlStr = rowEntry['default-ttl'] ?? 'y';
              hasTtl = String(ttlStr).trim() === 'y';
            }
            // Prepared-statement USING TTL evidence unions into has_ttl:
            // if any prepared statement writes TTL rows to this table, 100%
            // of its writes are treated as TTL deletes (same as a table
            // with default_time_to_live set).
            if (!hasTtl && preparedTtl && preparedTtl.has(fullyQualifiedTableName.toLowerCase())) {
              hasTtl = true;
            }
            dcTables[tableName] = {
              table_name: tableName,
              total_compressed_bytes: 0,
              total_uncompressed_bytes: 0,
              avg_row_size_bytes: averageBytes,
              writes_monthly: 0,
              reads_monthly: 0,
              has_ttl: hasTtl,
              sample_count: 0,
            };
          }

          let spaceUsed = Number(tableData.space_used) || 0;
          if (isNaN(spaceUsed)) spaceUsed = 0;
          const ratio = spaceUsed > 0 ? (tableData.compression_ratio ?? 1) : 1;
          let readCount = Number(tableData.read_count) || 0;
          let writeCount = Number(tableData.write_count) || 0;
          if (isNaN(readCount)) readCount = 0;
          if (isNaN(writeCount)) writeCount = 0;

          const table = dcTables[tableName];
          table.total_compressed_bytes += spaceUsed;
          table.total_uncompressed_bytes += calculateUncompressedStoragePerNode(spaceUsed, ratio);
          table.writes_monthly += calculateWriteOperationsPerNodePerMonth(writeCount, uptimeSeconds);
          table.reads_monthly += calculateReadOperationsPerNodePerMonth(readCount, uptimeSeconds);
          table.sample_count += 1;
        }
      }
    }
  }
  return result;
};

/** How view writes are counted: the view's own tablestats counters, or the base table's (UNLOGGED BATCH). */
export type ViewWriteMode = 'own' | 'unlogged-batch';

/** How a materialized view was treated (keyspaces-pricing-rules.md, 4.6). */
export interface MaterializedViewReport {
  base: string;
  priced: boolean;
  /** Where the view's writes came from when priced. */
  writes_source?: 'view tablestats' | 'base table (unlogged batch)';
  /** Where the view's storage came from when priced. */
  storage_source?: 'view tablestats' | 'base table (fallback)';
  /** view row bytes ÷ base row bytes, applied to the fallback storage and to a derived view row size. */
  row_size_ratio?: number;
  row_size_ratio_source?: 'row-size samples' | 'schema column types' | 'none';
  note?: string;
}

/**
 * Materialized views aren't supported by Keyspaces; each view is carried over as a regular table. Mutates one sample:
 *   - views not in `include` are removed from the tablestats (not priced)
 *   - included views: reads = the view's own counters; storage = the view's own tablestats, else the base table's
 *     scaled by (view row bytes ÷ base row bytes); a view without a row-size sample gets the base row size × that
 *     ratio; TTL falls back to the base table's (the row-size sampler doesn't sample views). The ratio comes from
 *     row-size samples of both, else `schemaRowRatios` (column-type estimate), else 1.
 *   - writes: 'own' (default) = the view's own tablestats write counters (straight writes);
 *     'unlogged-batch' = the base table's writes (view row written with the base row in an UNLOGGED BATCH)
 */
export const applyMaterializedViewPolicy = (
  tablestats: Record<string, Record<string, { space_used: number; compression_ratio: number; read_count: number; write_count: number }>>,
  views: Map<string, string>,
  include: Set<string> | 'all',
  writeMode: ViewWriteMode = 'own',
  rowSizeData?: Record<string, Record<string, string>>,
  preparedTtlTables?: Set<string>,
  schemaRowRatios?: Map<string, number>
): Record<string, MaterializedViewReport> => {
  const report: Record<string, MaterializedViewReport> = {};
  for (const [view, base] of Array.from(views.entries())) {
    const [vks, vname] = view.split('.');
    const [bks, bname] = base.split('.');
    const wanted = include === 'all' || include.has(view);
    if (!wanted) {
      if (tablestats[vks]?.[vname]) delete tablestats[vks][vname];
      report[view] = { base, priced: false };
      continue;
    }
    const own = tablestats[vks]?.[vname];
    const baseStats = tablestats[bks]?.[bname];
    if (!baseStats) {
      report[view] = own
        ? { base, priced: true, writes_source: 'view tablestats', storage_source: 'view tablestats',
            note: 'base table not in tablestats; view priced from its own counters' }
        : { base, priced: false, note: 'neither the view nor its base table is in tablestats' };
      continue;
    }
    const hasOwnStorage = !!own && Number(own.space_used) > 0;
    // Without the view's own counters there are no straight writes to use, so base writes stand in.
    const useBaseWrites = writeMode === 'unlogged-batch' || !own;

    // view row bytes ÷ base row bytes: measured samples first, then the schema's column types, else 1.
    const viewAvg = Number(rowSizeData?.[view]?.average);
    const baseAvg = Number(rowSizeData?.[base]?.average);
    let ratio = 1;
    let ratioSource: MaterializedViewReport['row_size_ratio_source'] = 'none';
    if (viewAvg > 0 && baseAvg > 0) {
      ratio = viewAvg / baseAvg;
      ratioSource = 'row-size samples';
    } else if (schemaRowRatios?.has(view)) {
      ratio = schemaRowRatios.get(view)!;
      ratioSource = 'schema column types';
    }

    tablestats[vks] ??= {};
    tablestats[vks][vname] = {
      space_used: hasOwnStorage ? own.space_used : baseStats.space_used * ratio,
      compression_ratio: hasOwnStorage ? own.compression_ratio : baseStats.compression_ratio,
      read_count: own?.read_count ?? 0,
      write_count: useBaseWrites ? baseStats.write_count : own.write_count,
    };
    const derivesRowSize = !!rowSizeData && !rowSizeData[view] && !!rowSizeData[base] && baseAvg > 0;
    if (rowSizeData && !rowSizeData[view] && rowSizeData[base]) {
      rowSizeData[view] = {
        ...rowSizeData[base],
        ...(baseAvg > 0 ? { average: String(Math.max(1, Math.round(baseAvg * ratio))) } : {}),
      };
    }
    if (preparedTtlTables?.has(base.toLowerCase())) preparedTtlTables.add(view.toLowerCase());
    report[view] = {
      base,
      priced: true,
      writes_source: useBaseWrites ? 'base table (unlogged batch)' : 'view tablestats',
      storage_source: hasOwnStorage ? 'view tablestats' : 'base table (fallback)',
      // Report the ratio only where it was applied: fallback storage or a derived view row size.
      ...(!hasOwnStorage || derivesRowSize
        ? { row_size_ratio: Math.round(ratio * 10000) / 10000, row_size_ratio_source: ratioSource }
        : {}),
      ...(!own ? { note: 'view not in tablestats; base table writes and size used' } : {}),
    };
  }
  return report;
};

/**
 * Aggregate table-level metrics to keyspace-level for a specific datacenter.
 */
export const getKeyspaceCassandraAggregate = (
  cassandra_set: CassandraLocalSet,
  datacenter: string
): Record<string, KeyspaceAggregate> => {
  const keyspace_aggregate: Record<string, KeyspaceAggregate> = {};

  for (const [keyspace, keyspaceData] of Object.entries(cassandra_set.data.keyspaces)) {
    if (keyspaceData.type === 'system') continue;
    const dcData = keyspaceData.dcs[datacenter];
    if (!dcData) continue;

    const number_of_nodes = dcData.number_of_nodes;
    const replication_factor = dcData.replication_factor;

    let keyspace_writes_total = 0;
    let keyspace_reads_total = 0;
    let total_live_space = 0;
    let uncompressed_single_replica = 0;
    let write_row_size_bytes = 0;
    let read_row_size_bytes = 0;
    let keyspace_ttls_total = 0;

    for (const [, tableData] of Object.entries(dcData.tables)) {
      const sc = tableData.sample_count || 1;
      keyspace_writes_total += tableData.writes_monthly / sc;
      total_live_space += tableData.total_compressed_bytes / sc;
      uncompressed_single_replica += tableData.total_uncompressed_bytes / sc;
      write_row_size_bytes += (tableData.writes_monthly * tableData.avg_row_size_bytes) / sc;
      read_row_size_bytes += (tableData.reads_monthly * tableData.avg_row_size_bytes) / sc;
      keyspace_reads_total += tableData.reads_monthly / sc;
      keyspace_ttls_total += tableData.has_ttl ? tableData.writes_monthly / sc : 0;
    }

    const total_ops = keyspace_reads_total + keyspace_writes_total;
    const combined_row_size_bytes = (read_row_size_bytes + write_row_size_bytes) / (total_ops > 0 ? total_ops : 1);
    const average_read_row_size_bytes = combined_row_size_bytes;
    const average_write_row_size_bytes = combined_row_size_bytes;
    keyspace_aggregate[keyspace] = {
      keyspace_name: keyspace,
      keyspace_type: keyspaceData.type,
      replication_factor,
      total_live_space_gb: (total_live_space * number_of_nodes) / GIGABYTE,
      uncompressed_single_replica_gb: (uncompressed_single_replica * number_of_nodes) / replication_factor / GIGABYTE,
      avg_write_row_size_bytes: average_write_row_size_bytes,
      avg_read_row_size_bytes: average_read_row_size_bytes,
      writes_per_second: (keyspace_writes_total / SECONDS_PER_MONTH) * (number_of_nodes / replication_factor),
      reads_per_second: (keyspace_reads_total / SECONDS_PER_MONTH) * (number_of_nodes / (replication_factor - 1 > 0 ? replication_factor - 1 : 1)),
      ttls_per_second: (keyspace_ttls_total / SECONDS_PER_MONTH) * (number_of_nodes / replication_factor),
    };
  }
  return keyspace_aggregate;
};

/** Deep-copy keyspace aggregates (used when fanning out shared-bundle results per DC). */
export const cloneKeyspaceAggregates = (
  aggregates: Record<string, KeyspaceAggregate>
): Record<string, KeyspaceAggregate> =>
  Object.fromEntries(
    Object.entries(aggregates).map(([k, v]) => [k, { ...v }])
  );

/**
 * Multiply billable workload metrics by `factor` (e.g. datacenter count for a single diagnostic bundle).
 * Row-size averages are unchanged — they describe payload shape, not billable quantity.
 */
export const scaleKeyspaceAggregates = (
  aggregates: Record<string, KeyspaceAggregate>,
  factor: number
): Record<string, KeyspaceAggregate> => {
  if (!Number.isFinite(factor) || factor === 1) {
    return cloneKeyspaceAggregates(aggregates);
  }
  const out: Record<string, KeyspaceAggregate> = {};
  for (const [ks, agg] of Object.entries(aggregates)) {
    out[ks] = {
      ...agg,
      total_live_space_gb: (agg.total_live_space_gb ?? 0) * factor,
      uncompressed_single_replica_gb: agg.uncompressed_single_replica_gb * factor,
      writes_per_second: agg.writes_per_second * factor,
      reads_per_second: agg.reads_per_second * factor,
      ttls_per_second: agg.ttls_per_second * factor,
    };
  }
  return out;
};

export const applyUseBackupToAggregates = (
  aggregates: Record<string, KeyspaceAggregate>,
  use_backup: boolean
): void => {
  for (const agg of Object.values(aggregates)) {
    agg.use_backup = use_backup;
  }
};

/**
 * For each DC in `overrides`, scale keyspace read rates proportionally so their sum matches the target reads/s.
 */
export const applyPerDcReadOverridesToEstimateResults = (
  estimateResults: EstimateResults,
  overrides: Map<string, number>
): void => {
  Array.from(overrides.entries()).forEach(([dc, targetReads]) => {
    const aggs = estimateResults[dc];
    if (!aggs) return;

    let sum = 0;
    for (const a of Object.values(aggs)) {
      if (a.keyspace_type === 'user') sum += a.reads_per_second;
    }
    if (targetReads === 0) {
      for (const a of Object.values(aggs)) {
        if (a.keyspace_type === 'user') a.reads_per_second = 0;
      }
      return;
    }
    if (sum <= 0) {
      const userKs = Object.values(aggs).filter((a) => a.keyspace_type === 'user');
      if (userKs.length === 1) {
        userKs[0].reads_per_second = targetReads;
      } else if (userKs.length > 1) {
        const share = targetReads / userKs.length;
        for (const a of userKs) a.reads_per_second = share;
      }
      return;
    }
    const ratio = targetReads / sum;
    for (const a of Object.values(aggs)) {
      if (a.keyspace_type === 'user') a.reads_per_second *= ratio;
    }
  });
};

/** Drop keyspaces from a DC estimate when NTS has no replica on that DC; sync replication_factor. */
export const filterEstimateResultsBySchemaReplication = (
  estimateResults: EstimateResults,
  schema: Record<string, { datacenters: Record<string, number> }> | null | undefined,
  statusDatacenters: string[]
): void => {
  if (!schema) return;
  for (const dc of statusDatacenters) {
    const aggs = estimateResults[dc];
    if (!aggs) continue;
    for (const ks of Object.keys(aggs)) {
      const entry = schema[ks];
      if (!entry) continue;
      const schemaDc = resolveStatusDcToSchemaDc(dc, Object.keys(entry.datacenters));
      const rf = schemaDc ? entry.datacenters[schemaDc] : undefined;
      if (rf === undefined || rf <= 0) {
        delete aggs[ks];
      } else {
        aggs[ks].replication_factor = rf;
      }
    }
  }
};

/**
 * `parse-cassandra.ts` defaults to **`zero`**: DCs with no `--info` capture get 0 reads.
 * **`same`**: skip adjustment (cloned read rates apply to every DC).
 * **`zero`**: zero reads on DCs without a matching `--info` file.
 * **`measured`** + shared bundle: only `representativeDc` retains reads unless overridden.
 */
export const applyInferReadsModeToEstimateResults = (
  estimateResults: EstimateResults,
  mode: InferReadsMode,
  opts: { dcsWithInfoFiles: Set<string>; representativeDc: string | null; bundleShared: boolean }
): void => {
  if (mode === 'same') return;
  for (const dc of Object.keys(estimateResults)) {
    let zeroReads = false;
    if (mode === 'zero') {
      zeroReads = !opts.dcsWithInfoFiles.has(dc);
    } else if (mode === 'measured') {
      if (opts.bundleShared && opts.representativeDc) {
        zeroReads = dc !== opts.representativeDc;
      } else {
        zeroReads = !opts.dcsWithInfoFiles.has(dc);
      }
    }
    if (!zeroReads) continue;
    const aggs = estimateResults[dc];
    for (const agg of Object.values(aggs)) {
      if (agg.keyspace_type === 'user') agg.reads_per_second = 0;
    }
  }
};

/**
 * Compute per-datacenter and total monthly costs from keyspace aggregates.
 */
export const calculatePricingEstimate = (
  datacenters: DatacenterRef[],
  regions: Record<string, string>,
  estimateResults: EstimateResults
): PricingEstimateResult | null => {
  if (!Array.isArray(datacenters) || datacenters.length === 0) return null;

  const pricingData: Record<string, DatacenterCost> = {};
  let total_monthly_provisioned_cost = 0;
  let total_monthly_on_demand_cost = 0;
  let total_monthly_provisioned_cost_savings = 0;
  let total_monthly_on_demand_cost_savings = 0;

  datacenters.forEach((dc) => {
    const region = regions[dc.name];
    const results = estimateResults[dc.name];
    if (!results || !region) return;

    const regionPricing = getRegionPricing(region);
    if (!regionPricing) return;

    const savingsPlan = savingsPlanMap[region];
    let total_datacenter_provisioned_cost = 0;
    let total_datacenter_on_demand_cost = 0;
    let total_datacenter_provisioned_cost_savings = 0;
    let total_datacenter_on_demand_cost_savings = 0;

    const keyspaceCosts: Record<string, KeyspaceCostEntry> = {};
    keyspaceCosts['totals'] = {
      name: 'region total',
      storage: 0,
      backup: 0,
      reads_provisioned: 0,
      reads_provisioned_savings: 0,
      writes_provisioned: 0,
      writes_provisioned_savings: 0,
      reads_on_demand: 0,
      reads_on_demand_savings: 0,
      writes_on_demand: 0,
      writes_on_demand_savings: 0,
      ttlDeletes: 0,
      provisioned_total: 0,
      on_demand_total: 0,
      provisioned_total_savings: 0,
      on_demand_total_savings: 0,
    };

    Object.entries(results).forEach(([keyspace, data]) => {
      const writePrice = regionPricing.writeRequestPrice;
      const readPrice = regionPricing.readRequestPrice;
      const oneDemandWriteCost = calculateOnDemandWriteUnitsPerMonthCost(data.writes_per_second, data.avg_write_row_size_bytes, writePrice);
      const rowsPerRead = data.avg_rows_per_read ?? 1;
      const oneDemandReadCost = calculateOnDemandReadUnitsPerMonthCost(data.reads_per_second, data.avg_read_row_size_bytes, readPrice, rowsPerRead);
      const ttlDeleteCost = calculateTtlUnitsPerMonthCost(data.ttls_per_second, data.avg_write_row_size_bytes, regionPricing.ttlDeletesPrice);

      const oneDemandWriteCostWithSavings = savingsPlan
        ? calculateOnDemandWriteUnitsPerMonthCost(data.writes_per_second, data.avg_write_row_size_bytes, savingsPlan['WriteRequestUnits']?.rate ?? writePrice)
        : oneDemandWriteCost;
      const oneDemandReadCostWithSavings = savingsPlan
        ? calculateOnDemandReadUnitsPerMonthCost(data.reads_per_second, data.avg_read_row_size_bytes, savingsPlan['ReadRequestUnits']?.rate ?? readPrice, rowsPerRead)
        : oneDemandReadCost;
      const provisionedWriteCostWithSavings = savingsPlan
        ? calculateProvisionedWriteCostPerMonth(data.writes_per_second, data.avg_write_row_size_bytes, savingsPlan['WriteCapacityUnitHrs']?.rate ?? regionPricing.writeRequestPricePerHour, DEFAULT_TARGET_UTILIZATION)
        : 0;
      const provisionedReadCostWithSavings = savingsPlan
        ? calculateProvisionedReadCostPerMonth(data.reads_per_second, data.avg_read_row_size_bytes, savingsPlan['ReadCapacityUnitHrs']?.rate ?? regionPricing.readRequestPricePerHour, DEFAULT_TARGET_UTILIZATION, rowsPerRead)
        : 0;

      const provisionedWriteCost = calculateProvisionedWriteCostPerMonth(data.writes_per_second, data.avg_write_row_size_bytes, regionPricing.writeRequestPricePerHour, DEFAULT_TARGET_UTILIZATION);
      const provisionedReadCost = calculateProvisionedReadCostPerMonth(data.reads_per_second, data.avg_read_row_size_bytes, regionPricing.readRequestPricePerHour, DEFAULT_TARGET_UTILIZATION, rowsPerRead);

      const storageCost = calculateStorageCostPerMonth(data.uncompressed_single_replica_gb, regionPricing.storagePricePerGB);
      const backupCost = data.use_backup === true
        ? calculateBackupCostPerMonth(data.uncompressed_single_replica_gb, regionPricing.pitrPricePerGB)
        : 0;

      const provisioned_total_savings = calculateProvisionedCapacityTotalMonthlyCostWithAggregates(provisionedReadCostWithSavings, provisionedWriteCostWithSavings, ttlDeleteCost, storageCost, backupCost);
      const on_demand_total_savings = calculateOnDemandCapcityTotalMonthlyCostWithAggregates(oneDemandReadCostWithSavings, oneDemandWriteCostWithSavings, ttlDeleteCost, storageCost, backupCost);
      const provisioned_total = calculateProvisionedCapacityTotalMonthlyCostWithAggregates(provisionedReadCost, provisionedWriteCost, ttlDeleteCost, storageCost, backupCost);
      const on_demand_total = calculateOnDemandCapcityTotalMonthlyCostWithAggregates(oneDemandReadCost, oneDemandWriteCost, ttlDeleteCost, storageCost, backupCost);

      keyspaceCosts[keyspace] = {
        name: keyspace,
        storage: storageCost,
        backup: backupCost,
        reads_provisioned: provisionedReadCost,
        writes_provisioned: provisionedWriteCost,
        reads_provisioned_savings: provisionedReadCostWithSavings,
        writes_provisioned_savings: provisionedWriteCostWithSavings,
        reads_on_demand: oneDemandReadCost,
        writes_on_demand: oneDemandWriteCost,
        reads_on_demand_savings: oneDemandReadCostWithSavings,
        writes_on_demand_savings: oneDemandWriteCostWithSavings,
        ttlDeletes: ttlDeleteCost,
        provisioned_total,
        on_demand_total,
        provisioned_total_savings,
        on_demand_total_savings,
      };

      const totals = keyspaceCosts['totals'];
      totals.storage += storageCost;
      totals.backup += backupCost;
      totals.reads_provisioned += provisionedReadCost;
      totals.writes_provisioned += provisionedWriteCost;
      totals.reads_on_demand += oneDemandReadCost;
      totals.writes_on_demand += oneDemandWriteCost;
      totals.reads_provisioned_savings += provisionedReadCostWithSavings;
      totals.writes_provisioned_savings += provisionedWriteCostWithSavings;
      totals.reads_on_demand_savings += oneDemandReadCostWithSavings;
      totals.writes_on_demand_savings += oneDemandWriteCostWithSavings;
      totals.ttlDeletes += ttlDeleteCost;
      totals.provisioned_total += provisioned_total;
      totals.on_demand_total += on_demand_total;
      totals.provisioned_total_savings += provisioned_total_savings;
      totals.on_demand_total_savings += on_demand_total_savings;

      total_datacenter_provisioned_cost += provisioned_total;
      total_datacenter_on_demand_cost += on_demand_total;
      total_datacenter_provisioned_cost_savings += provisioned_total_savings;
      total_datacenter_on_demand_cost_savings += on_demand_total_savings;
    });

    const totals = keyspaceCosts['totals'];
    delete keyspaceCosts['totals'];
    keyspaceCosts['totals'] = totals;

    pricingData[dc.name] = {
      region,
      keyspaceCosts,
      total_datacenter_provisioned_cost,
      total_datacenter_on_demand_cost,
      total_datacenter_provisioned_cost_savings,
      total_datacenter_on_demand_cost_savings,
    };

    total_monthly_provisioned_cost += total_datacenter_provisioned_cost;
    total_monthly_on_demand_cost += total_datacenter_on_demand_cost;
    total_monthly_provisioned_cost_savings += total_datacenter_provisioned_cost_savings;
    total_monthly_on_demand_cost_savings += total_datacenter_on_demand_cost_savings;
  });

  return {
    total_datacenter_cost: pricingData,
    total_monthly_provisioned_cost,
    total_monthly_on_demand_cost,
    total_monthly_provisioned_cost_savings,
    total_monthly_on_demand_cost_savings,
  };
};

/**
 * Build datacenters, regions, and estimateResults from Keyspaces form data.
 */
export const buildKeyspacesEstimateInput = (
  formData: Record<string, FormDataRegion>,
  selectedRegion: string,
  multiSelectedRegions: MultiRegionOption[] = []
): KeyspacesEstimateInput => {
  const regionNames = [selectedRegion, ...multiSelectedRegions.map((r) => r.value)];
  const datacenters: DatacenterRef[] = regionNames.map((name) => ({ name }));
  const regions: Record<string, string> = {};
  const estimateResults: EstimateResults = {};
  regionNames.forEach((regionName) => {
    regions[regionName] = regionName;
    const d = formData[regionName] ?? formData.default ?? {};
    const storageGb = Number(d.storageSizeInGb) || 0;
    estimateResults[regionName] = {
      default: {
        keyspace_name: 'default',
        keyspace_type: 'user',
        replication_factor: 3,
        total_live_space_gb: storageGb,
        uncompressed_single_replica_gb: storageGb,
        avg_read_row_size_bytes: Number(d.averageRowSizeInBytes) || 1024,
        avg_write_row_size_bytes: Number(d.averageRowSizeInBytes) || 1024,
        reads_per_second: Number(d.averageReadRequestsPerSecond) || 0,
        writes_per_second: Number(d.averageWriteRequestsPerSecond) || 0,
        ttls_per_second: Number(d.averageTtlDeletesPerSecond) || 0,
        use_backup: !!d.pointInTimeRecoveryForBackups,
      },
    };
  });
  return { datacenters, regions, estimateResults };
};

export interface PricingTotals {
  storage: number;
  backup: number;
  ttl_deletes: number;
  reads_provisioned: number;
  writes_provisioned: number;
  reads_provisioned_savings: number;
  writes_provisioned_savings: number;
  reads_on_demand: number;
  writes_on_demand: number;
  reads_on_demand_savings: number;
  writes_on_demand_savings: number;
}

/**
 * Sum the pre-computed 'totals' row across all datacenters in a PricingEstimateResult.
 */
export const aggregatePricingTotals = (pricing: PricingEstimateResult): PricingTotals => {
  const acc: PricingTotals = {
    storage: 0, backup: 0, ttl_deletes: 0,
    reads_provisioned: 0, writes_provisioned: 0,
    reads_provisioned_savings: 0, writes_provisioned_savings: 0,
    reads_on_demand: 0, writes_on_demand: 0,
    reads_on_demand_savings: 0, writes_on_demand_savings: 0,
  };
  for (const dcCost of Object.values(pricing.total_datacenter_cost)) {
    const t = dcCost.keyspaceCosts['totals'];
    if (!t) continue;
    acc.storage                   += t.storage;
    acc.backup                    += t.backup;
    acc.ttl_deletes               += t.ttlDeletes;
    acc.reads_provisioned         += t.reads_provisioned;
    acc.writes_provisioned        += t.writes_provisioned;
    acc.reads_provisioned_savings += t.reads_provisioned_savings;
    acc.writes_provisioned_savings += t.writes_provisioned_savings;
    acc.reads_on_demand           += t.reads_on_demand;
    acc.writes_on_demand          += t.writes_on_demand;
    acc.reads_on_demand_savings   += t.reads_on_demand_savings;
    acc.writes_on_demand_savings  += t.writes_on_demand_savings;
  }
  return acc;
};

/**
 * Map calculatePricingEstimate result to the Keyspaces PricingTable shape.
 */
export const mapPricingEstimateToKeyspacesTable = (
  pricingResult: PricingEstimateResult | null
): MapPricingResult => {
  if (!pricingResult?.total_datacenter_cost) {
    return { provisionedPricing: {} as ProvisionedPricing, onDemandPricing: {} as ProvisionedPricing };
  }

  const t = aggregatePricingTotals(pricingResult);
  const {
    storage: totalStorage, backup: totalBackup, ttl_deletes: totalTtl,
    reads_provisioned: readsProvisioned, writes_provisioned: writesProvisioned,
    reads_provisioned_savings: readsProvisionedSavings, writes_provisioned_savings: writesProvisionedSavings,
    reads_on_demand: readsOnDemand, writes_on_demand: writesOnDemand,
    reads_on_demand_savings: readsOnDemandSavings, writes_on_demand_savings: writesOnDemandSavings,
  } = t;

  const provisionedPricing: ProvisionedPricing = {
    strongConsistencyReads: readsProvisioned,
    strongConsistencyWrites: writesProvisioned,
    eventualConsistencyReads: readsProvisioned / 2,
    eventualConsistencyWrites: writesProvisioned,
    strongConsistencyReadsSavings: readsProvisionedSavings,
    strongConsistencyWritesSavings: writesProvisionedSavings,
    eventualConsistencyReadsSavings: readsProvisionedSavings / 2,
    eventualConsistencyWritesSavings: writesProvisionedSavings,
    strongConsistencyStorage: totalStorage,
    strongConsistencyBackup: totalBackup,
    eventualConsistencyStorage: totalStorage,
    eventualConsistencyBackup: totalBackup,
    strongConsistencyTtlDeletesPrice: totalTtl,
    eventualConsistencyTtlDeletesPrice: totalTtl,
  };
  const onDemandPricing: ProvisionedPricing = {
    strongConsistencyReads: readsOnDemand,
    strongConsistencyWrites: writesOnDemand,
    eventualConsistencyReads: readsOnDemand / 2,
    eventualConsistencyWrites: writesOnDemand,
    strongConsistencyReadsSavings: readsOnDemandSavings,
    strongConsistencyWritesSavings: writesOnDemandSavings,
    eventualConsistencyReadsSavings: readsOnDemandSavings / 2,
    eventualConsistencyWritesSavings: writesOnDemandSavings,
    strongConsistencyStorage: totalStorage,
    strongConsistencyBackup: totalBackup,
    eventualConsistencyStorage: totalStorage,
    eventualConsistencyBackup: totalBackup,
    strongConsistencyTtlDeletesPrice: totalTtl,
    eventualConsistencyTtlDeletesPrice: totalTtl,
  };
  return { provisionedPricing, onDemandPricing };
};

// --- Cassandra / price formulas ---

export const isSystemKeyspace = (keyspaceName: string): 'system' | 'user' =>
  system_keyspaces.has(keyspaceName) ? 'system' : 'user';

export const calculateWriteOperationsPerNodePerMonth = (total_writes_per_node: number, node_uptime_seconds: number): number =>
  (total_writes_per_node / node_uptime_seconds) * SECONDS_PER_MONTH;

export const calculateReadOperationsPerNodePerMonth = (total_reads_per_node: number, node_uptime_seconds: number): number =>
  (total_reads_per_node / node_uptime_seconds) * SECONDS_PER_MONTH;

export const calculateUncompressedStoragePerNode = (table_live_space_gb: number, compression_ratio: number): number =>
  table_live_space_gb / compression_ratio;

export const calculateOnDemandCapcityTotalMonthlyCostWithAggregates = (
  onDemandReadCost: number,
  onDemandWriteCost: number,
  onDemandTtlDeleteCost: number,
  storageCost: number,
  backupCost: number
): number =>
  onDemandReadCost + onDemandWriteCost + onDemandTtlDeleteCost + storageCost + backupCost;

export const calculateProvisionedCapacityTotalMonthlyCostWithAggregates = (
  provisionedReadCost: number,
  provisionedWriteCost: number,
  ttlDeleteCost: number,
  storageCost: number,
  backupCost: number
): number =>
  provisionedReadCost + provisionedWriteCost + ttlDeleteCost + storageCost + backupCost;

export const calculateProvisionedReadCostPerMonth = (
  reads_per_second: number,
  avg_read_row_size_bytes: number,
  readRequestPricePerHour: number,
  target_utilization = DEFAULT_TARGET_UTILIZATION,
  rows_per_read = 1
): number =>
  (reads_per_second * calculateReadUnitsPerOperation(avg_read_row_size_bytes, rows_per_read) * HOURS_PER_MONTH * readRequestPricePerHour) / target_utilization;

export const calculateProvisionedWriteCostPerMonth = (
  writes_per_second: number,
  avg_write_row_size_bytes: number,
  writeRequestPricePerHour: number,
  target_utilization = DEFAULT_TARGET_UTILIZATION
): number =>
  (writes_per_second * calculateWriteUnitsPerOperation(avg_write_row_size_bytes) * HOURS_PER_MONTH * writeRequestPricePerHour) / target_utilization;

export const calculateStorageCostPerMonth = (uncompressed_single_replica_gb: number, storagePricePerGB: number): number =>
  uncompressed_single_replica_gb * storagePricePerGB;

export const calculateBackupCostPerMonth = (uncompressed_single_replica_gb: number, pitrPricePerGB: number): number =>
  uncompressed_single_replica_gb * pitrPricePerGB;

export const calculateOnDemandReadUnitsPerMonthCost = (
  reads_per_second: number,
  avg_read_row_size_bytes: number,
  onDemandReadPrice: number,
  rows_per_read = 1
): number =>
  calcualteOnDemandReadUnitsPerMonth(reads_per_second, avg_read_row_size_bytes, rows_per_read) * onDemandReadPrice;

export const calculateOnDemandWriteUnitsPerMonthCost = (
  writes_per_second: number,
  avg_write_row_size_bytes: number,
  onDemandWritePrice: number
): number =>
  calculateOnDemandWriteUnitsPerMonth(writes_per_second, avg_write_row_size_bytes) * onDemandWritePrice;

export const calculateTtlUnitsPerMonthCost = (
  ttls_per_second: number,
  avg_write_row_size_bytes: number,
  ttlDeletesPrice: number
): number =>
  calculateOnDemandTtlUnitsPerMonth(ttls_per_second, avg_write_row_size_bytes) * ttlDeletesPrice;

export const calcualteOnDemandReadUnitsPerMonth = (
  reads_per_second: number,
  avg_read_row_size_bytes: number,
  rows_per_read = 1
): number =>
  reads_per_second * calculateReadUnitsPerOperation(avg_read_row_size_bytes, rows_per_read) * SECONDS_PER_MONTH;

export const calculateOnDemandWriteUnitsPerMonth = (writes_per_second: number, avg_write_row_size_bytes: number): number =>
  writes_per_second * calculateWriteUnitsPerOperation(avg_write_row_size_bytes) * SECONDS_PER_MONTH;

export const calculateOnDemandTtlUnitsPerMonth = (ttls_per_second: number, avg_write_row_size_bytes: number): number =>
  ttls_per_second * calculateTtlUnitsPerOperation(avg_write_row_size_bytes) * SECONDS_PER_MONTH;

export const calculateWriteUnitsPerOperation = (avg_write_row_size_bytes: number): number =>
  Math.ceil(avg_write_row_size_bytes / 1024);

/** Largest result one read page returns; bigger reads span several pages. */
export const READ_PAGE_SIZE_BYTES = 1024 * 1024;

/**
 * Read request units for one read. A read is billed on the bytes it returns, aggregated across all rows
 * it reads (not per row): each page (up to READ_PAGE_SIZE_BYTES) is ceil(page_bytes / 4 KB) units.
 */
export const calculateReadUnitsPerOperation = (avg_read_row_size_bytes: number, rows_per_read = 1): number => {
  const rows = Number.isFinite(rows_per_read) && rows_per_read > 0 ? rows_per_read : 1;
  const bytes = avg_read_row_size_bytes * rows;
  const fullPages = Math.floor(bytes / READ_PAGE_SIZE_BYTES);
  const remainder = bytes - fullPages * READ_PAGE_SIZE_BYTES;
  return fullPages * Math.ceil(READ_PAGE_SIZE_BYTES / 4096) + Math.ceil(remainder / 4096);
};

export const calculateTtlUnitsPerOperation = (avg_write_row_size_bytes: number): number =>
  Math.ceil(avg_write_row_size_bytes / 1024);

// --- Formula explanations ---

const fmtNum = (n: number): string =>
  Number.isInteger(n) ? n.toLocaleString('en-US') : n.toLocaleString('en-US', { maximumFractionDigits: 4 });
const fmtPrice = (n: number | string): string =>
  `$${Number(n).toLocaleString('en-US', { maximumSignificantDigits: 10 })}`;
const fmtUsd = (n: number): string =>
  `$${n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;

/** "ceil(...) = N" for one read, including the 1 MB page split when the read spans several pages. */
const explainReadUnits = (rowBytes: number, rowsPerRead: number): string => {
  const units = calculateReadUnitsPerOperation(rowBytes, rowsPerRead);
  const bytes = rowBytes * rowsPerRead;
  const bytesExpr = rowsPerRead === 1 ? fmtNum(rowBytes) : `${fmtNum(rowBytes)} × ${fmtNum(rowsPerRead)} rows`;
  const fullPages = Math.floor(bytes / READ_PAGE_SIZE_BYTES);
  if (fullPages === 0) return `ceil(${bytesExpr} / 4,096) = ${fmtNum(units)}`;
  const remainder = bytes - fullPages * READ_PAGE_SIZE_BYTES;
  return (
    `${bytesExpr} = ${fmtNum(bytes)} bytes → ${fmtNum(fullPages)} full 1 MB page(s) × 256 + ` +
    `ceil(${fmtNum(remainder)} / 4,096) = ${fmtNum(units)}`
  );
};

/**
 * Write out every pricing formula with the estimate's own numbers substituted, per datacenter and keyspace.
 * Values come from the same functions calculatePricingEstimate uses, so the lines add up to its totals.
 */
export const explainPricingEstimate = (
  datacenters: DatacenterRef[],
  regions: Record<string, string>,
  estimateResults: EstimateResults
): Record<string, Record<string, string[]>> => {
  const out: Record<string, Record<string, string[]>> = {};
  for (const dc of datacenters) {
    const region = regions[dc.name];
    const results = estimateResults[dc.name];
    const p = region ? getRegionPricing(region) : null;
    if (!results || !p) continue;
    const sp = savingsPlanMap[region];
    out[dc.name] = {};

    for (const [keyspace, d] of Object.entries(results)) {
      const rows = d.avg_rows_per_read ?? 1;
      const ru = calculateReadUnitsPerOperation(d.avg_read_row_size_bytes, rows);
      const wu = calculateWriteUnitsPerOperation(d.avg_write_row_size_bytes);
      const tu = calculateTtlUnitsPerOperation(d.avg_write_row_size_bytes);
      const gb = d.uncompressed_single_replica_gb;

      const odR = calculateOnDemandReadUnitsPerMonthCost(d.reads_per_second, d.avg_read_row_size_bytes, p.readRequestPrice, rows);
      const odW = calculateOnDemandWriteUnitsPerMonthCost(d.writes_per_second, d.avg_write_row_size_bytes, p.writeRequestPrice);
      const pvR = calculateProvisionedReadCostPerMonth(d.reads_per_second, d.avg_read_row_size_bytes, p.readRequestPricePerHour, DEFAULT_TARGET_UTILIZATION, rows);
      const pvW = calculateProvisionedWriteCostPerMonth(d.writes_per_second, d.avg_write_row_size_bytes, p.writeRequestPricePerHour, DEFAULT_TARGET_UTILIZATION);
      const ttl = calculateTtlUnitsPerMonthCost(d.ttls_per_second, d.avg_write_row_size_bytes, p.ttlDeletesPrice);
      const storage = calculateStorageCostPerMonth(gb, p.storagePricePerGB);
      const backup = d.use_backup === true ? calculateBackupCostPerMonth(gb, p.pitrPricePerGB) : 0;
      const shared = ttl + storage + backup;

      const lines: string[] = [
        `Region: ${region} (list prices from mcs.json, published ${PRICE_LIST_DATE})`,
        `Workload: ${fmtNum(d.reads_per_second)} reads/s, ${fmtNum(d.writes_per_second)} writes/s, ` +
          `${fmtNum(d.ttls_per_second)} TTL deletes/s, ${fmtNum(d.avg_read_row_size_bytes)} B rows, ` +
          `${fmtNum(rows)} row(s) per read, ${fmtNum(gb)} GB, PITR ${d.use_backup ? 'on' : 'off'}`,
        `Write units per write = ceil(${fmtNum(d.avg_write_row_size_bytes)} / 1,024) = ${fmtNum(wu)}`,
        `Read units per read = ${explainReadUnits(d.avg_read_row_size_bytes, rows)}`,
        `On-demand reads = ${fmtNum(d.reads_per_second)} × ${fmtNum(ru)} × 2,628,000 s × ${fmtPrice(p.readRequestPrice)} = ${fmtUsd(odR)}`,
        `On-demand writes = ${fmtNum(d.writes_per_second)} × ${fmtNum(wu)} × 2,628,000 s × ${fmtPrice(p.writeRequestPrice)} = ${fmtUsd(odW)}`,
        `Provisioned reads = ${fmtNum(d.reads_per_second)} × ${fmtNum(ru)} × 730 h × ${fmtPrice(p.readRequestPricePerHour)} ÷ ${DEFAULT_TARGET_UTILIZATION.toFixed(2)} target utilization = ${fmtUsd(pvR)}`,
        `Provisioned writes = ${fmtNum(d.writes_per_second)} × ${fmtNum(wu)} × 730 h × ${fmtPrice(p.writeRequestPricePerHour)} ÷ ${DEFAULT_TARGET_UTILIZATION.toFixed(2)} target utilization = ${fmtUsd(pvW)}`,
        `TTL deletes = ${fmtNum(d.ttls_per_second)} × ${fmtNum(tu)} × 2,628,000 s × ${fmtPrice(p.ttlDeletesPrice)} = ${fmtUsd(ttl)}`,
        `Storage = ${fmtNum(gb)} GB × ${fmtPrice(p.storagePricePerGB)} = ${fmtUsd(storage)}`,
        d.use_backup === true
          ? `PITR = ${fmtNum(gb)} GB × ${fmtPrice(p.pitrPricePerGB)} = ${fmtUsd(backup)}`
          : 'PITR = off = $0.00',
        `On-demand total = ${fmtUsd(odR)} + ${fmtUsd(odW)} + ${fmtUsd(ttl)} + ${fmtUsd(storage)} + ${fmtUsd(backup)} = ${fmtUsd(odR + odW + shared)}`,
        `Provisioned total = ${fmtUsd(pvR)} + ${fmtUsd(pvW)} + ${fmtUsd(ttl)} + ${fmtUsd(storage)} + ${fmtUsd(backup)} = ${fmtUsd(pvR + pvW + shared)}`,
      ];

      if (sp) {
        const odRate = (k: string, list: number) => sp[k]?.rate ?? list;
        const odRs = calculateOnDemandReadUnitsPerMonthCost(d.reads_per_second, d.avg_read_row_size_bytes, odRate('ReadRequestUnits', p.readRequestPrice), rows);
        const odWs = calculateOnDemandWriteUnitsPerMonthCost(d.writes_per_second, d.avg_write_row_size_bytes, odRate('WriteRequestUnits', p.writeRequestPrice));
        const pvRs = calculateProvisionedReadCostPerMonth(d.reads_per_second, d.avg_read_row_size_bytes, odRate('ReadCapacityUnitHrs', p.readRequestPricePerHour), DEFAULT_TARGET_UTILIZATION, rows);
        const pvWs = calculateProvisionedWriteCostPerMonth(d.writes_per_second, d.avg_write_row_size_bytes, odRate('WriteCapacityUnitHrs', p.writeRequestPricePerHour), DEFAULT_TARGET_UTILIZATION);
        lines.push(
          `Savings plan: 1-year No Upfront Database Savings Plan rates from savings-plans.json`,
          `On-demand + savings plan = reads ${fmtUsd(odRs)} (rate ${fmtPrice(odRate('ReadRequestUnits', p.readRequestPrice))}) + ` +
            `writes ${fmtUsd(odWs)} (rate ${fmtPrice(odRate('WriteRequestUnits', p.writeRequestPrice))}) + TTL, storage, PITR at list price = ${fmtUsd(odRs + odWs + shared)}`,
          `Provisioned + savings plan = reads ${fmtUsd(pvRs)} (rate ${fmtPrice(odRate('ReadCapacityUnitHrs', p.readRequestPricePerHour))}) + ` +
            `writes ${fmtUsd(pvWs)} (rate ${fmtPrice(odRate('WriteCapacityUnitHrs', p.writeRequestPricePerHour))}) + TTL, storage, PITR at list price = ${fmtUsd(pvRs + pvWs + shared)}`
        );
      } else {
        lines.push('Savings plans: no savings-plan rates for this Region.');
      }
      out[dc.name][keyspace] = lines;
    }
  }
  return out;
};

export default calculatePricingEstimate;
