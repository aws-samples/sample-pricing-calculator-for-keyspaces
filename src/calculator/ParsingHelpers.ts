export const HOURS_PER_MONTH = (365 / 12) * 24;
export const WRITE_UNIT_SIZE = 1024; // 1KB
export const READ_UNIT_SIZE = 4096; // 4KB

// --- Return types ---

export interface TablestatsData {
  space_used: number;
  compression_ratio: number;
  read_count: number;
  write_count: number;
}

export interface NodetoolInfoResult {
  uptime_seconds: number;
  dc: string;
  id: string;
}

interface KeyspaceSchemaEntry {
  class: string;
  datacenters: Record<string, number>;
  tables: string[];
}

export type SchemaInfo = Record<string, KeyspaceSchemaEntry>;
export type RowSizeInfo = Record<string, Record<string, string>>;
export type TablestatsResult = Record<string, Record<string, TablestatsData>>;

// --- Compatibility types ---

interface TableCompatibilityIssue {
  indexes: string[];
  triggers: string[];
  materializedViews: string[];
  shortColumnNames: string[];
}

export interface UdtCompatibilityIssue {
  name: string;
  keyspace: string;
  issues: string[];
}

export interface UdtQuotasSummary {
  total_udts: number;
  exceeds_region_limit: boolean;
  issues: UdtCompatibilityIssue[];
}

export interface RowSizeIssue {
  table: string;
  max_bytes: number;
}

export interface CompatibilityInfo {
  functions: number;
  aggregates: number;
  keyspaces: Record<string, Record<string, TableCompatibilityIssue>>;
  udt_quotas: UdtQuotasSummary | null;
  row_size_issues: RowSizeIssue[];
}

// --- Prepared-statement compatibility types ---

export interface QueryPatternIssue {
  prepared_id: string;
  query_string: string;
}

export interface AggregationIssue extends QueryPatternIssue {
  function: string; // e.g. 'COUNT', 'MIN'
}

export interface TtlTableInfo {
  uses_ttl: true;
  ttl_values: number[];
}

export interface QueryPatternsInfo {
  lwt_in_unlogged_batch: QueryPatternIssue[];
  aggregations: AggregationIssue[];
  ttl_tables: Record<string, TtlTableInfo>; // key is "ks.table" (lowercased)
}

interface TcoSingleNode {
  instance: { monthly_cost: number; [key: string]: unknown };
  storage?: { monthly_cost: number; [key: string]: unknown };
  backup?: { monthly_cost: number; [key: string]: unknown };
  network_out?: { monthly_cost: number; [key: string]: unknown };
  network_in?: { monthly_cost: number; [key: string]: unknown };
  license?: { monthly_cost: number; [key: string]: unknown };
}

interface TcoOperations {
  operator_hours: { monthly_cost: number; [key: string]: unknown };
}

export interface TcoData {
  single_node: TcoSingleNode;
  operations: TcoOperations;
}

// --- Parsers ---

export const parseNodetoolStatus = (content: string): Map<string, number> => {
  const lines = content.split('\n');
  const datacenters = new Map<string, number>();
  let currentDC: string | null = null;
  let nodeCount = 0;

  for (const line of lines) {
    const trimmedLine = line.trim();

    if (/Datacenter\s*:/i.test(trimmedLine)) {
      if (currentDC) {
        datacenters.set(currentDC, nodeCount);
      }
      const match = trimmedLine.match(/Datacenter\s*:\s*(.+)/i);
      if (match?.[1]) {
        currentDC = match[1].trim();
        nodeCount = 0;
      }
    } else if (currentDC && (/^UN\b/i.test(trimmedLine) || /^DN\b/i.test(trimmedLine))) {
      nodeCount++;
    }
  }

  if (currentDC) {
    datacenters.set(currentDC, nodeCount);
  }

  return datacenters;
};

/** Shared = one tablestats bundle scaled across all status datacenters; perDc = separate aggregation per DC. */
export type CassandraBundleMode = 'shared' | 'perDc';

/**
 * Ensure every datacenter listed in `nodetool status` has at least one node entry for sampling math.
 * Missing DCs get a synthetic node using template uptime from any existing info capture, else monthly default.
 */
export const mergeTopologyWithStatus = (
  statusData: Map<string, number>,
  nodesByDc: Map<string, Array<{ id: string; uptime_seconds: number }>>
): void => {
  const SECONDS_PER_MONTH = (365 / 12) * 86400;
  let templateUptime = SECONDS_PER_MONTH;
  for (const nodes of Array.from(nodesByDc.values())) {
    if (nodes.length > 0) {
      templateUptime = nodes[0].uptime_seconds;
      break;
    }
  }
  let synth = 0;
  for (const dc of Array.from(statusData.keys())) {
    const existing = nodesByDc.get(dc);
    if (!existing || existing.length === 0) {
      nodesByDc.set(dc, [{ id: `synthetic-${synth++}`, uptime_seconds: templateUptime }]);
    }
  }
};

/**
 * Single classified tablestats path + multi-DC status → shared-bundle scale-factor workflow.
 * More than one tablestats file → per-DC / per-node captures (no extra cluster-wide scale factor).
 */
export const inferCassandraBundleMode = (
  statusData: Map<string, number>,
  tablestatsPathCount: number
): CassandraBundleMode => {
  if (statusData.size <= 1) return 'perDc';
  return tablestatsPathCount === 1 ? 'shared' : 'perDc';
};

/** Parse `dc1=100,dc2=200` into datacenter → reads/s. */
export const parseReadsOverrideCsv = (raw: string): Map<string, number> => {
  const out = new Map<string, number>();
  for (const part of raw.split(',')) {
    const seg = part.trim();
    if (!seg) continue;
    const eq = seg.indexOf('=');
    if (eq <= 0) continue;
    const dc = seg.slice(0, eq).trim();
    const v = Number(seg.slice(eq + 1).trim());
    if (!dc || Number.isNaN(v) || v < 0) continue;
    out.set(dc, v);
  }
  return out;
};

/**
 * Estimated bytes per column type, for SQL types (Mode 4 data models) and CQL types (Cassandra schemas).
 * Same table as the skill's SQL type table; CQL types use the closest SQL size. `VARCHAR(n)` / `CHAR(n)` /
 * `VARBINARY(n)` use n bytes. Collections, frozen types, tuples, UDTs and unknown types use 128 bytes.
 */
export const estimateColumnBytes = (columnType: string): number => {
  const t = columnType.trim().toLowerCase();
  const sized = t.match(/^(n?varchar|n?char|character varying|character|varbinary|binary)\s*\(\s*(\d+)\s*\)/);
  if (sized) return Number(sized[2]);
  if (/^(frozen|list|set|map|tuple)\s*</.test(t)) return 128;
  const base = t.replace(/\s*\(.*$/, '').trim();
  const sizes: Record<string, number> = {
    bool: 1, boolean: 1, tinyint: 1, smallint: 2,
    int: 4, integer: 4, serial: 4, date: 4, float: 4, real: 4,
    bigint: 8, bigserial: 8, double: 8, 'double precision': 8, timestamp: 8, datetime: 8, time: 8,
    counter: 8, decimal: 8, numeric: 8,
    uuid: 16, timeuuid: 16, inet: 16, varint: 16, duration: 16,
    text: 64, varchar: 64, nvarchar: 64, ascii: 64, clob: 64, 'character varying': 64,
    blob: 512, binary: 512, varbinary: 512, bytea: 512,
  };
  return sizes[base] ?? 128;
};

/** CQL types use the same estimate (kept for callers that compare CQL rows, e.g. view vs base table). */
export const estimateCqlColumnBytes = estimateColumnBytes;

/** Estimated bytes for a row with these column types (sum of estimateColumnBytes). */
export const estimateCqlRowBytes = (columnTypes: string[]): number =>
  columnTypes.reduce((sum, t) => sum + estimateColumnBytes(t), 0);

/** Materialized views in a schema: "ks.view" → "ks.base_table". */
export const parseMaterializedViewBases = (schemaContent: string): Map<string, string> => {
  const out = new Map<string, string>();
  const mvPattern = /CREATE\s+MATERIALIZED\s+VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?"?(\w+)"?\."?(\w+)"?\s+AS\s+SELECT\s+[\s\S]*?\s+FROM\s+"?(\w+)"?\."?(\w+)"?/gi;
  let m: RegExpExecArray | null;
  while ((m = mvPattern.exec(schemaContent || '')) !== null) {
    out.set(`${m[1]}.${m[2]}`, `${m[3]}.${m[4]}`);
  }
  return out;
};

/** Parse `dc1=on,dc2=off` into datacenter / Region → PITR enabled. Accepts on/off, true/false, yes/no. */
export const parsePitrOverrideCsv = (raw: string): Map<string, boolean> => {
  const out = new Map<string, boolean>();
  for (const part of (raw || '').split(',')) {
    const seg = part.trim();
    const eq = seg.indexOf('=');
    if (eq <= 0) continue;
    const dc = seg.slice(0, eq).trim();
    const v = seg.slice(eq + 1).trim().toLowerCase();
    if (['on', 'true', 'yes', '1'].includes(v)) out.set(dc, true);
    else if (['off', 'false', 'no', '0'].includes(v)) out.set(dc, false);
  }
  return out;
};

/** Map nodetool status datacenter names to NetworkTopologyStrategy keys in DESCRIBE SCHEMA. */
export const resolveStatusDcToSchemaDc = (statusDc: string, schemaDcKeys: string[]): string | null => {
  if (schemaDcKeys.length === 0) return null;
  if (schemaDcKeys.includes(statusDc)) return statusDc;
  const lower = statusDc.toLowerCase();
  const ci = schemaDcKeys.find((k) => k.toLowerCase() === lower);
  if (ci) return ci;
  // Only map a -seeds DC to another -seeds schema key (never to a base-region schema key).
  const stripped = statusDc.replace(/-seeds$/i, '');
  if (stripped !== statusDc) {
    const byStrip = schemaDcKeys.find(
      (k) => /-seeds$/i.test(k) && k.replace(/-seeds$/i, '') === stripped
    );
    if (byStrip) return byStrip;
  }
  return null;
};

export type InferReadsMode = 'same' | 'zero' | 'measured';

export const parse_nodetool_tablestats = (content: string): TablestatsResult => {
  const lines = content.split('\n');
  const data: TablestatsResult = {};
  let currentKeyspace: string | null = null;
  let currentTable: string | null = null;
  let spaceUsed: number | null = null;
  let compressionRatio: number | null = null;
  let writeCount: number | null = null;
  let readCount: number | null = null;

  for (const line of lines) {
    const trimmedLine = line.trim();

    if (trimmedLine.startsWith('Keyspace')) {
      const keyspaceMatch = trimmedLine.match(/Keyspace\s*:\s*(.+)/);
      if (keyspaceMatch?.[1]) {
        currentKeyspace = keyspaceMatch[1].trim();
        if (!data[currentKeyspace]) {
          data[currentKeyspace] = {};
        }
      } else {
        currentKeyspace = null;
      }
      currentTable = null;
    }

    if (currentKeyspace && (trimmedLine.startsWith('Table:') || trimmedLine.startsWith('Table (index):'))) {
      const tableMatch = trimmedLine.match(/Table(?:\s*\(index\))?\s*:\s*(.+)/);
      if (tableMatch?.[1]) {
        currentTable = tableMatch[1].trim();
        spaceUsed = null;
        compressionRatio = null;
        writeCount = null;
        readCount = null;
      }
    }

    if (currentKeyspace && currentTable) {
      if (trimmedLine.includes('Space used (live):')) {
        const match = trimmedLine.match(/Space used \(live\)\s*:\s*(.+)/);
        if (match?.[1]) {
          spaceUsed = parseFloat(match[1].trim()) || 0;
        }
      } else if (trimmedLine.includes('SSTable Compression Ratio:')) {
        const match = trimmedLine.match(/SSTable Compression Ratio\s*:\s*(.+)/);
        if (match?.[1]) {
          const parsed = parseFloat(match[1].trim());
          compressionRatio = (!isNaN(parsed) && parsed > 0) ? parsed : 1;
        }
      } else if (trimmedLine.includes('Local read count:')) {
        const match = trimmedLine.match(/Local read count\s*:\s*(.+)/);
        if (match?.[1]) {
          readCount = parseFloat(match[1].trim()) || 0;
        }
      } else if (trimmedLine.includes('Local write count:')) {
        const match = trimmedLine.match(/Local write count\s*:\s*(.+)/);
        if (match?.[1]) {
          writeCount = parseFloat(match[1].trim()) || 0;
        }

        if (
          spaceUsed !== null &&
          compressionRatio !== null &&
          readCount !== null &&
          writeCount !== null
        ) {
          data[currentKeyspace][currentTable] = {
            space_used: spaceUsed,
            compression_ratio: compressionRatio,
            read_count: readCount,
            write_count: writeCount,
          };
          currentTable = null;
          spaceUsed = null;
          compressionRatio = null;
          writeCount = null;
          readCount = null;
        }
      }
    }
  }

  return data;
};

export const parseNodetoolInfo = (content: string): NodetoolInfoResult => {
  const lines = content.split('\n');
  let uptimeSeconds = 1;
  let id = '';
  let dc = '';

  for (const line of lines) {
    const trimmedLine = line.trim();

    if (/Uptime\s*\(seconds\)/i.test(trimmedLine)) {
      const match = trimmedLine.match(/Uptime\s*\(seconds\)\s*:\s*(.+)/i);
      if (match?.[1]) {
        const parsed = parseFloat(match[1].trim());
        if (isNaN(parsed)) {
          throw new Error(`Error parsing uptime in seconds: ${match[1].trim()}`);
        }
        uptimeSeconds = parsed;
      }
    }

    if (/^ID\s*:/i.test(trimmedLine)) {
      const match = trimmedLine.match(/^ID\s*:\s*(.+)/i);
      if (match?.[1]) {
        id = match[1].trim();
      }
    }

    if (/Data\s+Center\s*:/i.test(trimmedLine)) {
      const match = trimmedLine.match(/Data\s+Center\s*:\s*(.+)/i);
      if (match?.[1]) {
        dc = match[1].trim();
      }
    }
  }

  return { uptime_seconds: uptimeSeconds, dc, id };
};

export const parse_cassandra_schema = (schemaContent: string, datacenter: string): SchemaInfo => {
  const ksPattern = /CREATE KEYSPACE (\w+)\s+WITH replication = \{[^}]*'class': '(\w+)'(?:,\s*)?([^}]*)\}/gi;
  const tablePattern = /CREATE TABLE (\w+)\.(\w+)/gi;

  const keyspaces: Array<{ name: string; class: string; rest: string }> = [];
  let ksMatch: RegExpExecArray | null;
  while ((ksMatch = ksPattern.exec(schemaContent)) !== null) {
    keyspaces.push({ name: ksMatch[1], class: ksMatch[2], rest: ksMatch[3] });
  }

  const tables: Array<{ keyspace: string; table: string }> = [];
  let tableMatch: RegExpExecArray | null;
  while ((tableMatch = tablePattern.exec(schemaContent)) !== null) {
    tables.push({ keyspace: tableMatch[1], table: tableMatch[2] });
  }

  const ksInfo: SchemaInfo = {};
  for (const ks of keyspaces) {
    const dcRepl: Record<string, number> = {};
    if (ks.class === 'NetworkTopologyStrategy') {
      const dcEntries = ks.rest.match(/'([^']+)':\s*'(\d+)'/g);
      if (dcEntries) {
        for (const entry of dcEntries) {
          const entryMatch = entry.match(/'([^']+)':\s*'(\d+)'/);
          if (entryMatch) {
            dcRepl[entryMatch[1]] = parseInt(entryMatch[2], 10);
          }
        }
      }
    } else if (ks.class === 'SimpleStrategy') {
      const rfMatch = ks.rest.match(/'replication_factor':\s*'(\d+)'/);
      if (rfMatch) {
        dcRepl[datacenter] = parseInt(rfMatch[1], 10);
      }
    }
    ksInfo[ks.name] = { class: ks.class, datacenters: dcRepl, tables: [] };
  }

  for (const table of tables) {
    if (ksInfo[table.keyspace]) {
      ksInfo[table.keyspace].tables.push(table.table);
    }
  }

  return ksInfo;
};

/**
 * Scan a CQL schema dump for features unsupported by Amazon Keyspaces:
 *   - CREATE INDEX        → table-level
 *   - CREATE TRIGGER      → table-level
 *   - CREATE MATERIALIZED VIEW → table-level (attached to the base table)
 *   - CREATE FUNCTION     → keyspace-level (counted globally)
 *   - CREATE AGGREGATE    → keyspace-level (counted globally)
 */
export const parse_cassandra_schema_compatibility = (schemaContent: string, rowSizeData?: RowSizeInfo): CompatibilityInfo => {
  const MAX_ROW_SIZE_BYTES = 1024 * 1024; // 1 MB

  const result: CompatibilityInfo = {
    functions: 0,
    aggregates: 0,
    keyspaces: {},
    udt_quotas: null,
    row_size_issues: [],
  };

  const ensureTable = (ks: string, table: string): TableCompatibilityIssue => {
    if (!result.keyspaces[ks]) result.keyspaces[ks] = {};
    if (!result.keyspaces[ks][table]) {
      result.keyspaces[ks][table] = { indexes: [], triggers: [], materializedViews: [], shortColumnNames: [] };
    }
    return result.keyspaces[ks][table];
  };

  // CREATE [CUSTOM] INDEX [IF NOT EXISTS] <name> ON <ks>.<table> ...
  const indexPattern = /CREATE\s+(?:CUSTOM\s+)?INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)\s+ON\s+(\w+)\.(\w+)/gi;
  let m: RegExpExecArray | null;
  while ((m = indexPattern.exec(schemaContent)) !== null) {
    const [, indexName, ks, table] = m;
    ensureTable(ks, table).indexes.push(indexName);
  }

  // CREATE TRIGGER [IF NOT EXISTS] <name> ON <ks>.<table> ...
  const triggerPattern = /CREATE\s+TRIGGER\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)\s+ON\s+(\w+)\.(\w+)/gi;
  while ((m = triggerPattern.exec(schemaContent)) !== null) {
    const [, triggerName, ks, table] = m;
    ensureTable(ks, table).triggers.push(triggerName);
  }

  // CREATE MATERIALIZED VIEW [IF NOT EXISTS] <ks>.<view> AS SELECT ... FROM <ks>.<base_table>
  const mvPattern = /CREATE\s+MATERIALIZED\s+VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)\.(\w+)\s+AS\s+SELECT\s+[\s\S]*?\s+FROM\s+(\w+)\.(\w+)/gi;
  while ((m = mvPattern.exec(schemaContent)) !== null) {
    const [, , viewName, baseKs, baseTable] = m;
    ensureTable(baseKs, baseTable).materializedViews.push(viewName);
  }

  // Column names shorter than 2 characters are incompatible with Amazon Keyspaces
  // (CloudFormation pattern: ^[a-zA-Z0-9][a-zA-Z0-9_]{1,47}$)
  const tableColPattern = /CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)\.(\w+)\s*\(([\s\S]*?)\)\s*(?:WITH|;)/gi;
  let tcm: RegExpExecArray | null;
  while ((tcm = tableColPattern.exec(schemaContent)) !== null) {
    const ks = tcm[1];
    const table = tcm[2];
    const body = tcm[3];
    const lines = body.split(',');
    for (const line of lines) {
      const trimmed = line.trim();
      if (/^\s*PRIMARY\s+KEY/i.test(trimmed)) continue;
      const colMatch = trimmed.match(/^"?(\w+)"?\s+/);
      if (colMatch) {
        const colName = colMatch[1];
        if (colName.length < 2) {
          ensureTable(ks, table).shortColumnNames.push(colName);
        }
      }
    }
  }

  // CREATE [OR REPLACE] FUNCTION [IF NOT EXISTS] <ks>.<name> ...
  const functionPattern = /CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)\.(\w+)/gi;
  while ((m = functionPattern.exec(schemaContent)) !== null) {
    result.functions++;
  }

  // CREATE [OR REPLACE] AGGREGATE [IF NOT EXISTS] <ks>.<name> ...
  const aggregatePattern = /CREATE\s+(?:OR\s+REPLACE\s+)?AGGREGATE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)\.(\w+)/gi;
  while ((m = aggregatePattern.exec(schemaContent)) !== null) {
    result.aggregates++;
  }

  // --- UDT quota checks ---
  const UDT_MAX_PER_REGION = 256;
  const UDT_MAX_NESTING_DEPTH = 8;
  const UDT_MAX_CHILD_UDTS = 10;
  const UDT_MAX_PARENT_UDTS = 10;
  const UDT_MAX_SCHEMA_SIZE_BYTES = 25 * 1024;
  const UDT_MAX_NAME_LENGTH = 48;
  const UDT_MAX_FIELD_NAME_LENGTH = 128;
  const UDT_MAX_PER_TABLE = 50;

  interface ParsedUdtInfo {
    keyspace: string;
    name: string;
    fields: Array<{ name: string; type: string }>;
    schemaSize: number;
    childUdts: string[];
  }

  const udtPattern = /CREATE\s+TYPE\s+(?:IF\s+NOT\s+EXISTS\s+)?"?(\w+)"?\."?(\w+)"?\s*\(([\s\S]*?)\);/gi;
  const allUdts: ParsedUdtInfo[] = [];
  let um: RegExpExecArray | null;
  while ((um = udtPattern.exec(schemaContent)) !== null) {
    const ks = um[1];
    const name = um[2];
    const body = um[3];
    const fields: Array<{ name: string; type: string }> = [];
    const childUdts: string[] = [];

    // Split on top-level commas (respecting angle brackets for nested types)
    let depth = 0;
    let current = '';
    for (const ch of body) {
      if (ch === '<' || ch === '(') depth++;
      else if (ch === '>' || ch === ')') depth--;
      if (ch === ',' && depth === 0) {
        const trimmed = current.trim();
        if (trimmed) {
          const spaceIdx = trimmed.indexOf(' ');
          if (spaceIdx !== -1) {
            fields.push({ name: trimmed.slice(0, spaceIdx).replace(/"/g, ''), type: trimmed.slice(spaceIdx + 1).trim() });
          }
        }
        current = '';
      } else {
        current += ch;
      }
    }
    const lastTrimmed = current.trim();
    if (lastTrimmed) {
      const spaceIdx = lastTrimmed.indexOf(' ');
      if (spaceIdx !== -1) {
        fields.push({ name: lastTrimmed.slice(0, spaceIdx).replace(/"/g, ''), type: lastTrimmed.slice(spaceIdx + 1).trim() });
      }
    }

    // Estimate schema size: Keyspaces internal representation uses approximately
    // 185 bytes per field (includes binary type encoding, field name storage, and
    // metadata overhead). Calibrated against known rejection at 140 fields.
    const schemaSize = Math.max(body.length, fields.length * 185);

    // Detect child UDT references (frozen<udt_name> or bare udt_name in field types)
    const otherUdtNames = allUdts.filter((u) => u.keyspace === ks).map((u) => u.name);
    for (const f of fields) {
      for (const otherName of otherUdtNames) {
        const re = new RegExp(`\\b${otherName}\\b`, 'i');
        if (re.test(f.type) && !childUdts.includes(otherName)) {
          childUdts.push(otherName);
        }
      }
    }

    allUdts.push({ keyspace: ks, name, fields, schemaSize, childUdts });
  }

  // Now check for parent references (reverse of child)
  const parentCounts: Record<string, number> = {};
  for (const udt of allUdts) {
    const key = `${udt.keyspace}.${udt.name}`;
    if (!parentCounts[key]) parentCounts[key] = 0;
    for (const child of udt.childUdts) {
      const childKey = `${udt.keyspace}.${child}`;
      parentCounts[childKey] = (parentCounts[childKey] ?? 0) + 1;
    }
  }

  // Calculate nesting depth via BFS
  function calcNestingDepth(udt: ParsedUdtInfo): number {
    const udtMap = new Map(allUdts.filter((u) => u.keyspace === udt.keyspace).map((u) => [u.name, u]));
    let depth = 0;
    let current = [udt.name];
    const visited = new Set<string>();
    while (current.length > 0) {
      const next: string[] = [];
      for (const name of current) {
        if (visited.has(name)) continue;
        visited.add(name);
        const u = udtMap.get(name);
        if (u) {
          for (const child of u.childUdts) {
            if (!visited.has(child)) next.push(child);
          }
        }
      }
      if (next.length > 0) depth++;
      current = next;
    }
    return depth;
  }

  // Count UDTs referenced per table
  const udtNamesSet = new Set(allUdts.map((u) => u.name.toLowerCase()));
  const tableUdtPattern = /CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)\.(\w+)\s*\(([\s\S]*?)\)\s*(?:WITH|;)/gi;
  let tum: RegExpExecArray | null;
  const tablesReferencingUdt: Record<string, number> = {}; // udt name → table count
  while ((tum = tableUdtPattern.exec(schemaContent)) !== null) {
    const tableKs = tum[1];
    const tableName = tum[2];
    const tableBody = tum[3];
    const referencedUdts = new Set<string>();
    for (const udtName of udtNamesSet) {
      const re = new RegExp(`\\b${udtName}\\b`, 'i');
      if (re.test(tableBody)) {
        referencedUdts.add(udtName);
        tablesReferencingUdt[udtName] = (tablesReferencingUdt[udtName] ?? 0) + 1;
      }
    }
    if (referencedUdts.size > UDT_MAX_PER_TABLE) {
      ensureTable(tableKs, tableName);
    }
  }

  const udtIssues: UdtCompatibilityIssue[] = [];
  for (const udt of allUdts) {
    const issues: string[] = [];

    if (udt.name.length > UDT_MAX_NAME_LENGTH) {
      issues.push(`UDT name exceeds ${UDT_MAX_NAME_LENGTH} characters (${udt.name.length})`);
    }

    for (const f of udt.fields) {
      if (f.name.length > UDT_MAX_FIELD_NAME_LENGTH) {
        issues.push(`Field "${f.name}" exceeds ${UDT_MAX_FIELD_NAME_LENGTH} characters (${f.name.length})`);
      }
    }

    if (udt.schemaSize > UDT_MAX_SCHEMA_SIZE_BYTES) {
      issues.push(`UDT schema size likely exceeds 25 KB limit (estimated ${(udt.schemaSize / 1024).toFixed(1)} KB from ${udt.fields.length} fields)`);
    }

    if (udt.childUdts.length > UDT_MAX_CHILD_UDTS) {
      issues.push(`Exceeds max ${UDT_MAX_CHILD_UDTS} direct child UDTs (${udt.childUdts.length})`);
    }

    const key = `${udt.keyspace}.${udt.name}`;
    if ((parentCounts[key] ?? 0) > UDT_MAX_PARENT_UDTS) {
      issues.push(`Exceeds max ${UDT_MAX_PARENT_UDTS} direct parent UDTs (${parentCounts[key]})`);
    }

    const depth = calcNestingDepth(udt);
    if (depth > UDT_MAX_NESTING_DEPTH) {
      issues.push(`Nesting depth exceeds ${UDT_MAX_NESTING_DEPTH} levels (${depth})`);
    }

    const tableRefCount = tablesReferencingUdt[udt.name.toLowerCase()] ?? 0;
    if (tableRefCount > 100) {
      issues.push(`Referenced by more than 100 tables (${tableRefCount})`);
    }

    if (issues.length > 0) {
      udtIssues.push({ name: udt.name, keyspace: udt.keyspace, issues });
    }
  }

  result.udt_quotas = {
    total_udts: allUdts.length,
    exceeds_region_limit: allUdts.length > UDT_MAX_PER_REGION,
    issues: udtIssues,
  };

  // --- Max row size check (1 MB) using row size sampler data ---
  if (rowSizeData) {
    for (const [tableName, metrics] of Object.entries(rowSizeData)) {
      const maxStr = metrics['max'] ?? metrics['maximum'];
      if (!maxStr) continue;
      const maxBytes = parseInt(maxStr, 10);
      if (!isNaN(maxBytes) && maxBytes > MAX_ROW_SIZE_BYTES) {
        result.row_size_issues.push({ table: tableName, max_bytes: maxBytes });
      }
    }
  }

  return result;
};

/**
 * Parse the output of `SELECT JSON * FROM system.prepared_statements` (or
 * the newline-delimited JSON produced by prepared-statements-sampler.sh)
 * and detect Amazon Keyspaces compatibility concerns in the query text:
 *
 *   - LWT inside `BEGIN UNLOGGED BATCH` (not supported)
 *   - Aggregate function calls (COUNT / MIN / MAX / SUM / AVG — not supported)
 *   - `USING TTL <n>` per target table (informational — used to populate
 *     has_ttl for pricing when the base schema has no default TTL)
 *
 * UDF usage is intentionally not detected here — `CREATE FUNCTION` in the
 * schema is the source of truth, surfaced by parse_cassandra_schema_compatibility.
 *
 * Accepts either:
 *   - NDJSON (one JSON object per line), or
 *   - raw cqlsh output containing `{...}` lines mixed with header/footer
 *     noise (non-JSON lines are skipped).
 */
/**
 * Parse one prepared-statements NDJSON line. cqlsh doubles every backslash when it prints `SELECT JSON` text,
 * so a quoted identifier ("C0") arrives as \\"C0\\" and the line isn't valid JSON. Retry with the doubling undone
 * (an exact inverse), so captures made with older samplers still parse.
 */
export const parsePreparedStatementLine = (line: string): Record<string, unknown> | null => {
  for (const candidate of [line, line.replace(/\\\\/g, '\\')]) {
    try {
      const obj = JSON.parse(candidate);
      if (obj && typeof obj === 'object') return obj as Record<string, unknown>;
    } catch {
      /* try the next form */
    }
  }
  return null;
};

export const parse_prepared_statements = (content: string): QueryPatternsInfo => {
  const result: QueryPatternsInfo = {
    lwt_in_unlogged_batch: [],
    aggregations: [],
    ttl_tables: {},
  };

  const aggNames = ['COUNT', 'MIN', 'MAX', 'SUM', 'AVG'];

  for (const rawLine of content.split('\n')) {
    const line = rawLine.trim();
    if (!line.startsWith('{') || !line.endsWith('}')) continue;

    const stmt = parsePreparedStatementLine(line) as
      | { prepared_id?: string; query_string?: string; logged_keyspace?: string | null }
      | null;
    if (!stmt) continue;

    const query = stmt.query_string ?? '';
    const preparedId = stmt.prepared_id ?? '';
    if (!query) continue;
    const issueRef: QueryPatternIssue = { prepared_id: preparedId, query_string: query };

    // 1. LWT inside UNLOGGED BATCH
    //    BEGIN UNLOGGED BATCH ... (IF NOT EXISTS | IF EXISTS | IF <col>=) ... APPLY BATCH
    const unloggedBatch = /\bBEGIN\s+UNLOGGED\s+BATCH\b([\s\S]*?)\bAPPLY\s+BATCH\b/i.exec(query);
    if (unloggedBatch && /\bIF\s+(NOT\s+EXISTS\b|EXISTS\b|\w+\s*[=<>!])/i.test(unloggedBatch[1])) {
      result.lwt_in_unlogged_batch.push(issueRef);
    }

    // 2. Aggregations — look in SELECT projection. Simplest: any occurrence
    //    of a supported aggregate name followed by '(' inside a SELECT query.
    //    Guarded with a \b boundary + whitespace-tolerant '(' to avoid
    //    matching column names that happen to contain these words.
    if (/\bSELECT\b/i.test(query)) {
      for (const fn of aggNames) {
        const re = new RegExp(`\\b${fn}\\s*\\(`, 'i');
        if (re.test(query)) {
          result.aggregations.push({ ...issueRef, function: fn });
          break; // one finding per statement is enough
        }
      }
    }

    // 3. USING TTL per target table
    //    Match each USING ... TTL <n> occurrence, then associate it with the
    //    nearest preceding INSERT INTO / UPDATE target table.
    const ttlPattern = /\bUSING\b[\s\S]*?\bTTL\s+(\d+)/gi;
    let tm: RegExpExecArray | null;
    while ((tm = ttlPattern.exec(query)) !== null) {
      const ttlValue = parseInt(tm[1], 10);
      const before = query.substring(0, tm.index);
      // Find the last INSERT INTO / UPDATE before this USING TTL clause.
      const targetPattern = /\b(?:INSERT\s+INTO|UPDATE)\s+(?:"?(\w+)"?\.)?"?(\w+)"?/gi;
      let tgt: RegExpExecArray | null;
      let lastMatch: RegExpExecArray | null = null;
      while ((tgt = targetPattern.exec(before)) !== null) lastMatch = tgt;
      if (!lastMatch) continue;
      const ks = (lastMatch[1] ?? stmt.logged_keyspace ?? '').toLowerCase();
      const tbl = lastMatch[2].toLowerCase();
      if (!ks || !tbl) continue;
      const key = `${ks}.${tbl}`;
      if (!result.ttl_tables[key]) {
        result.ttl_tables[key] = { uses_ttl: true, ttl_values: [] };
      }
      if (!result.ttl_tables[key].ttl_values.includes(ttlValue)) {
        result.ttl_tables[key].ttl_values.push(ttlValue);
      }
    }
  }

  return result;
};

export const parseRowSizeInfo = (content: string): RowSizeInfo => {
  const lines = content.split('\n');
  const result: RowSizeInfo = {};

  for (const line of lines) {
    const trimmedLine = line.trim();

    if (!/=/.test(trimmedLine) || /NoHostAvailable/i.test(trimmedLine)) {
      continue;
    }

    const match = trimmedLine.match(/^(.+?)\s*=\s*(.+)$/);
    if (!match) continue;

    const [, keyName, right] = match;
    const trimmedKeyName = keyName.trim();
    const trimmedRight = right.trim();
    if (!trimmedRight.startsWith('{') || !trimmedRight.endsWith('}')) {
      continue;
    }

    const inner = trimmedRight.slice(1, -1).trim();
    const fields = inner.split(',');
    const valueDict: Record<string, string> = {};
    for (const field of fields) {
      const trimmedField = field.trim();
      if (!/:\s*/.test(trimmedField)) continue;
      const [k, v] = trimmedField.split(':');
      if (!k || !v) continue;
      valueDict[k.trim()] = v.replace('bytes', '').trim();
    }
    result[trimmedKeyName] = valueDict;
  }
  return result;
};

// --- File type detection ---

export type CassandraFileType = 'tablestats' | 'status' | 'info' | 'rowsize' | 'schema' | 'tco' | 'prepared' | 'unknown';

export interface CassandraFileScan {
  tablestats: string[];   // filenames (multiple nodes allowed)
  status: string | null;
  info: string[];         // filenames (one per node)
  rowsize: string | null;
  schema: string | null;
  tco: string | null;
  prepared: string | null;
  unknown: string[];
}

export const isTablestatsFile = (content: string): boolean =>
  /Keyspace\s*:/i.test(content) && /Space used \(live\)/i.test(content);

export const isStatusFile = (content: string): boolean =>
  /Datacenter\s*:/i.test(content) && /^(U|D)(N|L|J|M)\s+/m.test(content);

export const isInfoFile = (content: string): boolean =>
  /^ID\s*:/im.test(content) && /Uptime\s*\(seconds\)/i.test(content);

export const isRowSizeFile = (content: string): boolean =>
  /\w+\.\w+\s*=\s*\{/.test(content) && /average\s*:\s*\d+\s*bytes/i.test(content);

export const isSchemaFile = (content: string): boolean =>
  /CREATE\s+(KEYSPACE|TABLE)/i.test(content);

export const isTcoFile = (content: string): boolean => {
  try {
    const obj = JSON.parse(content);
    return !!(obj?.single_node && obj?.operations);
  } catch {
    return false;
  }
};

// NDJSON (or cqlsh SELECT JSON) output of system.prepared_statements — any
// line containing a JSON object with both `prepared_id` and `query_string`
// keys counts as a match.
export const isPreparedStatementsFile = (content: string): boolean => {
  for (const rawLine of content.split('\n')) {
    const line = rawLine.trim();
    if (!line.startsWith('{') || !line.endsWith('}')) continue;
    const obj = parsePreparedStatementLine(line);
    if (obj && 'prepared_id' in obj && 'query_string' in obj) return true;
  }
  return false;
};

export const detectFileType = (content: string): CassandraFileType => {
  if (isPreparedStatementsFile(content)) return 'prepared';
  if (isTablestatsFile(content)) return 'tablestats';
  if (isStatusFile(content)) return 'status';
  if (isInfoFile(content)) return 'info';
  if (isRowSizeFile(content)) return 'rowsize';
  if (isSchemaFile(content)) return 'schema';
  if (isTcoFile(content)) return 'tco';
  return 'unknown';
};

// Classify a set of files (filename → content) into their respective roles.
export const scanCassandraFiles = (files: Record<string, string>): CassandraFileScan => {
  const result: CassandraFileScan = {
    tablestats: [],
    status: null,
    info: [],
    rowsize: null,
    schema: null,
    tco: null,
    prepared: null,
    unknown: [],
  };

  for (const [name, content] of Object.entries(files)) {
    switch (detectFileType(content)) {
      case 'tablestats': result.tablestats.push(name); break;
      case 'status':     result.status  ??= name; break;
      case 'info':       result.info.push(name); break;
      case 'rowsize':    result.rowsize ??= name; break;
      case 'schema':     result.schema  ??= name; break;
      case 'tco':        result.tco     ??= name; break;
      case 'prepared':   result.prepared ??= name; break;
      default:           result.unknown.push(name); break;
    }
  }

  // e.g. nodetool.txt with pasted `nodetool status` — content is status but filename not matched above
  if (!result.status) {
    for (const name of result.unknown) {
      const c = files[name];
      if (c && isStatusFile(c)) {
        result.status = name;
        break;
      }
    }
  }

  return result;
};

export const parseTCOInfo = (data: string): TcoData => {
  let obj: TcoData;
  try {
    obj = JSON.parse(data) as TcoData;
  } catch (err: unknown) {
    throw new Error(`Invalid JSON: ${(err as Error).message}`);
  }

  if (!obj.single_node || !obj.operations) {
    throw new Error("Invalid structure: expected 'single_node' and 'operations' fields");
  }
  if (!obj.single_node.instance || typeof obj.single_node.instance.monthly_cost !== 'number') {
    throw new Error("Invalid or missing 'instance.monthly_cost'");
  }
  if (!obj.operations.operator_hours || typeof obj.operations.operator_hours.monthly_cost !== 'number') {
    throw new Error("Invalid or missing 'operations.operator_hours.monthly_cost'");
  }

  return obj;
};
