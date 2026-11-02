/**
 * CloudFormationGenerator.ts
 *
 * Parses CQL schema (CREATE KEYSPACE / CREATE TYPE / CREATE TABLE) and
 * generates an AWS CloudFormation template containing:
 *   - AWS::Cassandra::Keyspace
 *   - AWS::Cassandra::Type
 *   - AWS::Cassandra::Table
 *
 * Handles: composite partition keys, clustering columns with order,
 * static columns, frozen collections, UDTs, default_time_to_live,
 * and proper DependsOn ordering.
 */

import { parse_cassandra_schema_compatibility } from './ParsingHelpers';
import { calculateReadUnitsPerOperation, calculateWriteUnitsPerOperation } from './PricingFormulas';
import { DEFAULT_TARGET_UTILIZATION } from './Constants';
import regionsJson from './data/regions.json';

/** AWS Region code → display name, e.g. us-east-1 → US East (N. Virginia). */
const REGION_NAMES = regionsJson as Record<string, string>;
const isRegionCode = (name: string): boolean => Object.prototype.hasOwnProperty.call(REGION_NAMES, name);

// ─── Parsed CQL types ─────────────────────────────────────────────────────────

export interface ParsedKeyspace {
  name: string;
  replicationClass: 'SimpleStrategy' | 'NetworkTopologyStrategy';
  replicationFactor?: number;
  datacenters?: Record<string, number>;
}

export interface ParsedUdtField {
  name: string;
  type: string;
}

export interface ParsedUdt {
  keyspace: string;
  name: string;
  fields: ParsedUdtField[];
}

export interface ParsedColumn {
  name: string;
  type: string;
  isStatic: boolean;
}

export interface ParsedTable {
  keyspace: string;
  name: string;
  columns: ParsedColumn[];
  partitionKeys: string[];
  clusteringKeys: string[];
  clusteringOrder: Record<string, 'ASC' | 'DESC'>;
  defaultTtl: number | null;
}

// ─── CloudFormation output types ──────────────────────────────────────────────

interface CfnColumn {
  ColumnName: string;
  ColumnType: string;
}

interface CfnClusteringKeyColumn {
  Column: CfnColumn;
  OrderBy: 'ASC' | 'DESC';
}

interface CfnField {
  FieldName: string;
  FieldType: string;
}

interface CfnReplicationSpecification {
  ReplicationStrategy: 'SINGLE_REGION' | 'MULTI_REGION';
  RegionList?: string[];
}

interface CfnKeyspaceProperties {
  KeyspaceName: string;
  ReplicationSpecification?: CfnReplicationSpecification;
  Tags?: Array<{ Key: string; Value: string }>;
}

interface CfnTypeProperties {
  KeyspaceName: string;
  TypeName: string;
  Fields: CfnField[];
}

interface CfnTableProperties {
  KeyspaceName: string;
  TableName: string;
  PartitionKeyColumns: CfnColumn[];
  ClusteringKeyColumns?: CfnClusteringKeyColumn[];
  RegularColumns?: CfnColumn[];
  DefaultTimeToLive?: number;
  BillingMode?: {
    Mode: string;
    ProvisionedThroughput?: { ReadCapacityUnits: number; WriteCapacityUnits: number };
  };
  AutoScalingSpecifications?: {
    ReadCapacityAutoScaling: CfnAutoScalingSetting;
    WriteCapacityAutoScaling: CfnAutoScalingSetting;
  };
  PointInTimeRecoveryEnabled?: boolean;
}

interface CfnAutoScalingSetting {
  AutoScalingDisabled: boolean;
  MinimumUnits: number;
  MaximumUnits: number;
  ScalingPolicy: { TargetTrackingScalingPolicyConfiguration: { TargetValue: number } };
}

interface CfnResource {
  Type: string;
  Properties: CfnKeyspaceProperties | CfnTypeProperties | CfnTableProperties;
  DependsOn?: string | string[];
}

export interface CfnTemplate {
  AWSTemplateFormatVersion: string;
  Description: string;
  Resources: Record<string, CfnResource>;
  Metadata?: { Warnings?: string[] };
}

export interface GenerateOptions {
  billingMode?: 'ON_DEMAND' | 'PROVISIONED';
  pointInTimeRecovery?: boolean;
  includeSystemKeyspaces?: boolean;
  /** Skip resources with known compatibility gaps (default: true). */
  skipIncompatible?: boolean;
  /** Force single-region even if schema defines NetworkTopologyStrategy (default: false). */
  forceSingleRegion?: boolean;
  /** Include materialized views as base tables (default: true). */
  includeMaterializedViews?: boolean;
  /** When set, only these views ("ks.view") become tables: the views an estimate priced (--views). */
  materializedViews?: string[];
  /**
   * Cassandra datacenter name → AWS Region code, used to build MULTI_REGION RegionLists.
   * Datacenters already named after a Region code (e.g. us-east-1) map to themselves.
   */
  datacenterRegions?: Record<string, string>;
  /**
   * Keyspace-level provisioned capacity (PROVISIONED billing only), split evenly across the keyspace's
   * tables. Key '*' applies to keyspaces without their own entry.
   */
  keyspaceCapacity?: Record<string, KeyspaceCapacity>;
  /** Keyspaces whose tables get PointInTimeRecovery (in addition to pointInTimeRecovery = all). */
  pitrKeyspaces?: string[];
  /** Target utilization % (20-90) for provisioned auto scaling; 0 disables auto scaling (default: 70). */
  autoScalingTargetPercent?: number;
}

export interface KeyspaceCapacity {
  readCapacityUnits: number;
  writeCapacityUnits: number;
}

/** Keyspaces auto scaling bounds: 1..40,000 units per table (default account quota). */
const MAX_CAPACITY_UNITS = 40000;
/** Auto scaling target (%) = the default target utilization used for pricing. */
const DEFAULT_AUTO_SCALING_TARGET = Math.round(DEFAULT_TARGET_UTILIZATION * 100);

interface EstimateKeyspace {
  reads_per_second?: number;
  writes_per_second?: number;
  avg_read_row_size_bytes?: number;
  avg_write_row_size_bytes?: number;
  avg_rows_per_read?: number;
  use_backup?: boolean;
}

/** Subset of calculate.ts / parse-cassandra.ts estimate JSON used to shape the template. */
export interface EstimateForTemplate {
  on_demand?: { total?: number };
  provisioned?: { total?: number };
  regions_by_datacenter?: Record<string, { short: string; long: string }>;
  materialized_views?: Record<string, { priced: boolean }>;
  report_data?: {
    regions?: Record<string, string>;
    estimateResults?: Record<string, Record<string, EstimateKeyspace>>;
  };
}

/**
 * Derive template options from a pricing estimate so the template matches the priced model:
 * provisioned RCU/WCU per keyspace (largest datacenter, same unit rounding and default target utilization
 * as the pricing formulas), PITR where the estimate priced backups, and datacenter → Region from the
 * estimate. Billing mode is NOT derived: templates default to ON_DEMAND, and the capacity is used only
 * when the caller asks for PROVISIONED. Mode 1 / SQL-model estimates use keyspace "default", which is
 * mapped to '*' (all keyspaces in the schema).
 */
export function deriveTemplateOptionsFromEstimate(estimate: EstimateForTemplate): GenerateOptions {

  const keyspaceCapacity: Record<string, KeyspaceCapacity> = {};
  const pitr = new Set<string>();
  for (const keyspaces of Object.values(estimate.report_data?.estimateResults ?? {})) {
    for (const [name, ks] of Object.entries(keyspaces)) {
      const key = name === 'default' ? '*' : name;
      const readUnits = calculateReadUnitsPerOperation(ks.avg_read_row_size_bytes ?? 1024, ks.avg_rows_per_read ?? 1);
      const writeUnits = calculateWriteUnitsPerOperation(ks.avg_write_row_size_bytes ?? 1024);
      const rcu = Math.ceil(((ks.reads_per_second ?? 0) * readUnits) / DEFAULT_TARGET_UTILIZATION);
      const wcu = Math.ceil(((ks.writes_per_second ?? 0) * writeUnits) / DEFAULT_TARGET_UTILIZATION);
      const prev = keyspaceCapacity[key];
      keyspaceCapacity[key] = {
        readCapacityUnits: Math.max(prev?.readCapacityUnits ?? 0, rcu),
        writeCapacityUnits: Math.max(prev?.writeCapacityUnits ?? 0, wcu),
      };
      if (ks.use_backup) pitr.add(key);
    }
  }

  const datacenterRegions: Record<string, string> = {};
  const nameToCode = new Map(Object.entries(REGION_NAMES).map(([code, long]) => [long, code]));
  for (const [dc, long] of Object.entries(estimate.report_data?.regions ?? {})) {
    const code = nameToCode.get(long);
    if (code) datacenterRegions[dc] = code;
  }
  for (const [dc, r] of Object.entries(estimate.regions_by_datacenter ?? {})) {
    if (r?.short) datacenterRegions[dc] = r.short;
  }

  const views = estimate.materialized_views;
  return {
    ...(views ? { materializedViews: Object.keys(views).filter((v) => views[v].priced) } : {}),
    keyspaceCapacity,
    pointInTimeRecovery: pitr.has('*'),
    pitrKeyspaces: [...pitr].filter((k) => k !== '*'),
    datacenterRegions,
  };
}

// ─── CQL Parsing ──────────────────────────────────────────────────────────────

const SYSTEM_KEYSPACES = new Set([
  'OpsCenter', 'dse_insights_local', 'solr_admin',
  'dse_system', 'HiveMetaStore', 'system_auth',
  'dse_analytics', 'system_traces', 'dse_audit', 'system',
  'dse_system_local', 'dsefs', 'system_distributed', 'system_schema',
  'dse_perf', 'dse_insights', 'system_backups', 'dse_security',
  'dse_leases', 'system_distributed_everywhere', 'reaper_db',
  'system_virtual_schema', 'system_views',
]);

function sanitizeLogicalId(name: string): string {
  return name.replace(/[^a-zA-Z0-9]/g, '');
}

function splitTopLevelCommas(s: string): string[] {
  const parts: string[] = [];
  let depth = 0;
  let current = '';
  for (const ch of s) {
    if (ch === '(' || ch === '<') depth++;
    else if (ch === ')' || ch === '>') depth--;
    if (ch === ',' && depth === 0) {
      parts.push(current.trim());
      current = '';
    } else {
      current += ch;
    }
  }
  if (current.trim()) parts.push(current.trim());
  return parts;
}

function extractParenContent(s: string): string {
  const start = s.indexOf('(');
  if (start === -1) return s;
  let depth = 0;
  let end = start;
  for (let i = start; i < s.length; i++) {
    if (s[i] === '(') depth++;
    else if (s[i] === ')') {
      depth--;
      if (depth === 0) { end = i; break; }
    }
  }
  return s.slice(start + 1, end);
}

function extractBracedContent(s: string): string {
  const start = s.indexOf('{');
  if (start === -1) return '';
  const end = s.lastIndexOf('}');
  if (end === -1) return '';
  return s.slice(start + 1, end);
}

export function parseKeyspaces(cql: string): ParsedKeyspace[] {
  const results: ParsedKeyspace[] = [];
  const pattern = /CREATE\s+KEYSPACE\s+(?:IF\s+NOT\s+EXISTS\s+)?["']?(\w+)["']?\s+WITH\s+replication\s*=\s*\{([^}]*)\}/gi;
  let m: RegExpExecArray | null;
  while ((m = pattern.exec(cql)) !== null) {
    const name = m[1];
    const replBody = m[2];
    const classMatch = replBody.match(/'class'\s*:\s*'([^']+)'/i);
    const clazz = classMatch?.[1] ?? 'SimpleStrategy';

    const ks: ParsedKeyspace = {
      name,
      replicationClass: clazz.includes('NetworkTopology') ? 'NetworkTopologyStrategy' : 'SimpleStrategy',
    };

    if (ks.replicationClass === 'SimpleStrategy') {
      const rfMatch = replBody.match(/'replication_factor'\s*:\s*'?(\d+)'?/i);
      ks.replicationFactor = rfMatch ? parseInt(rfMatch[1], 10) : 1;
    } else {
      ks.datacenters = {};
      const dcPattern = /'([^']+)'\s*:\s*'?(\d+)'?/g;
      let dcm: RegExpExecArray | null;
      while ((dcm = dcPattern.exec(replBody)) !== null) {
        if (dcm[1].toLowerCase() !== 'class') {
          ks.datacenters[dcm[1]] = parseInt(dcm[2], 10);
        }
      }
    }
    results.push(ks);
  }
  return results;
}

export function parseUdts(cql: string): ParsedUdt[] {
  const results: ParsedUdt[] = [];
  const pattern = /CREATE\s+TYPE\s+(?:IF\s+NOT\s+EXISTS\s+)?["']?(\w+)["']?\.["']?(\w+)["']?\s*\(([\s\S]*?)\);/gi;
  let m: RegExpExecArray | null;
  while ((m = pattern.exec(cql)) !== null) {
    const keyspace = m[1];
    const name = m[2];
    const body = m[3];
    const fields: ParsedUdtField[] = [];
    for (const line of splitTopLevelCommas(body)) {
      const trimmed = line.trim();
      if (!trimmed) continue;
      const spaceIdx = trimmed.indexOf(' ');
      if (spaceIdx === -1) continue;
      const fieldName = trimmed.slice(0, spaceIdx).replace(/"/g, '');
      const fieldType = trimmed.slice(spaceIdx + 1).trim();
      fields.push({ name: fieldName, type: fieldType });
    }
    results.push({ keyspace, name, fields });
  }
  return results;
}

export function parseTables(cql: string): ParsedTable[] {
  const results: ParsedTable[] = [];
  const pattern = /CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?["']?(\w+)["']?\.["']?(\w+)["']?\s*\(([\s\S]*?)\)(?:\s+WITH\s+([\s\S]*?))?;/gi;
  let m: RegExpExecArray | null;
  while ((m = pattern.exec(cql)) !== null) {
    const keyspace = m[1];
    const tableName = m[2];
    const columnsBody = m[3];
    const withClause = m[4] ?? '';

    const columns: ParsedColumn[] = [];
    const partitionKeys: string[] = [];
    const clusteringKeys: string[] = [];
    const clusteringOrder: Record<string, 'ASC' | 'DESC'> = {};

    const columnDefs = splitTopLevelCommas(columnsBody);
    for (const def of columnDefs) {
      const trimmed = def.trim();
      if (!trimmed) continue;

      if (/^\s*PRIMARY\s+KEY\s*/i.test(trimmed)) {
        const pkContent = extractParenContent(trimmed);
        parsePrimaryKeyClause(pkContent, partitionKeys, clusteringKeys);
        continue;
      }

      const parts = trimmed.split(/\s+/);
      const colName = parts[0].replace(/"/g, '');

      const staticIdx = parts.findIndex((p) => /^STATIC$/i.test(p));
      const isPrimaryKey = parts.some((p) => /^PRIMARY$/i.test(p));
      const isStatic = staticIdx !== -1;

      let typeEnd = staticIdx !== -1 ? staticIdx : parts.length;
      if (isPrimaryKey) {
        const pkIdx = parts.findIndex((p) => /^PRIMARY$/i.test(p));
        typeEnd = pkIdx;
      }
      const colType = parts.slice(1, typeEnd).join(' ');

      columns.push({ name: colName, type: colType, isStatic });

      if (isPrimaryKey) {
        partitionKeys.push(colName);
      }
    }

    // Parse CLUSTERING ORDER BY from WITH clause
    const clusteringOrderMatch = withClause.match(
      /CLUSTERING\s+ORDER\s+BY\s*\(([^)]+)\)/i
    );
    if (clusteringOrderMatch) {
      const orderParts = clusteringOrderMatch[1].split(',');
      for (const part of orderParts) {
        const tokens = part.trim().split(/\s+/);
        const col = tokens[0]?.replace(/"/g, '');
        const order = (tokens[1]?.toUpperCase() === 'DESC' ? 'DESC' : 'ASC') as 'ASC' | 'DESC';
        if (col) clusteringOrder[col] = order;
      }
    }

    // Parse default_time_to_live from WITH clause
    let defaultTtl: number | null = null;
    const ttlMatch = withClause.match(/default_time_to_live\s*=\s*(\d+)/i);
    if (ttlMatch) {
      defaultTtl = parseInt(ttlMatch[1], 10);
    }

    // For clustering keys without explicit order, default to ASC
    for (const ck of clusteringKeys) {
      if (!clusteringOrder[ck]) clusteringOrder[ck] = 'ASC';
    }

    results.push({
      keyspace,
      name: tableName,
      columns,
      partitionKeys,
      clusteringKeys,
      clusteringOrder,
      defaultTtl,
    });
  }
  return results;
}

export function parseMaterializedViews(cql: string, baseTables: ParsedTable[]): ParsedTable[] {
  const results: ParsedTable[] = [];

  // Build lookup: "keyspace.table" → ParsedTable
  const tableMap = new Map<string, ParsedTable>();
  for (const t of baseTables) {
    tableMap.set(`${t.keyspace}.${t.name}`, t);
  }

  const mvPattern = /CREATE\s+MATERIALIZED\s+VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?["']?(\w+)["']?\.["']?(\w+)["']?\s+AS\s+SELECT\s+([\s\S]*?)\s+FROM\s+["']?(\w+)["']?\.["']?(\w+)["']?\s+WHERE\s+[\s\S]*?PRIMARY\s+KEY\s*\(([\s\S]*?)\)(?:\s+WITH\s+([\s\S]*?))?;/gi;

  let m: RegExpExecArray | null;
  while ((m = mvPattern.exec(cql)) !== null) {
    const keyspace = m[1];
    const viewName = m[2];
    const selectCols = m[3];
    const baseKs = m[4];
    const baseTable = m[5];
    const pkClause = m[6];
    const withClause = m[7] ?? '';

    const base = tableMap.get(`${baseKs}.${baseTable}`);
    if (!base) continue;

    // Build column type lookup from base table
    const baseColTypes = new Map<string, { type: string; isStatic: boolean }>();
    for (const col of base.columns) {
      baseColTypes.set(col.name, { type: col.type, isStatic: col.isStatic });
    }

    // Parse selected columns
    const selectedNames: string[] = [];
    for (const part of selectCols.split(',')) {
      const name = part.trim().replace(/"/g, '');
      if (name && name !== '*') selectedNames.push(name);
    }

    // If SELECT *, use all columns from base table
    const colNames = selectedNames.length > 0 ? selectedNames : base.columns.map((c) => c.name);

    // Parse PRIMARY KEY
    const partitionKeys: string[] = [];
    const clusteringKeys: string[] = [];
    parsePrimaryKeyClause(pkClause.trim(), partitionKeys, clusteringKeys);

    // Parse CLUSTERING ORDER BY
    const clusteringOrder: Record<string, 'ASC' | 'DESC'> = {};
    const clusteringOrderMatch = withClause.match(
      /CLUSTERING\s+ORDER\s+BY\s*\(([^)]+)\)/i
    );
    if (clusteringOrderMatch) {
      for (const part of clusteringOrderMatch[1].split(',')) {
        const tokens = part.trim().split(/\s+/);
        const col = tokens[0]?.replace(/"/g, '');
        const order = (tokens[1]?.toUpperCase() === 'DESC' ? 'DESC' : 'ASC') as 'ASC' | 'DESC';
        if (col) clusteringOrder[col] = order;
      }
    }
    for (const ck of clusteringKeys) {
      if (!clusteringOrder[ck]) clusteringOrder[ck] = 'ASC';
    }

    // Build columns with types from base table (MVs have no static columns)
    const columns: ParsedColumn[] = [];
    for (const name of colNames) {
      const baseCol = baseColTypes.get(name);
      columns.push({
        name,
        type: baseCol?.type ?? 'text',
        isStatic: false,
      });
    }

    // Parse default_time_to_live
    let defaultTtl: number | null = null;
    const ttlMatch = withClause.match(/default_time_to_live\s*=\s*(\d+)/i);
    if (ttlMatch) {
      const val = parseInt(ttlMatch[1], 10);
      if (val > 0) defaultTtl = val;
    }

    results.push({
      keyspace,
      name: viewName,
      columns,
      partitionKeys,
      clusteringKeys,
      clusteringOrder,
      defaultTtl,
    });
  }

  return results;
}

function parsePrimaryKeyClause(
  content: string,
  partitionKeys: string[],
  clusteringKeys: string[]
): void {
  const trimmed = content.trim();

  if (trimmed.startsWith('(')) {
    // Composite partition key: ((pk1, pk2), ck1, ck2)
    const innerEnd = trimmed.indexOf(')', 1);
    const pkPart = trimmed.slice(1, innerEnd);
    const rest = trimmed.slice(innerEnd + 1);

    for (const pk of pkPart.split(',')) {
      const name = pk.trim().replace(/"/g, '');
      if (name) partitionKeys.push(name);
    }

    if (rest.includes(',')) {
      const ckPart = rest.slice(rest.indexOf(',') + 1);
      for (const ck of ckPart.split(',')) {
        const name = ck.trim().replace(/"/g, '');
        if (name) clusteringKeys.push(name);
      }
    }
  } else {
    // Simple: (pk, ck1, ck2)
    const parts = trimmed.split(',').map((p) => p.trim().replace(/"/g, ''));
    if (parts.length > 0) partitionKeys.push(parts[0]);
    for (let i = 1; i < parts.length; i++) {
      if (parts[i]) clusteringKeys.push(parts[i]);
    }
  }
}

// ─── CloudFormation Generation ────────────────────────────────────────────────

function mapCqlTypeToCfn(cqlType: string): string {
  return cqlType.toUpperCase()
    .replace(/\bFROZEN\b/gi, 'FROZEN')
    .replace(/\bTEXT\b/gi, 'TEXT')
    .replace(/\bVARCHAR\b/gi, 'TEXT')
    .replace(/\bINT\b/gi, 'INT')
    .replace(/\bBIGINT\b/gi, 'BIGINT')
    .replace(/\bBLOB\b/gi, 'BLOB')
    .replace(/\bBOOLEAN\b/gi, 'BOOLEAN')
    .replace(/\bDOUBLE\b/gi, 'DOUBLE')
    .replace(/\bFLOAT\b/gi, 'FLOAT')
    .replace(/\bTIMESTAMP\b/gi, 'TIMESTAMP')
    .replace(/\bUUID\b/gi, 'UUID')
    .replace(/\bTIMEUUID\b/gi, 'TIMEUUID')
    .replace(/\bASCII\b/gi, 'ASCII')
    .replace(/\bDECIMAL\b/gi, 'DECIMAL')
    .replace(/\bDATE\b/gi, 'DATE')
    .replace(/\bTIME\b/gi, 'TIME')
    .replace(/\bSMALLINT\b/gi, 'SMALLINT')
    .replace(/\bTINYINT\b/gi, 'TINYINT')
    .replace(/\bVARINT\b/gi, 'VARINT')
    .replace(/\bCOUNTER\b/gi, 'COUNTER');
}

function normalizeColumnType(col: ParsedColumn): string {
  let cfnType = mapCqlTypeToCfn(col.type);
  if (col.isStatic) {
    cfnType += ' STATIC';
  }
  return cfnType;
}

function findUdtDependencies(type: string, keyspace: string, availableUdts: Set<string>): string[] {
  const deps: string[] = [];
  for (const udtName of availableUdts) {
    const re = new RegExp(`\\b${udtName}\\b`, 'i');
    if (re.test(type)) {
      deps.push(`${keyspace}${sanitizeLogicalId(udtName)}`);
    }
  }
  return deps;
}

const CFN_COLUMN_NAME_PATTERN = /^[a-zA-Z0-9][a-zA-Z0-9_]{1,47}$/;

export function generateCloudFormationTemplate(
  cql: string,
  options: GenerateOptions = {}
): CfnTemplate {
  const {
    billingMode = 'ON_DEMAND',
    pointInTimeRecovery = false,
    includeSystemKeyspaces = false,
    skipIncompatible = true,
    forceSingleRegion = false,
    includeMaterializedViews = true,
    materializedViews,
    datacenterRegions = {},
    keyspaceCapacity,
    pitrKeyspaces = [],
    autoScalingTargetPercent = DEFAULT_AUTO_SCALING_TARGET,
  } = options;
  const pitrKeyspaceSet = new Set(pitrKeyspaces);

  const keyspaces = parseKeyspaces(cql);
  const udts = parseUdts(cql);
  const baseTables = parseTables(cql);
  const mvTables = includeMaterializedViews
    ? parseMaterializedViews(cql, baseTables).filter(
        (v) => !materializedViews || materializedViews.includes(`${v.keyspace}.${v.name}`)
      )
    : [];
  const tables = [...baseTables, ...mvTables];
  const warnings: string[] = [];

  // Override replication to single-region if requested
  if (forceSingleRegion) {
    for (const ks of keyspaces) {
      ks.replicationClass = 'SimpleStrategy';
      ks.datacenters = undefined;
      ks.replicationFactor = 3;
    }
  }

  // Run compatibility check to identify resources that should be modified
  const skippedUdts = new Set<string>(); // "keyspace.udt_name" — only truly undeployable UDTs
  const skippedTables = new Set<string>(); // "keyspace.table_name"
  if (skipIncompatible) {
    const compat = parse_cassandra_schema_compatibility(cql);

    // Build set of all known UDT names per keyspace for field-type detection
    const allUdtNames = new Map<string, Set<string>>();
    for (const udt of udts) {
      if (!allUdtNames.has(udt.keyspace)) allUdtNames.set(udt.keyspace, new Set());
      allUdtNames.get(udt.keyspace)!.add(udt.name.toLowerCase());
    }

    // Helper: does a field type reference a UDT?
    function fieldReferencesUdt(fieldType: string, keyspace: string): boolean {
      const ksUdts = allUdtNames.get(keyspace);
      if (!ksUdts) return false;
      for (const udtName of ksUdts) {
        const re = new RegExp(`\\b${udtName}\\b`, 'i');
        if (re.test(fieldType)) return true;
      }
      return false;
    }

    // 1. For oversized UDTs: replace all non-UDT fields with a single "payload blob"
    //    but preserve fields that are UDTs or contain UDTs (maintains type hierarchy)
    const modifiedUdts = new Set<string>(); // UDTs that were transformed (payload blob)
    if (compat.udt_quotas) {
      for (const issue of compat.udt_quotas.issues) {
        const key = `${issue.keyspace}.${issue.name}`;
        const udt = udts.find((u) => u.keyspace === issue.keyspace && u.name === issue.name);
        if (!udt) {
          skippedUdts.add(key);
          warnings.push(`UDT "${key}" skipped: ${issue.issues.join('; ')}`);
          continue;
        }

        // Partition fields: keep UDT-referencing fields, replace the rest with "payload blob"
        const udtFields = udt.fields.filter((f) => fieldReferencesUdt(f.type, udt.keyspace));
        const nonUdtFields = udt.fields.filter((f) => !fieldReferencesUdt(f.type, udt.keyspace));

        if (nonUdtFields.length > 0) {
          udt.fields = [...udtFields, { name: 'payload', type: 'blob' }];
          modifiedUdts.add(key);
          warnings.push(
            `UDT "${key}": ${nonUdtFields.length} non-UDT fields replaced with single "payload" blob (${issue.issues.join('; ')}). UDT fields preserved: ${udtFields.length > 0 ? udtFields.map((f) => f.name).join(', ') : 'none'}`
          );
        }
      }
    }

    // 2. For UDTs that reference a modified or skipped UDT, replace that field with blob.
    //    Keyspaces counts frozen/collection wrappers toward nesting depth, so
    //    frozen<list<frozen<UDT>>> adds multiple levels per hop. Referencing a
    //    modified UDT (which itself has nested UDTs) would exceed the 8-level limit.
    for (const udt of udts) {
      const key = `${udt.keyspace}.${udt.name}`;
      if (skippedUdts.has(key)) continue;
      if (modifiedUdts.has(key)) continue;
      for (const field of udt.fields) {
        const toCheck = [...skippedUdts, ...modifiedUdts];
        for (const target of toCheck) {
          const targetName = target.split('.')[1];
          const targetKs = target.split('.')[0];
          if (udt.keyspace !== targetKs) continue;
          const re = new RegExp(`\\b${targetName}\\b`, 'i');
          if (re.test(field.type)) {
            warnings.push(`UDT "${key}" field "${field.name}": reference to "${targetName}" replaced with blob (nesting depth limit)`);
            field.type = 'blob';
          }
        }
      }
    }

    // 3. Replace references to skipped UDTs with blob in table columns
    for (const table of tables) {
      for (const col of table.columns) {
        for (const skipped of skippedUdts) {
          const skippedName = skipped.split('.')[1];
          const skippedKs = skipped.split('.')[0];
          if (table.keyspace !== skippedKs) continue;
          const re = new RegExp(`\\b${skippedName}\\b`, 'i');
          if (re.test(col.type)) {
            warnings.push(`Column "${col.name}" in ${table.keyspace}.${table.name}: UDT "${skippedName}" replaced with blob (UDT exceeds schema size limit)`);
            col.type = 'blob';
            break;
          }
        }
      }
    }
  }

  const template: CfnTemplate = {
    AWSTemplateFormatVersion: '2010-09-09',
    Description: 'Amazon Keyspaces resources generated from Cassandra schema',
    Resources: {},
  };

  const includedKeyspaces = new Set<string>();

  // Multi-region keyspaces: map NetworkTopologyStrategy datacenter names to AWS Region codes.
  // RegionList needs >= 2 distinct Regions; unmapped datacenters are reported, not emitted.
  const keyspaceRegionLists = new Map<string, string[]>();
  const unmappedDatacenters = new Set<string>();
  for (const ks of keyspaces) {
    if (!includeSystemKeyspaces && SYSTEM_KEYSPACES.has(ks.name)) continue;
    if (ks.replicationClass !== 'NetworkTopologyStrategy' || !ks.datacenters) continue;
    const dcs = Object.keys(ks.datacenters);
    if (dcs.length < 2) continue;
    const regions: string[] = [];
    for (const dc of dcs) {
      const region = datacenterRegions[dc] ?? (isRegionCode(dc) ? dc : null);
      if (!region) unmappedDatacenters.add(dc);
      else if (!regions.includes(region)) regions.push(region);
    }
    if (dcs.some((dc) => !(datacenterRegions[dc] ?? (isRegionCode(dc) ? dc : null)))) continue;
    if (regions.length < 2) {
      warnings.push(
        `Keyspace ${ks.name}: datacenters ${dcs.join(', ')} map to a single Region (${regions[0]}); generated as single-Region.`
      );
      continue;
    }
    keyspaceRegionLists.set(ks.name, regions);
  }
  if (unmappedDatacenters.size > 0) {
    warnings.push(
      `Datacenters without an AWS Region: ${[...unmappedDatacenters].join(', ')}. ` +
        'Their keyspaces were generated as single-Region; supply a datacenter-to-Region mapping for multi-Region replication.'
    );
  }
  const deployRegions = new Set([...keyspaceRegionLists.values()].flat());
  if (deployRegions.size > 0) {
    warnings.push(
      `Multi-Region keyspaces: deploy this stack in one of their Regions (${[...deployRegions].join(', ')}).`
    );
  }

  // Generate Keyspace resources
  for (const ks of keyspaces) {
    if (!includeSystemKeyspaces && SYSTEM_KEYSPACES.has(ks.name)) continue;
    includedKeyspaces.add(ks.name);

    const logicalId = `${sanitizeLogicalId(ks.name)}Keyspace`;
    const props: CfnKeyspaceProperties = {
      KeyspaceName: ks.name,
    };

    const regionList = keyspaceRegionLists.get(ks.name);
    if (regionList) {
      props.ReplicationSpecification = {
        ReplicationStrategy: 'MULTI_REGION',
        RegionList: regionList,
      };
    }

    template.Resources[logicalId] = {
      Type: 'AWS::Cassandra::Keyspace',
      Properties: props,
    };
  }

  // Track UDTs per keyspace for dependency resolution (excluding skipped UDTs)
  const udtsByKeyspace = new Map<string, Set<string>>();
  for (const udt of udts) {
    if (!includedKeyspaces.has(udt.keyspace)) continue;
    if (skippedUdts.has(`${udt.keyspace}.${udt.name}`)) continue;
    if (!udtsByKeyspace.has(udt.keyspace)) udtsByKeyspace.set(udt.keyspace, new Set());
    udtsByKeyspace.get(udt.keyspace)!.add(udt.name);
  }

  // Generate Type resources — serialized per keyspace to avoid concurrent DDL
  // issues with multi-region UDT stabilization.
  const prevUdtByKeyspace = new Map<string, string>();

  for (const udt of udts) {
    if (!includedKeyspaces.has(udt.keyspace)) continue;
    if (skippedUdts.has(`${udt.keyspace}.${udt.name}`)) continue;

    const logicalId = `${sanitizeLogicalId(udt.keyspace)}${sanitizeLogicalId(udt.name)}Type`;
    const ksLogicalId = `${sanitizeLogicalId(udt.keyspace)}Keyspace`;
    const deps: string[] = [ksLogicalId];

    // Check if this UDT depends on other UDTs in the same keyspace (nested UDTs)
    const sameKsUdts = udtsByKeyspace.get(udt.keyspace) ?? new Set();
    for (const field of udt.fields) {
      for (const otherUdt of sameKsUdts) {
        if (otherUdt === udt.name) continue;
        const re = new RegExp(`\\b${otherUdt}\\b`, 'i');
        if (re.test(field.type)) {
          deps.push(`${sanitizeLogicalId(udt.keyspace)}${sanitizeLogicalId(otherUdt)}Type`);
        }
      }
    }

    // Chain: each UDT depends on the previous UDT in its keyspace (serial creation)
    const prevUdt = prevUdtByKeyspace.get(udt.keyspace);
    if (prevUdt && !deps.includes(prevUdt)) {
      deps.push(prevUdt);
    }
    prevUdtByKeyspace.set(udt.keyspace, logicalId);

    const fields: CfnField[] = udt.fields.map((f) => ({
      FieldName: f.name,
      FieldType: f.type.toLowerCase(),
    }));

    template.Resources[logicalId] = {
      Type: 'AWS::Cassandra::Type',
      Properties: {
        KeyspaceName: udt.keyspace,
        TypeName: udt.name,
        Fields: fields,
      } as CfnTypeProperties,
      DependsOn: deps.length === 1 ? deps[0] : deps,
    };
  }

  // Identify multi-region keyspaces (tables in these need serial creation
  // because the CloudFormation handler timeout is too short for parallel
  // multi-region table stabilization).
  const multiRegionKeyspaces = new Set<string>(keyspaceRegionLists.keys());

  // Provisioned capacity is set per keyspace by the estimate and split evenly across its tables.
  const tableCountByKeyspace = new Map<string, number>();
  for (const table of tables) {
    if (!includedKeyspaces.has(table.keyspace) || skippedTables.has(`${table.keyspace}.${table.name}`)) continue;
    tableCountByKeyspace.set(table.keyspace, (tableCountByKeyspace.get(table.keyspace) ?? 0) + 1);
  }
  const totalTableCount = [...tableCountByKeyspace.values()].reduce((a, b) => a + b, 0);
  const capacityForTable = (keyspace: string): KeyspaceCapacity => {
    const own = keyspaceCapacity?.[keyspace];
    const shared = keyspaceCapacity?.['*'];
    const ksCapacity = own ?? shared;
    const tableCount = own ? tableCountByKeyspace.get(keyspace) ?? 1 : shared ? totalTableCount || 1 : 1;
    const per = (units: number) => Math.min(MAX_CAPACITY_UNITS, Math.max(1, Math.ceil((units ?? 0) / tableCount)));
    return {
      readCapacityUnits: per(ksCapacity?.readCapacityUnits ?? 1),
      writeCapacityUnits: per(ksCapacity?.writeCapacityUnits ?? 1),
    };
  };
  if (billingMode === 'PROVISIONED') {
    if (!keyspaceCapacity) {
      warnings.push('PROVISIONED tables use 1 RCU / 1 WCU placeholders; pass an estimate to size capacity.');
    } else if ([...tableCountByKeyspace.values()].some((n) => n > 1)) {
      warnings.push(
        'Provisioned capacity is split evenly across each keyspace\'s tables; auto scaling adjusts per-table capacity after deployment.'
      );
    }
  }

  // Generate Table resources.
  // - Multi-region keyspace tables: fully serial (each depends on previous)
  // - Single-region keyspace tables: waves of BATCH_SIZE (under the 50 DDL limit,
  //   accounting for concurrent keyspace + UDT creation)
  const BATCH_SIZE = 10;
  const singleRegionTableIds: string[] = [];
  const prevMultiRegionTableByKs = new Map<string, string>();

  for (const table of tables) {
    if (!includedKeyspaces.has(table.keyspace)) continue;
    if (skippedTables.has(`${table.keyspace}.${table.name}`)) continue;

    const logicalId = `${sanitizeLogicalId(table.keyspace)}${sanitizeLogicalId(table.name)}Table`;
    const ksLogicalId = `${sanitizeLogicalId(table.keyspace)}Keyspace`;
    const deps: string[] = [ksLogicalId];

    // Determine if this table references any UDTs
    const ksUdts = udtsByKeyspace.get(table.keyspace) ?? new Set();
    let referencesUdt = false;
    for (const col of table.columns) {
      for (const udtName of ksUdts) {
        const re = new RegExp(`\\b${udtName}\\b`, 'i');
        if (re.test(col.type)) { referencesUdt = true; break; }
      }
      if (referencesUdt) break;
    }

    if (referencesUdt) {
      // Tables with UDT dependencies: depend on ALL UDTs
      for (const [ksName, ksUdtSet] of udtsByKeyspace.entries()) {
        for (const udtName of ksUdtSet) {
          const udtLogicalId = `${sanitizeLogicalId(ksName)}${sanitizeLogicalId(udtName)}Type`;
          if (!deps.includes(udtLogicalId)) deps.push(udtLogicalId);
        }
      }
    }

    if (multiRegionKeyspaces.has(table.keyspace)) {
      // Multi-region: serial chain — each table depends on the previous one
      const prev = prevMultiRegionTableByKs.get(table.keyspace);
      if (prev && !deps.includes(prev)) deps.push(prev);
      prevMultiRegionTableByKs.set(table.keyspace, logicalId);
    } else {
      // Single-region: tables are grouped into batches via ResourceGroups
      // Each table depends on the previous batch's ResourceGroup
      const batchIndex = Math.floor(singleRegionTableIds.length / BATCH_SIZE);
      if (batchIndex > 0) {
        const prevGroupId = `TableBatch${batchIndex}`;
        if (!deps.includes(prevGroupId)) deps.push(prevGroupId);
      }
      singleRegionTableIds.push(logicalId);
    }

    const partitionKeyColumns: CfnColumn[] = table.partitionKeys.map((pk) => {
      const col = table.columns.find((c) => c.name === pk);
      return {
        ColumnName: pk,
        ColumnType: col ? normalizeColumnType({ ...col, isStatic: false }) : 'TEXT',
      };
    });

    let clusteringKeyColumns: CfnClusteringKeyColumn[] | undefined;
    if (table.clusteringKeys.length > 0) {
      clusteringKeyColumns = table.clusteringKeys.map((ck) => {
        const col = table.columns.find((c) => c.name === ck);
        return {
          Column: {
            ColumnName: ck,
            ColumnType: col ? normalizeColumnType({ ...col, isStatic: false }) : 'TEXT',
          },
          OrderBy: table.clusteringOrder[ck] ?? 'ASC',
        };
      });
    }

    const pkAndCkNames = new Set([...table.partitionKeys, ...table.clusteringKeys]);
    const regularCols = table.columns.filter((c) => !pkAndCkNames.has(c.name));
    let regularColumns: CfnColumn[] | undefined;
    if (regularCols.length > 0) {
      const validCols: CfnColumn[] = [];
      for (const c of regularCols) {
        let colName = c.name;
        if (colName.length < 2) {
          colName = `${colName}_ks`;
          warnings.push(
            `Column "${c.name}" in ${table.keyspace}.${table.name} renamed to "${colName}" (CloudFormation requires min 2 chars)`
          );
        }
        if (!CFN_COLUMN_NAME_PATTERN.test(colName)) {
          warnings.push(
            `Column "${colName}" in ${table.keyspace}.${table.name} skipped: name does not match CloudFormation pattern.`
          );
          continue;
        }
        validCols.push({ ColumnName: colName, ColumnType: normalizeColumnType(c) });
      }
      if (validCols.length > 0) regularColumns = validCols;
    }

    const tableProps: CfnTableProperties = {
      KeyspaceName: table.keyspace,
      TableName: table.name,
      PartitionKeyColumns: partitionKeyColumns,
    };

    if (clusteringKeyColumns) tableProps.ClusteringKeyColumns = clusteringKeyColumns;
    if (regularColumns) tableProps.RegularColumns = regularColumns;
    if (table.defaultTtl && table.defaultTtl > 0) tableProps.DefaultTimeToLive = table.defaultTtl;

    if (billingMode === 'PROVISIONED') {
      const capacity = capacityForTable(table.keyspace);
      tableProps.BillingMode = {
        Mode: 'PROVISIONED',
        ProvisionedThroughput: {
          ReadCapacityUnits: capacity.readCapacityUnits,
          WriteCapacityUnits: capacity.writeCapacityUnits,
        },
      };
      if (autoScalingTargetPercent > 0) {
        const target = Math.min(90, Math.max(20, Math.round(autoScalingTargetPercent)));
        const setting = (units: number) => ({
          AutoScalingDisabled: false,
          MinimumUnits: 1,
          // Room to absorb peaks above the sampled average.
          MaximumUnits: Math.min(MAX_CAPACITY_UNITS, Math.max(units * 2, 10)),
          ScalingPolicy: { TargetTrackingScalingPolicyConfiguration: { TargetValue: target } },
        });
        tableProps.AutoScalingSpecifications = {
          ReadCapacityAutoScaling: setting(capacity.readCapacityUnits),
          WriteCapacityAutoScaling: setting(capacity.writeCapacityUnits),
        };
      }
    }

    if (pointInTimeRecovery || pitrKeyspaceSet.has(table.keyspace)) {
      tableProps.PointInTimeRecoveryEnabled = true;
    }

    template.Resources[logicalId] = {
      Type: 'AWS::Cassandra::Table',
      Properties: tableProps,
      DependsOn: deps.length === 1 ? deps[0] : deps,
    };
  }

  // Create ResourceGroups for single-region table batches.
  // Each group contains up to BATCH_SIZE tables and depends on the previous group,
  // ensuring batches are created sequentially.
  if (singleRegionTableIds.length > 0) {
    const totalBatches = Math.ceil(singleRegionTableIds.length / BATCH_SIZE);
    for (let batch = 0; batch < totalBatches; batch++) {
      const groupId = `TableBatch${batch + 1}`;
      const batchTables = singleRegionTableIds.slice(batch * BATCH_SIZE, (batch + 1) * BATCH_SIZE);
      const groupDeps: string[] = [...batchTables];
      if (batch > 0) {
        groupDeps.push(`TableBatch${batch}`);
      }
      template.Resources[groupId] = {
        Type: 'AWS::ResourceGroups::Group',
        Properties: {
          Name: groupId,
          Description: `Keyspaces table creation batch ${batch + 1} of ${totalBatches}`,
        } as any,
        DependsOn: groupDeps,
      };
    }
  }

  if (warnings.length > 0) {
    template.Metadata = { Warnings: warnings };
  }

  return template;
}

export function templateToJson(template: CfnTemplate): string {
  return JSON.stringify(template, null, 2);
}

export function templateToYaml(template: CfnTemplate): string {
  return toYaml(template, 0);
}

function toYaml(obj: unknown, indent: number): string {
  const prefix = '  '.repeat(indent);

  if (obj === null || obj === undefined) return 'null';
  if (typeof obj === 'boolean') return obj ? 'true' : 'false';
  if (typeof obj === 'number') return String(obj);
  if (typeof obj === 'string') {
    if (obj.includes('\n') || obj.includes(':') || obj.includes('#') ||
        obj.startsWith('{') || obj.startsWith('[') || obj.startsWith('*') ||
        obj.startsWith('&') || obj.startsWith('!') || /^\d/.test(obj) ||
        obj === 'true' || obj === 'false' || obj === 'null' || obj === '') {
      return `'${obj.replace(/'/g, "''")}'`;
    }
    return obj;
  }

  if (Array.isArray(obj)) {
    if (obj.length === 0) return '[]';
    let out = '';
    for (const item of obj) {
      if (typeof item === 'object' && item !== null && !Array.isArray(item)) {
        const entries = Object.entries(item);
        out += `\n${prefix}- ${entries[0][0]}: ${toYaml(entries[0][1], indent + 2)}`;
        for (let i = 1; i < entries.length; i++) {
          out += `\n${prefix}  ${entries[i][0]}: ${toYaml(entries[i][1], indent + 2)}`;
        }
      } else {
        out += `\n${prefix}- ${toYaml(item, indent + 1)}`;
      }
    }
    return out;
  }

  if (typeof obj === 'object') {
    const entries = Object.entries(obj as Record<string, unknown>);
    if (entries.length === 0) return '{}';
    let out = '';
    for (const [key, value] of entries) {
      if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
        out += `\n${prefix}${key}:`;
        out += toYaml(value, indent + 1);
      } else if (Array.isArray(value)) {
        out += `\n${prefix}${key}:${toYaml(value, indent + 1)}`;
      } else {
        out += `\n${prefix}${key}: ${toYaml(value, indent + 1)}`;
      }
    }
    return out;
  }

  return String(obj);
}
