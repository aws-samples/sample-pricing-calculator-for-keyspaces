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

// --- File handlers ---

interface FileWithText {
  text(): Promise<string>;
}

export const handleTablestatsFile = async (file: FileWithText): Promise<TablestatsResult> => {
  try {
    const content = await file.text();
    return parse_nodetool_tablestats(content);
  } catch (error) {
    console.error('Error parsing tablestats file:', error);
    throw new Error('Failed to parse tablestats file');
  }
};

export const handleSchemaFile = async (file: FileWithText | null, datacenter: string): Promise<SchemaInfo> => {
  if (!file) throw new Error('No file selected');
  try {
    const content = await file.text();
    if (!content) throw new Error('File is empty');
    const parsedData = parse_cassandra_schema(content, datacenter);
    if (!parsedData || Object.keys(parsedData).length === 0) {
      throw new Error('No valid schema definitions found in file');
    }
    return parsedData;
  } catch (error: unknown) {
    console.error('Error parsing schema file:', error);
    throw new Error(`Failed to parse schema file: ${(error as Error).message}`);
  }
};

export const handleInfoFile = async (file: FileWithText | null): Promise<NodetoolInfoResult> => {
  if (!file) throw new Error('No file selected');
  try {
    const content = await file.text();
    if (!content) throw new Error('File is empty');
    const parsedData = parseNodetoolInfo(content);
    if (!parsedData?.uptime_seconds) {
      throw new Error('No valid info data found in file');
    }
    return parsedData;
  } catch (error: unknown) {
    console.error('Error parsing info file:', error);
    throw new Error(`Failed to parse info file: ${(error as Error).message}`);
  }
};

export const handleRowSizeFile = async (file: FileWithText | null): Promise<RowSizeInfo> => {
  if (!file) throw new Error('No file selected');
  try {
    const content = await file.text();
    if (!content) throw new Error('File is empty');
    const parsedData = parseRowSizeInfo(content);
    if (!parsedData || Object.keys(parsedData).length === 0) {
      throw new Error('No valid row size data found in file');
    }
    return parsedData;
  } catch (error: unknown) {
    console.error('Error parsing row size file:', error);
    throw new Error(`Failed to parse row size file: ${(error as Error).message}`);
  }
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
