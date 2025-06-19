// Parsing functions for Cassandra data files
const GIGABYTE = 1000000000;
const GOSSIP_OUT_BYTES = 1638;
const GOSSIP_IN_BYTES = 3072;
export const REPLICATION_FACTOR = 3;
export const SECONDS_PER_MONTH = (365/12) * (24 * 60 * 60);
export const HOURS_PER_MONTH = (365/12) * 24;
export const WRITE_UNIT_SIZE = 1024;  // 1KB
export const READ_UNIT_SIZE = 4096;   // 4KB
const ONE_MILLION = 1000000;

export const system_keyspaces = new Set([
    'OpsCenter', 'dse_insights_local', 'solr_admin',
    'dse_system', 'HiveMetaStore', 'system_auth',
    'dse_analytics', 'system_traces', 'dse_audit', 'system',
    'dse_system_local', 'dsefs', 'system_distributed', 'system_schema',
    'dse_perf', 'dse_insights', 'system_backups', 'dse_security',
    'dse_leases', 'system_distributed_everywhere', 'reaper_db'
]);

export const parseNodetoolStatus = (content) => {
    const lines = content.split('\n');
    const datacenters = new Map();
    let currentDC = null;
    let nodeCount = 0;

    for (const line of lines) {
        
        if (line.includes('Datacenter:')) {
           
            if (currentDC) {
                datacenters.set(currentDC, nodeCount);
            }
            currentDC = line.split('Datacenter:')[1].trim();
            nodeCount = 0;
        }
        else if (currentDC && (line.includes('UN') || line.includes('DN'))) {
            nodeCount++;
        }
    }
    if (currentDC) {
        datacenters.set(currentDC, nodeCount);
    }
    
    return datacenters;
};

export const buildCassandraLocalSet = (samples, statusData, singleKeyspace = null) => {
    const result = {
        data: {
            keyspaces: {}
        }
    };

    // Process each datacenter's samples
    for (const [dcName, dcData] of Object.entries(samples)) {
        for (const [nodeId, nodeData] of Object.entries(dcData.nodes)) {
            const tablestatsData = nodeData.tablestats_data;
            const schema = nodeData.schema;
            const infoData = nodeData.info_data;
            const rowSizeData = nodeData.row_size_data;
            
            const uptimeSeconds = infoData.uptime_seconds;

            // Process each keyspace
            for (const [keyspaceName, keyspaceData] of Object.entries(tablestatsData)) {
                // Skip if filtering for a single keyspace
                if (singleKeyspace && keyspaceName !== singleKeyspace) {
                    continue;
                }

                // Initialize keyspace structure if it doesn't exist
                if (!result.data.keyspaces[keyspaceName]) {
                    result.data.keyspaces[keyspaceName] = {
                        type: system_keyspaces.has(keyspaceName) ? 'system' : 'user',
                        dcs: {}
                    };
                }

                const numberOfNodes = statusData.datacenters[dcName].node_count;

                let replicationFactor = REPLICATION_FACTOR;
                if (schema && schema[keyspaceName]) {
                    replicationFactor = schema[keyspaceName].datacenters[dcName];
                }

                // Initialize datacenter structure if it doesn't exist
                if (!result.data.keyspaces[keyspaceName].dcs[dcName]) {
                    result.data.keyspaces[keyspaceName].dcs[dcName] = {
                        number_of_nodes: numberOfNodes,
                        replication_factor: replicationFactor,
                        tables: {}
                    };
                }

                // Process each table in the keyspace
                for (const [tableName, tableData] of Object.entries(keyspaceData)) {
                    // Initialize table structure if it doesn't exist
                    if (!result.data.keyspaces[keyspaceName].dcs[dcName].tables[tableName]) {
                        result.data.keyspaces[keyspaceName].dcs[dcName].tables[tableName] = {
                            total_compressed_bytes: 0,
                            total_uncompressed_bytes: 0,
                            avg_row_size_bytes: 0,
                            writes_monthly: 0,
                            reads_monthly: 0,
                            has_ttl: false,
                            sample_count: 0
                        };
                    }

                    // Get table data
                    const spaceUsed = tableData.space_used;  // compressed bytes
                    const ratio = tableData.space_used > 0 ? tableData.compression_ratio : 1;
                    const readCount = tableData.read_count;
                    const writeCount = tableData.write_count;

                    // Calculate uncompressed size
                    const uncompressedSize = spaceUsed / ratio;

                    // Get row size and TTL info
                    const fullyQualifiedTableName = `${keyspaceName}.${tableName}`;
                    let averageBytes = 1;
                    let hasTtl = false;

                    if (rowSizeData[fullyQualifiedTableName]) {
                        const avgStr = rowSizeData[fullyQualifiedTableName].average || '0 bytes';
                        const avgNumberStr = avgStr.split(' ')[0];
                        averageBytes = parseInt(avgNumberStr);
                        const ttlStr = rowSizeData[fullyQualifiedTableName]['default-ttl'] || 'y';
                        hasTtl = ttlStr.trim() === 'n';
                    }

                    // Update table data
                    const table = result.data.keyspaces[keyspaceName].dcs[dcName].tables[tableName];
                    table.total_compressed_bytes += spaceUsed;
                    table.total_uncompressed_bytes += uncompressedSize;
                    table.avg_row_size_bytes = averageBytes;
                    table.writes_monthly += (writeCount / uptimeSeconds) * SECONDS_PER_MONTH;
                    table.reads_monthly += (readCount / uptimeSeconds) * SECONDS_PER_MONTH;
                    table.has_ttl = hasTtl;
                    table.sample_count += 1;
                }
            }
        }
    }

    return result;
};

export const parse_nodetool_tablestats = (content) => {
    const lines = content.split('\n');
    const data = {};
    let currentKeyspace = null;
    let currentTable = null;
    let spaceUsed = null;
    let compressionRatio = null;
    let writeCount = null;
    let readCount = null;

    for (const line of lines) {
        const trimmedLine = line.trim();

        // Identify when we start a new keyspace block
        if (trimmedLine.startsWith('Keyspace')) {
            // Format: "Keyspace : keyspace_name" or "Keyspace: keyspace_name"
            console.log("line: " + trimmedLine);
            
            // Use regex to extract keyspace name more reliably
            const keyspaceMatch = trimmedLine.match(/Keyspace\s*:\s*(.+)/);
            if (keyspaceMatch && keyspaceMatch[1]) {
                currentKeyspace = keyspaceMatch[1].trim();
                console.log("Extracted keyspace: " + currentKeyspace);
                
                // Initialize the keyspace in the dictionary if new
                if (!data[currentKeyspace]) {
                    data[currentKeyspace] = {};
                }
            } else {
                currentKeyspace = null;
            }
            currentTable = null;
        }

        // Identify when we start a new table block within the current keyspace
        if (currentKeyspace && (trimmedLine.startsWith('Table:') || trimmedLine.startsWith('Table (index):'))) {
            // Format: "Table: table_name"
            const tableMatch = trimmedLine.match(/Table(?:\s*\(index\))?\s*:\s*(.+)/);
            if (tableMatch && tableMatch[1]) {
                currentTable = tableMatch[1].trim();
                // Reset collected stats for this new table
                spaceUsed = null;
                compressionRatio = null;
                writeCount = null;
                readCount = null;
            }
        }

        // For lines within a table block, parse the required stats
        if (currentKeyspace && currentTable) {
            if (trimmedLine.includes('Space used (live):')) {
                // Format: "Space used (live): X"
                const match = trimmedLine.match(/Space used \(live\)\s*:\s*(.+)/);
                if (match && match[1]) {
                    const spaceUsedStr = match[1].trim();
                    try {
                        spaceUsed = parseFloat(spaceUsedStr);
                    } catch (e) {
                        spaceUsed = 0;
                    }
                }
            } else if (trimmedLine.includes('SSTable Compression Ratio:')) {
                // Format: "SSTable Compression Ratio: X"
                const match = trimmedLine.match(/SSTable Compression Ratio\s*:\s*(.+)/);
                if (match && match[1]) {
                    const ratioStr = match[1].trim();
                    try {
                        compressionRatio = parseFloat(ratioStr);
                    } catch (e) {
                        compressionRatio = 1;
                    }
                }
            } else if (trimmedLine.includes('Local read count:')) {
                // Format: "Local read count: X"
                const match = trimmedLine.match(/Local read count\s*:\s*(.+)/);
                if (match && match[1]) {
                    const readStr = match[1].trim();
                    try {
                        readCount = parseFloat(readStr);
                    } catch (e) {
                        readCount = 0;
                    }
                }
            } else if (trimmedLine.includes('Local write count:')) {
                // Format: "Local write count: X"
                const match = trimmedLine.match(/Local write count\s*:\s*(.+)/);
                if (match && match[1]) {
                    const writeStr = match[1].trim();
                    try {
                        writeCount = parseFloat(writeStr);
                    } catch (e) {
                        writeCount = 0;
                    }
                }

                // After identifying a write_count line, we expect that we now have all necessary metrics.
                // Only store the table data once all required fields are found.
                if (spaceUsed !== null &&
                    compressionRatio !== null &&
                    readCount !== null &&
                    writeCount !== null) {
                    data[currentKeyspace][currentTable] = {
                        space_used: spaceUsed,
                        compression_ratio: compressionRatio,
                        read_count: readCount,
                        write_count: writeCount
                    };

                    // Reset for the next table
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

export const parseNodetoolInfo = (content) => {
    const lines = content.split('\n');
    let uptimeSeconds = 1;
    let id = '';
    let dc = '';

    for (const line of lines) {
        const trimmedLine = line.trim();
        
        // Look for the line containing "Uptime (seconds)"
        if (trimmedLine.includes("Uptime (seconds)")) {
             // Format is something like: "Uptime (seconds): X"
            const parts = trimmedLine.replace('\n', ' ').replace('\\', '').split(':', 1);
            if (parts.length === 2) {
                const uptimeStr = parts[1].trim();
                try {
                    uptimeSeconds = parseFloat(uptimeStr);
                } catch (error) {
                    throw new Error(`Error parsing uptime in seconds: ${parts[1]}`);
                }
            }
        }
        
        // Look for the line containing "ID"
        if (trimmedLine.includes("ID")) {
            console.log(trimmedLine);
            const idParts = trimmedLine.replace('\n', ' ').replace('\\', '').split(':', 1);
            if (idParts.length === 2) {
                id = idParts[1].trim();
            }
        }

        // Look for the line containing "Data Center"
        if (trimmedLine.includes("Data Center")) {
            console.log(trimmedLine);
            const dcParts = trimmedLine.replace('\n', ' ').replace('\\', '').split(':', 1);
            if (dcParts.length === 2) {
                dc = dcParts[1].trim();
            }
        }
    }

    return {
        uptime_seconds: uptimeSeconds,
        dc: dc,
        id: id
    };
};

export const parse_cassandra_schema = (schemaContent) => {
    // Regular expressions for matching keyspace and table definitions
    const ksPattern = /CREATE KEYSPACE (\w+)\s+WITH replication = \{[^}]*'class': '(\w+)'(?:,\s*)?([^}]*)\}/gi;
    const tablePattern = /CREATE TABLE (\w+)\.(\w+)/gi;

    // Extract keyspaces
    const keyspaces = [];
    let ksMatch;
    while ((ksMatch = ksPattern.exec(schemaContent)) !== null) {
        keyspaces.push({
            name: ksMatch[1],
            class: ksMatch[2],
            rest: ksMatch[3]
        });
    }

    // Extract tables
    const tables = [];
    let tableMatch;
    while ((tableMatch = tablePattern.exec(schemaContent)) !== null) {
        tables.push({
            keyspace: tableMatch[1],
            table: tableMatch[2]
        });
    }

    // Build dictionary
    const ksInfo = {};
    for (const ks of keyspaces) {
        const dcRepl = {};
        if (ks.class === "NetworkTopologyStrategy") {
            // Extract datacenter replication factors
            const dcEntries = ks.rest.match(/'([^']+)':\s*'(\d+)'/g);
            if (dcEntries) {
                for (const entry of dcEntries) {
                    const [dc, rf] = entry.match(/'([^']+)':\s*'(\d+)'/).slice(1);
                    dcRepl[dc] = parseInt(rf, 10);
                }
            }
        }
        ksInfo[ks.name] = {
            class: ks.class,
            datacenters: dcRepl,
            tables: []
        };
    }

    // Attach tables to their keyspaces
    for (const table of tables) {
        if (ksInfo[table.keyspace]) {
            ksInfo[table.keyspace].tables.push(table.table);
        }
    }

    return ksInfo;
};

export const parseRowSizeInfo = (content) => {
    const lines = content.split('\n');
    const result = {};

    for (const line of lines) {
        const trimmedLine = line.trim();
        // Skip lines that don't contain '=' or look like error lines
        if (!trimmedLine.includes('=') || trimmedLine.includes('NoHostAvailable')) {
            continue;
        }

        // Split keyspace.table from the rest
        const [left, right] = trimmedLine.split('=', 1);
        const keyName = left.trim();

        // The right side should be something like:
        // { lines: 1986, columns: 12, average: 849 bytes, ... }
        const trimmedRight = right.trim();
        if (!trimmedRight.startsWith('{') || !trimmedRight.endsWith('}')) {
            continue;
        }

        // Remove the braces
        const inner = trimmedRight.slice(1, -1).trim();

        // Split by commas that separate fields
        // Each field looks like "lines: 1986" or "average: 849 bytes"
        const fields = inner.split(',');

        const valueDict = {};
        for (const field of fields) {
            const trimmedField = field.trim();
            if (!trimmedField.includes(': ')) {
                // Skip malformed fields
                continue;
            }
            const [k, v] = trimmedField.split(':', 1);
            const key = k.trim();
            const val = v.trim();
            // Store as is (string), cast later as needed
            valueDict[key] = val;
        }

        result[keyName] = valueDict;
    }

    return result;
};

// File handling functions
export const handleTablestatsFile = async (file) => {
    try {
        const content = await file.text();
        const parsedData = parse_nodetool_tablestats(content);
        return parsedData;
    } catch (error) {
        console.error('Error parsing tablestats file:', error);
        throw new Error('Failed to parse tablestats file');
    }
};

export const handleSchemaFile = async (file) => {
    if (!file) {
        throw new Error('No file selected');
    }

    try {
        const content = await file.text();
        if (!content) {
            throw new Error('File is empty');
        }

        const parsedData = parse_cassandra_schema(content);
        if (!parsedData || Object.keys(parsedData).length === 0) {
            throw new Error('No valid schema definitions found in file');
        }

        return parsedData;
    } catch (error) {
        console.error('Error parsing schema file:', error);
        throw new Error(`Failed to parse schema file: ${error.message}`);
    }
};

export const handleInfoFile = async (file) => {
    if (!file) {
        throw new Error('No file selected');
    }

    try {
        const content = await file.text();
        if (!content) {
            throw new Error('File is empty');
        }

        const parsedData = parseNodetoolInfo(content);
        if (!parsedData || !parsedData.uptime_seconds) {
            throw new Error('No valid info data found in file');
        }

        return parsedData;
    } catch (error) {
        console.error('Error parsing info file:', error);
        throw new Error(`Failed to parse info file: ${error.message}`);
    }
};

export const handleRowSizeFile = async (file) => {
    if (!file) {
        throw new Error('No file selected');
    }

    try {
        const content = await file.text();
        if (!content) {
            throw new Error('File is empty');
        }

        const parsedData = parseRowSizeInfo(content);
        if (!parsedData || Object.keys(parsedData).length === 0) {
            throw new Error('No valid row size data found in file');
        }

        return parsedData;
    } catch (error) {
        console.error('Error parsing row size file:', error);
        throw new Error(`Failed to parse row size file: ${error.message}`);
    }
}; 