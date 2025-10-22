// Parsing functions for Cassandra data files
export const GIGABYTE = 1000000000;
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
        const trimmedLine = line.trim();
        
        // Look for datacenter lines using regex
        if (/Datacenter\s*:/i.test(trimmedLine)) {
            // Save previous datacenter if exists
            if (currentDC) {
                datacenters.set(currentDC, nodeCount);
            }
            
            // Extract datacenter name using regex
            const match = trimmedLine.match(/Datacenter\s*:\s*(.+)/i);
            if (match && match[1]) {
                currentDC = match[1].trim();
                nodeCount = 0;
            }
        }
        // Look for node status lines using regex
        else if (currentDC && (/^UN\b/i.test(trimmedLine) || /^DN\b/i.test(trimmedLine))) {
            nodeCount++;
        }
    }
    
    // Save the last datacenter
    if (currentDC) {
        datacenters.set(currentDC, nodeCount);
    }
    
    return datacenters;
};

export const getKeyspaceCassandraAggregate=(cassandra_set, datacenter) => {

    /*
    'data': {
            'keyspaces': {
                'keyspace_name': {
                    'type': 'system' or 'user'
                    'total_live_space_gb': Decimal,
                    'uncompressed_single_replica_gb': Decimal,
                    'avg_row_size_bytes': Decimal,
                    'writes_per_second': Decimal,
                    'reads_per_second': Decimal,
                    'ttls_per_second': Boolean,
                    'sample_count': Decimal
                }
            }
        }
    }
    */

    const keyspace_aggregate = {};

    for (const [keyspace, keyspaceData] of Object.entries(cassandra_set.data.keyspaces)) {

        if(keyspaceData.type === 'system'){
            continue;
        }

        const number_of_nodes = keyspaceData.dcs[datacenter].number_of_nodes;
        const replication_factor = keyspaceData.dcs[datacenter].replication_factor;
        
        console.log("number_of_nodes: " + number_of_nodes);
        console.log("replication_factor: " + replication_factor);

        let keyspace_writes_total = 0;
        let keyspace_reads_total = 0;
        let total_live_space = 0;
        let uncompressed_single_replica = 0;
        let write_row_size_bytes = 0;
        let read_row_size_bytes = 0;
        let keyspace_ttls_total = 0;


        for (const [table, tableData] of Object.entries(keyspaceData.dcs[datacenter].tables)) {
            
            keyspace_writes_total += tableData.writes_monthly/ tableData.sample_count;
            total_live_space += tableData.total_compressed_bytes/ tableData.sample_count;
            uncompressed_single_replica += tableData.total_uncompressed_bytes/ tableData.sample_count;
            write_row_size_bytes += tableData.writes_monthly * tableData.avg_row_size_bytes / tableData.sample_count;
            read_row_size_bytes += tableData.reads_monthly * tableData.avg_row_size_bytes / tableData.sample_count;
            keyspace_reads_total += tableData.reads_monthly  / tableData.sample_count;
            keyspace_ttls_total += (tableData.has_ttl ? tableData.writes_monthly/tableData.sample_count : 0);
            
        }

        
        const average_read_row_size_bytes = read_row_size_bytes / (keyspace_reads_total > 0 ? keyspace_reads_total : 1);
        const average_write_row_size_bytes = write_row_size_bytes / (keyspace_writes_total > 0 ? keyspace_writes_total : 1);
        keyspace_aggregate[keyspace] = {
            keyspace_name: keyspace,
            keyspace_type: keyspaceData.type,
            replication_factor: replication_factor,
            total_live_space_gb: total_live_space  * number_of_nodes / GIGABYTE,
            uncompressed_single_replica_gb: uncompressed_single_replica * number_of_nodes / replication_factor / GIGABYTE,
            avg_write_row_size_bytes: average_write_row_size_bytes,
            avg_read_row_size_bytes: average_read_row_size_bytes,
            writes_per_second: keyspace_writes_total / SECONDS_PER_MONTH * number_of_nodes/replication_factor,
            reads_per_second: keyspace_reads_total/ SECONDS_PER_MONTH * number_of_nodes /((replication_factor - 1 > 0)? replication_factor -1: 1),
            ttls_per_second: keyspace_ttls_total  / SECONDS_PER_MONTH * number_of_nodes/replication_factor,
        }
        
    }
    return keyspace_aggregate;
}
export const buildCassandraLocalSet = (samples, statusData) => {

    /*
    'data': {
            'keyspaces': {
                'keyspace_name': {
                    'type': 'system' or 'user',
                    'dcs': {
                        'dc_name': {
                            'number_of_nodes': Decimal,
                            'replication_factor': Decimal,
                            'tables': {
                                'table_name': {
                                    'total_compressed_bytes': Decimal,
                                    'total_uncompressed_bytes': Decimal,
                                    'avg_row_size_bytes': Decimal,
                                    'writes_monthly': Decimal,
                                    'reads_monthly': Decimal,
                                    'has_ttl': Boolean,
                                    'sample_count': Decimal,
                                }
                            }
                        }
                    }
                }
            }
        }
    */
    const result = {
        data: {
            keyspaces: {}
        }
    };

    // Process each datacenter's samples
    for (const [dcName, dcData] of Object.entries(samples)) {

        // Get the number of nodes for the datacenter
        const numberOfNodes = statusData.get(dcName);

        for (const [nodeId, nodeData] of Object.entries(dcData)) {
            const tablestatsData = nodeData.tablestats_data;
            const schema = nodeData.schema;
            const infoData = nodeData.info_data;
            const rowSizeData = nodeData.row_size_data;
            const uptimeSeconds = infoData.uptime_seconds;

            console.log("rowSizeData: " + JSON.stringify(rowSizeData));
            console.log("tablestatsData: " + JSON.stringify(tablestatsData));
            // Process each keyspace
            for (const [keyspaceName, keyspaceData] of Object.entries(tablestatsData)) {
                // Skip if filtering for a single keyspace
                if (schema && schema[keyspaceName]) {
                    if(!schema[keyspaceName].datacenters[dcName]){
                        continue;
                    }
                }
                console.log("keyspaceName: " + keyspaceName);
                
                // Initialize keyspace structure if it doesn't exist
                if (!result.data.keyspaces[keyspaceName]) {
                    result.data.keyspaces[keyspaceName] = {
                        type: system_keyspaces.has(keyspaceName) ? 'system' : 'user',
                        dcs: {}
                    };
                }

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

                    console.log("tableName: " + tableName);
                    // Initialize table structure if it doesn't exist
                    if (!result.data.keyspaces[keyspaceName].dcs[dcName].tables[tableName]) {

                         // Get row size and TTL info
                        const fullyQualifiedTableName = `${keyspaceName}.${tableName}`;
                        let hasTtl = false;
                        let averageBytes = 1;
                        
                        if (rowSizeData[fullyQualifiedTableName]) {
                            const avgNumber = rowSizeData[fullyQualifiedTableName].average || '1';
                            
                            const parsedBytes = parseInt(avgNumber);
                            
                            // Check for NaN and set to default value if invalid
                            if (isNaN(parsedBytes) || parsedBytes <= 0) {
                                console.log(`Invalid average bytes value for ${fullyQualifiedTableName}: "${avgNumber}", using default value 1`);
                                averageBytes = 1;
                            } else {
                                averageBytes = parsedBytes;
                            }
                            
                            const ttlStr = rowSizeData[fullyQualifiedTableName]['default-ttl'] || 'y';
                            hasTtl = ttlStr.trim() === 'n';
                        }
                        console.log("keyspace: " + keyspaceName + " table: " + tableName + " averageBytes: " + averageBytes);
                        
                        result.data.keyspaces[keyspaceName].dcs[dcName].tables[tableName] = {
                            table_name: tableName,
                            total_compressed_bytes: 0,
                            total_uncompressed_bytes: 0,
                            avg_row_size_bytes: averageBytes,
                            writes_monthly: 0,
                            reads_monthly: 0,
                            has_ttl: hasTtl,
                            sample_count: 0
                        };
                    }

                    // Get table data
                    let spaceUsed = tableData.space_used || 0; // compressed bytes
                    // Check for NaN and set to 0 if invalid
                    if (isNaN(spaceUsed) || spaceUsed === null || spaceUsed === undefined) {
                        console.log("spaceUsed is NaN");
                        spaceUsed = 0;
                    }
                    const ratio = spaceUsed > 0 ? tableData.compression_ratio : 1;
                    let readCount = tableData.read_count || 0;
                    let writeCount = tableData.write_count || 0;
                    
                    // Check for NaN and set to 0 if invalid
                    if (isNaN(readCount) || readCount === null || readCount === undefined) {
                        console.log("readCount is NaN");
                        readCount = 0;
                    }
                    if (isNaN(writeCount) || writeCount === null || writeCount === undefined) {
                        console.log("writeCount is NaN");
                        writeCount = 0;
                    }

                    // Update table data
                    const table = result.data.keyspaces[keyspaceName].dcs[dcName].tables[tableName];
                    table.total_compressed_bytes += spaceUsed;
                    table.total_uncompressed_bytes += spaceUsed / ratio;
                    table.writes_monthly += (writeCount / uptimeSeconds) * SECONDS_PER_MONTH;
                    table.reads_monthly += (readCount / uptimeSeconds) * SECONDS_PER_MONTH;
                    table.sample_count += 1;
                }
            }
        }
    }
    console.log(result);
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
        
        // Look for the line containing "Uptime (seconds)" using regex
        if (/Uptime\s*\(seconds\)/i.test(trimmedLine)) {
            // Format is something like: "Uptime (seconds): X"
            const match = trimmedLine.match(/Uptime\s*\(seconds\)\s*:\s*(.+)/i);
            if (match && match[1]) {
                const uptimeStr = match[1].trim();
                try {
                    uptimeSeconds = parseFloat(uptimeStr);
                } catch (error) {
                    throw new Error(`Error parsing uptime in seconds: ${uptimeStr}`);
                }
            }
        }
        
        // Look for the line containing "ID" using regex
        if (/^ID\s*:/i.test(trimmedLine)) {
            const match = trimmedLine.match(/^ID\s*:\s*(.+)/i);
            if (match && match[1]) {
                id = match[1].trim();
            }
        }

        // Look for the line containing "Data Center" using regex
        if (/Data\s+Center\s*:/i.test(trimmedLine)) {
            const match = trimmedLine.match(/Data\s+Center\s*:\s*(.+)/i);
            if (match && match[1]) {
                dc = match[1].trim();
            }
        }
    }

    return {
        uptime_seconds: uptimeSeconds,
        dc: dc,
        id: id
    };
};

export const parse_cassandra_schema = (schemaContent, datacenter) => {
   
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
        }else if (ks.class === "SimpleStrategy") {

            const replicationFactor = ks.rest.match(/'replication_factor':\s*'(\d+)'/).slice(1);
            console.log("replicationFactor: " + replicationFactor + " datacenter: " + datacenter);

            dcRepl[datacenter] = parseInt(replicationFactor, 10);
            
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

        // Skip lines that don't contain '=' or look like error lines using regex
        if (!/=/i.test(trimmedLine) || /NoHostAvailable/i.test(trimmedLine)) {
            continue;
        }

        // Use regex to split keyspace.table from the rest more reliably
        const match = trimmedLine.match(/^(.+?)\s*=\s*(.+)$/);
        if (!match) {
            continue; // Skip if we don't have a proper match
        }
        
        const [, keyName, right] = match;
        const trimmedKeyName = keyName.trim();

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
            if (!/:\s*/i.test(trimmedField)) {
                // Skip malformed fields
                continue;
            }
            const [k, v] = trimmedField.split(':');
            if (!k || !v) {
                continue; // Skip if we don't have both key and value
            }
            const key = k.trim();
            const val = v.replace('bytes', '').trim();
            // Store as is (string), cast later as needed
            valueDict[key] = val;
        }
        result[trimmedKeyName] = valueDict;
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

export const handleSchemaFile = async (file, datacenter) =>{
    if (!file) {
        throw new Error('No file selected');
    }

    try {
        const content = await file.text();
        if (!content) {
            throw new Error('File is empty');
        }

        const parsedData = parse_cassandra_schema(content, datacenter);
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
