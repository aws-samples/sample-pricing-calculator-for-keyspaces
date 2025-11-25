import { system_keyspaces, REPLICATION_FACTOR, SECONDS_PER_MONTH, GIGABYTE } from '../utils/Constants';
// Parsing functions for Cassandra data files
// moved GIGABYTE to ../utils/Constants
const GOSSIP_OUT_BYTES = 1638;
const GOSSIP_IN_BYTES = 3072;
export const HOURS_PER_MONTH = (365/12) * 24;
export const WRITE_UNIT_SIZE = 1024;  // 1KB
export const READ_UNIT_SIZE = 4096;   // 4KB
const ONE_MILLION = 1000000;

// moved to ../utils/Constants

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

// moved getKeyspaceCassandraAggregate to src/utils/Transformers.js
// moved buildCassandraLocalSet to src/utils/Transformers.js

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

/**
 * Parse TCO (Total Cost of Ownership) Info JSON file.
 * 
 * @param {string} data - JSON string content
 * @returns {Object} Parsed JSON object with validated structure
 * @throws {Error} If JSON is invalid or required fields are missing
 */
export const parseTCOInfo = (data) => {
    let obj;

    try {
        obj = JSON.parse(data);
        console.log("✅ JSON parsed successfully");
    } catch (err) {
        console.error("❌ Invalid JSON:", err.message);
        throw new Error(`Invalid JSON: ${err.message}`);
    }

    // Basic sanity checks
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
