import React, { useState } from 'react';
import {
    Box,
    SpaceBetween,
    FormField,
    FileUpload,
    Container,
    Header,
    Select,
    Button,
    Alert,
    Table
} from '@cloudscape-design/components';
import { parseNodetoolStatus, parse_nodetool_tablestats, parseNodetoolInfo, parse_cassandra_schema, parseRowSizeInfo, buildCassandraLocalSet, getKeyspaceCassandraAggregate } from './ParsingHelpers';
import { awsRegions } from '../constants/regions';

// Function to get region for a datacenter - can be used throughout the application
export const getDatacenterRegion = (datacenterName, regionsMap) => {
    return regionsMap[datacenterName] || null;
};

// Function to get all datacenter-region mappings
export const getDatacenterRegionMap = (regionsMap) => {
    const mapping = {};
    Object.entries(regionsMap).forEach(([datacenter, region]) => {
        if (region) {
            mapping[datacenter] = region;
        }
    });
    return mapping;
};


// Results Table Component
const ResultsTable = ({ results }) => {
    if (!results) return null;
    
    const tableItems = Object.entries(results).map(([keyspace, data]) => ({
        /***type: keyspaceData.type,
                    total_live_space_gb: 0,
                    uncompressed_single_replica_gb: 0,
                    avg_row_size_bytes: 0,
                    writes_monthly: 0,
                    ttls_monthly: 0,
                    reads_monthly: 0,
                    sample_count: 0 */
        keyspace,
        keyspace_type: data.type,
        writes_per_second: Math.round(data.writes_per_second),
        reads_per_second: Math.round(data.reads_per_second),
        ttl_deletes_per_second: Math.round(data.ttl_deletes_per_second),
        avg_row_size_bytes: Math.round(data.avg_row_size_bytes),
        total_live_space_gb: Math.round(data.total_live_space_gb),
        ttls_per_second: Math.round(data.ttls_per_second),
        uncompressed_single_replica_gb: Math.round(data.uncompressed_single_replica_gb)
    }));

    return (
        <Table 
            items={tableItems}
            columnDefinitions={[
                {
                    id: 'keyspace',
                    header: 'Keyspace',
                    cell: item => item.keyspace,
                    align: 'left'
                },
                {
                    id: 'writes_per_second',
                    header: 'Writes per second',
                    cell: item => item.writes_per_second,
                    align: 'right'
                },
                {
                    id: 'reads_per_second',
                    header: 'Reads per second',
                    cell: item => item.reads_per_second,
                    align: 'right'
                },
                {
                    id: 'avg_row_size_bytes',
                    header: 'Avg row size',
                    cell: item => item.avg_row_size_bytes,
                    align: 'right'
                },
                {
                    id: 'total_live_space_gb',
                    header: 'total live space',
                    cell: item => item.total_live_space_gb,
                    align: 'right'
                },
                {
                    id: 'Single uncompressed_single_replica_gb of data',
                    header: 'Single uncompressed replica',
                    cell: item => item.uncompressed_single_replica_gb,
                    align: 'right'
                },
                {
                    id: 'ttl_deletes_per_second',
                    header: 'ttl deletes per second',
                    cell: item => item.ttls_per_second,
                    align: 'right'
                }
            ]}
            sortingDisabled
            variant="bordered"
        />
    );
};

function CassandraInput() {
    const [statusFile, setStatusFile] = useState(null);
    const [datacenters, setDatacenters] = useState([]);
    const [regions, setRegions] = useState({});
    const [datacenterFiles, setDatacenterFiles] = useState({});
    const [tablestatsData, setTablestatsData] = useState({});
    const [infoData, setInfoData] = useState({});
    const [schemaData, setSchemaData] = useState({});
    const [rowSizeData, setRowSizeData] = useState({});
    const [tablestatsValidation, setTablestatsValidation] = useState({});
    const [infoValidation, setInfoValidation] = useState({});
    const [schemaValidation, setSchemaValidation] = useState({});
    const [rowSizeValidation, setRowSizeValidation] = useState({});
    const [estimateValidation, setEstimateValidation] = useState({});
    const [estimateResults, setEstimateResults] = useState({});

    const fileUploadI18nStrings = {
        uploadButtonText: () => "Choose file",
        dropzoneText: () => "Drop files to upload, or choose file",
        removeFileAriaLabel: () => "Remove file",
        limitShowFewer: () => "Show fewer files",
        limitShowMore: () => "Show more files",
        errorIconAriaLabel: () => "Error"
    };

    // Convert awsRegions array to Select options format
    const regionOptions = awsRegions.map(region => ({
        label: region,
        value: region
    }));

    const handleStatusFileChange = async ({ detail }) => {
        const file = detail.value[0];
        if (file) {
            try {
                const content = await file.text();
                const statusData = parseNodetoolStatus(content);
                
                if (!statusData || statusData.size === 0) {
                    console.error('No datacenters found in status file');
                    return;
                }

                // Set the file
                setStatusFile(file);
                
                // Extract datacenters and their node counts from the Map
                const newDatacenters = Array.from(statusData.entries()).map(([dc, nodeCount]) => ({
                    name: dc,
                    nodeCount: nodeCount
                }));

                // Set the datacenters
                setDatacenters(newDatacenters);

                // Initialize regions state for each datacenter
                const initialRegions = {};
                const initialFiles = {};
                const initialTablestats = {};
                const initialInfo = {};
                const initialSchema = {};
                const initialRowSize = {};
                const initialTablestatsValidation = {};
                const initialInfoValidation = {};
                const initialSchemaValidation = {};
                const initialRowSizeValidation = {};
                const initialEstimateValidation = {};
                const initialEstimateResults = {};
                newDatacenters.forEach(dc => {
                    initialRegions[dc.name] = null;
                    initialFiles[dc.name] = {
                        tablestats: null,
                        info: null,
                        schema: null,
                        rowSize: null
                    };
                    initialTablestats[dc.name] = null;
                    initialInfo[dc.name] = null;
                    initialSchema[dc.name] = null;
                    initialRowSize[dc.name] = null;
                    initialTablestatsValidation[dc.name] = null;
                    initialInfoValidation[dc.name] = null;
                    initialSchemaValidation[dc.name] = null;
                    initialRowSizeValidation[dc.name] = null;
                    initialEstimateValidation[dc.name] = null;
                    initialEstimateResults[dc.name] = null;
                });
                setRegions(initialRegions);
                setDatacenterFiles(initialFiles);
                setTablestatsData(initialTablestats);
                setInfoData(initialInfo);
                setSchemaData(initialSchema);
                setRowSizeData(initialRowSize);
                setTablestatsValidation(initialTablestatsValidation);
                setInfoValidation(initialInfoValidation);
                setSchemaValidation(initialSchemaValidation);
                setRowSizeValidation(initialRowSizeValidation);
                setEstimateValidation(initialEstimateValidation);
                setEstimateResults(initialEstimateResults);

                console.log('Parsed status data:', statusData);
                console.log('New datacenters:', newDatacenters);
            } catch (error) {
                console.error('Error parsing status file:', error);
            }
        }
    };

    const handleRegionChange = (datacenter, { detail }) => {
        setRegions(prev => ({
            ...prev,
            [datacenter]: detail.selectedOption.value
        }));
    };

    const handleDatacenterFileChange = async (datacenter, fileType, { detail }) => {
        const file = detail.value[0];
        
        // Update the file state
        setDatacenterFiles(prev => ({
            ...prev,
            [datacenter]: {
                ...prev[datacenter],
                [fileType]: file
            }
        }));

        // Validate tablestats file specifically
        if (fileType === 'tablestats' && file) {
            try {
                const content = await file.text();
                const parsedData = parse_nodetool_tablestats(content);
                console.log('Parsed tablestats data:', parsedData);
                
                if (!parsedData || Object.keys(parsedData).length === 0) {
                    console.error('No valid tablestats data found in file for datacenter:', datacenter);
                    
                    // Set validation error
                    setTablestatsValidation(prev => ({
                        ...prev,
                        [datacenter]: {
                            success: false,
                            message: 'No valid tablestats data found in file'
                        }
                    }));
                } else {
                    // Count total tables across all keyspaces
                    let totalTables = 0;
                    let totalKeysapces = Object.keys(parsedData).length 
                    Object.values(parsedData).forEach(keyspace => {
                        totalTables += Object.keys(keyspace).length;
                    });
                    
                    console.log(`Successfully parsed tablestats for ${datacenter}:`, parsedData);
                    console.log(`Total system and user tables ${totalTables} and keyspaces ${totalKeysapces}` );
                    
                    // Store the parsed data
                    setTablestatsData(prev => ({
                        ...prev,
                        [datacenter]: parsedData
                    }));
                    
                    // Set validation success
                    setTablestatsValidation(prev => ({
                        ...prev,
                        [datacenter]: {
                            success: true,
                            message: `Successfully parsed ${totalTables} user definedtables from ${Object.keys(parsedData).length} keyspaces`
                        }
                    }));
                }
            } catch (error) {
                console.error(`Error parsing tablestats file for ${datacenter}:`, error);
                
                // Set validation error
                setTablestatsValidation(prev => ({
                    ...prev,
                    [datacenter]: {
                        success: false,
                        message: `Error parsing file: ${error.message}`
                    }
                }));
            }
        }

        // Validate info file specifically
        if (fileType === 'info' && file) {
            try {
                const content = await file.text();
                const parsedData = parseNodetoolInfo(content);
                console.log('Parsed info data:', parsedData);
                
                if (!parsedData || !parsedData.uptime_seconds) {
                    console.error('No valid info data found in file for datacenter:', datacenter);
                    
                    // Set validation error
                    setInfoValidation(prev => ({
                        ...prev,
                        [datacenter]: {
                            success: false,
                            message: 'No valid info data found in file'
                        }
                    }));
                } else {
                    // Check if datacenter matches
                    const fileDatacenter = parsedData.dc;
                    const datacenterMatch = fileDatacenter === datacenter;
                    
                    console.log(`Successfully parsed info for ${datacenter}:`, parsedData);
                    console.log(`File datacenter: ${fileDatacenter}, Expected: ${datacenter}, Match: ${datacenterMatch}`);
                    
                    // Store the parsed data
                    setInfoData(prev => ({
                        ...prev,
                        [datacenter]: parsedData
                    }));
                    
                    // Set validation result
                    if (datacenterMatch) {
                        setInfoValidation(prev => ({
                            ...prev,
                            [datacenter]: {
                                success: true,
                                message: `Uptime: ${parsedData.uptime_seconds} seconds, Datacenter: ${fileDatacenter} ✓`
                            }
                        }));
                    } else {
                        setInfoValidation(prev => ({
                            ...prev,
                            [datacenter]: {
                                success: false,
                                message: `Uptime: ${parsedData.uptime_seconds} seconds, Datacenter mismatch: expected "${datacenter}" but found "${fileDatacenter}"`
                            }
                        }));
                    }
                }
            } catch (error) {
                console.error(`Error parsing info file for ${datacenter}:`, error);
                
                // Set validation error
                setInfoValidation(prev => ({
                    ...prev,
                    [datacenter]: {
                        success: false,
                        message: `Error parsing file: ${error.message}`
                    }
                }));
            }
        }

        // Validate schema file specifically
        if (fileType === 'schema' && file) {
            try {
                const content = await file.text();
                const parsedData = parse_cassandra_schema(content);
                console.log('Parsed schema data:', parsedData);
                
                if (!parsedData || Object.keys(parsedData).length === 0) {
                    console.error('No valid schema data found in file for datacenter:', datacenter);
                    
                    // Set validation error
                    setSchemaValidation(prev => ({
                        ...prev,
                        [datacenter]: {
                            success: false,
                            message: 'No valid schema data found in file'
                        }
                    }));
                } else {
                    // Count total tables across all keyspaces
                    let totalTables = 0;
                    Object.values(parsedData).forEach(keyspace => {
                        totalTables += keyspace.tables.length;
                    });
                    
                    console.log(`Successfully parsed schema for ${datacenter}:`, parsedData);
                    console.log(`Total tables found: ${totalTables}`);
                    
                    // Store the parsed data
                    setSchemaData(prev => ({
                        ...prev,
                        [datacenter]: parsedData
                    }));
                    
                    // Set validation success
                    setSchemaValidation(prev => ({
                        ...prev,
                        [datacenter]: {
                            success: true,
                            message: `Successfully parsed ${totalTables} tables from ${Object.keys(parsedData).length} keyspaces`
                        }
                    }));
                }
            } catch (error) {
                console.error(`Error parsing schema file for ${datacenter}:`, error);
                
                // Set validation error
                setSchemaValidation(prev => ({
                    ...prev,
                    [datacenter]: {
                        success: false,
                        message: `Error parsing file: ${error.message}`
                    }
                }));
            }
        }

        // Validate row size sampler file specifically
        if (fileType === 'rowSize' && file) {
            try {
                const content = await file.text();
                const parsedData = parseRowSizeInfo(content);
                console.log('Parsed row size data:', parsedData);
                
                if (!parsedData || Object.keys(parsedData).length === 0) {
                    console.error('No valid row size data found in file for datacenter:', datacenter);
                    
                    // Set validation error
                    setRowSizeValidation(prev => ({
                        ...prev,
                        [datacenter]: {
                            success: false,
                            message: 'No valid row size data found in file'
                        }
                    }));
                } else {
                    // Count total tables
                    const totalTables = Object.keys(parsedData).length;
                    
                    console.log(`Successfully parsed row size data for ${datacenter}:`, parsedData);
                    console.log(`Total tables found: ${totalTables}`);
                    
                    // Store the parsed data
                    setRowSizeData(prev => ({
                        ...prev,
                        [datacenter]: parsedData
                    }));
                    
                    // Set validation success
                    setRowSizeValidation(prev => ({
                        ...prev,
                        [datacenter]: {
                            success: true,
                            message: `Successfully parsed row size data for ${totalTables} tables`
                        }
                    }));
                }
            } catch (error) {
                console.error(`Error parsing row size file for ${datacenter}:`, error);
                
                // Set validation error
                setRowSizeValidation(prev => ({
                    ...prev,
                    [datacenter]: {
                        success: false,
                        message: `Error parsing file: ${error.message}`
                    }
                }));
            }
        }
    };

    const isDatacenterReady = (datacenterName) => {
        const region = regions[datacenterName];
        const files = datacenterFiles[datacenterName];
        
        if (!region || !files) {
            return false;
        }

        // Check if all required files are uploaded
        return files.tablestats && files.info && files.schema && files.rowSize;
    };

    const handleEstimate = (datacenterName) => {
        console.log(`Estimating for datacenter: ${datacenterName}`);
        console.log('Region:', regions[datacenterName]);
        console.log('Files:', datacenterFiles[datacenterName]);
        console.log('Parsed tablestats data:', tablestatsData[datacenterName]);
        console.log('Parsed info data:', infoData[datacenterName]);
        console.log('Parsed schema data:', schemaData[datacenterName]);
        console.log('Parsed row size data:', rowSizeData[datacenterName]);

        try {
            const nodeid = infoData[datacenterName].id 

            const samples = { [datacenterName]:  {[nodeid]:{
                tablestats_data: tablestatsData[datacenterName],
                     schema: schemaData[datacenterName],
                      info_data: infoData[datacenterName],
                       row_size_data: rowSizeData[datacenterName]}}}

            //create a map of datacenters
           const statusData = new Map(datacenters.map(dc => [dc.name, dc.nodeCount]));
            console.log(samples);
            const result = getKeyspaceCassandraAggregate(buildCassandraLocalSet(samples, statusData), datacenterName);
            console.log('Estimation result:', result);
            
            // Set success message
            setEstimateValidation(prev => ({
                ...prev,
                [datacenterName]: {
                    success: true,
                    message: `Estimate for this region ${regions[datacenterName]} provided below`
                }
            }));

            // Store the estimation result
            setEstimateResults(prev => ({
                ...prev,
                [datacenterName]: result
            }));
        } catch (error) {
            console.error(`Error during estimation for this datacenter: ${datacenterName}:`, error);
            
            // Set failure message
            setEstimateValidation(prev => ({
                ...prev,
                [datacenterName]: {
                    success: false,
                    message: `Estimation failed: ${error.message}`
                }
            }));
        }
    };

    // Example usage of the mapping functions
    const getCurrentDatacenterRegion = (datacenterName) => {
        return getDatacenterRegion(datacenterName, regions);
    };

    const getCurrentDatacenterRegionMap = () => {
        return getDatacenterRegionMap(regions);
    };

    return (
        <Container>
            <SpaceBetween size="l">
                <FormField
                    label="Nodetool Status File"
                    description="Upload the output from `nodetool status` command. This file contains information about datacenters and nodes in your Cassandra cluster."
                >
                    <FileUpload
                        onChange={handleStatusFileChange}
                        value={statusFile ? [statusFile] : []}
                        accept=".txt"
                        i18nStrings={fileUploadI18nStrings}
                        constraintText="Upload a .txt file"
                    />
                </FormField>

                {datacenters.length > 0 && (
                    <SpaceBetween size="l">
                        {datacenters.map((datacenter) => (
                            <Container key={datacenter.name}>
                                <SpaceBetween size="l">
                                    <Header variant="h2">
                                        {datacenter.name}: {datacenter.nodeCount} nodes
                                    </Header>
                                    
                                    <FormField
                                        label="AWS Region"
                                        description="Select the AWS region that corresponds to this datacenter"
                                    >
                                        <Select
                                            selectedOption={regions[datacenter.name] ? { label: regions[datacenter.name], value: regions[datacenter.name] } : null}
                                            onChange={(detail) => handleRegionChange(datacenter.name, detail)}
                                            options={regionOptions}
                                            placeholder="Choose a region"
                                        />
                                    </FormField>

                                    <SpaceBetween size="m">
                                        <FormField
                                            label="Nodetool Tablestats File"
                                            description="Upload the output from `nodetool tablestats` command for this datacenter"
                                        >
                                            <FileUpload
                                                onChange={(detail) => handleDatacenterFileChange(datacenter.name, 'tablestats', detail)}
                                                value={datacenterFiles[datacenter.name]?.tablestats ? [datacenterFiles[datacenter.name].tablestats] : []}
                                                accept=".txt"
                                                i18nStrings={fileUploadI18nStrings}
                                                constraintText="Upload a .txt file"
                                            />
                                            {tablestatsValidation[datacenter.name] && (
                                                <Alert
                                                    type={tablestatsValidation[datacenter.name].success ? "success" : "error"}
                                                    header={tablestatsValidation[datacenter.name].success ? "Validation Successful" : "Validation Failed"}
                                                >
                                                    {tablestatsValidation[datacenter.name].message}
                                                </Alert>
                                            )}
                                        </FormField>

                                        <FormField
                                            label="Nodetool Info File"
                                            description="Upload the output from `nodetool info` command for this datacenter"
                                        >
                                            <FileUpload
                                                onChange={(detail) => handleDatacenterFileChange(datacenter.name, 'info', detail)}
                                                value={datacenterFiles[datacenter.name]?.info ? [datacenterFiles[datacenter.name].info] : []}
                                                accept=".txt"
                                                i18nStrings={fileUploadI18nStrings}
                                                constraintText="Upload a .txt file"
                                            />
                                            {infoValidation[datacenter.name] && (
                                                <Alert
                                                    type={infoValidation[datacenter.name].success ? "success" : "error"}
                                                    header={infoValidation[datacenter.name].success ? "Validation Successful" : "Validation Failed"}
                                                >
                                                    {infoValidation[datacenter.name].message}
                                                </Alert>
                                            )}
                                        </FormField>

                                        <FormField
                                            label="Schema File"
                                            description="Upload the Cassandra schema file (output from `cqlsh -e 'DESCRIBE SCHEMA'`) for this datacenter"
                                        >
                                            <FileUpload
                                                onChange={(detail) => handleDatacenterFileChange(datacenter.name, 'schema', detail)}
                                                value={datacenterFiles[datacenter.name]?.schema ? [datacenterFiles[datacenter.name].schema] : []}
                                                accept=".txt,.cql,.sql"
                                                i18nStrings={fileUploadI18nStrings}
                                                constraintText="Upload a .txt, .cql, or .sql file"
                                            />
                                            {schemaValidation[datacenter.name] && (
                                                <Alert
                                                    type={schemaValidation[datacenter.name].success ? "success" : "error"}
                                                    header={schemaValidation[datacenter.name].success ? "Validation Successful" : "Validation Failed"}
                                                >
                                                    {schemaValidation[datacenter.name].message}
                                                </Alert>
                                            )}
                                        </FormField>

                                        <FormField
                                            label="Row Size Sampler File"
                                            description="Upload the row size sampler output file for this datacenter"
                                        >
                                            <FileUpload
                                                onChange={(detail) => handleDatacenterFileChange(datacenter.name, 'rowSize', detail)}
                                                value={datacenterFiles[datacenter.name]?.rowSize ? [datacenterFiles[datacenter.name].rowSize] : []}
                                                accept=".txt"
                                                i18nStrings={fileUploadI18nStrings}
                                                constraintText="Upload a .txt file"
                                            />
                                            {rowSizeValidation[datacenter.name] && (
                                                <Alert
                                                    type={rowSizeValidation[datacenter.name].success ? "success" : "error"}
                                                    header={rowSizeValidation[datacenter.name].success ? "Validation Successful" : "Validation Failed"}
                                                >
                                                    {rowSizeValidation[datacenter.name].message}
                                                </Alert>
                                            )}
                                        </FormField>

                                        <Box textAlign="center">
                                            <Button
                                                variant="primary"
                                                onClick={() => handleEstimate(datacenter.name)}
                                                disabled={!isDatacenterReady(datacenter.name)}
                                            >
                                                Estimate for {datacenter.name}
                                            </Button>
                                        </Box>
                                        
                                        {estimateValidation[datacenter.name] && (
                                            <Alert
                                                type={estimateValidation[datacenter.name].success ? "success" : "error"}
                                                header={estimateValidation[datacenter.name].success ? "Estimation Complete" : "Estimation Failed"}
                                            >
                                                {estimateValidation[datacenter.name].message}
                                            </Alert>
                                        )}
                                        
                                        {estimateResults[datacenter.name] && estimateValidation[datacenter.name]?.success && (
                                            <FormField
                                                label="Estimation Results"
                                                description="Aggregated numbers used for pricing estimates below"
                                            >
                                                <ResultsTable results={estimateResults[datacenter.name]} />
                                            </FormField>
                                        )}
                                    </SpaceBetween>
                                </SpaceBetween>
                            </Container>
                        ))}
                    </SpaceBetween>
                )}
            </SpaceBetween>
        </Container>
    );
}

export default CassandraInput;


