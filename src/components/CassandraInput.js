import React, { useEffect, useMemo } from 'react';
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
import { parseNodetoolStatus, parse_nodetool_tablestats, parseNodetoolInfo, parse_cassandra_schema, parseRowSizeInfo, buildCassandraLocalSet, getKeyspaceCassandraAggregate, SECONDS_PER_MONTH, HOURS_PER_MONTH } from './ParsingHelpers';
import { awsRegions } from '../constants/regions';
import pricingDataJson from '../data/mcs.json';

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

// Function to format currency with commas and proper rounding
const formatCurrency = (amount) => {
    
    if (amount < 0.01) {
        return `$${Math.ceil(amount * 100) / 100}`;
    }
    if (amount < 1) {
        return `$${amount.toFixed(2)}`;
    }
    return `$${Math.ceil(amount).toLocaleString()}`;
};

// Function to get region pricing data
const getRegionPricing = (regionName) => {
    if (!pricingDataJson || !pricingDataJson.regions || !pricingDataJson.regions[regionName]) {
        console.log('No pricing data available for region:', regionName);
        return null;
    }

    const regionPricing = pricingDataJson.regions[regionName];
    
    return {
        readRequestPrice: regionPricing['MCS-ReadUnits'].price,
        writeRequestPrice: regionPricing['MCS-WriteUnits'].price,
        writeRequestPricePerHour: regionPricing['Provisioned Write Units'].price,
        readRequestPricePerHour: regionPricing['Provisioned Read Units'].price,
        storagePricePerGB: regionPricing['AmazonMCS - Indexed DataStore per GB-Mo'].price,
        pitrPricePerGB: regionPricing['Point-In-Time-Restore PITR Backup Storage per GB-Mo'].price,
        ttlDeletesPrice: regionPricing['Time to Live'].price
    };
};



function CassandraInput({
    statusFile,
    datacenters,
    regions,
    datacenterFiles,
    tablestatsData,
    infoData,
    schemaData,
    rowSizeData,
    tablestatsValidation,
    infoValidation,
    schemaValidation,
    rowSizeValidation,
    estimateValidation,
    estimateResults,
    onStatusFileChange,
    onRegionChange,
    onFileChange,
    onEstimate,
    getDatacenterRegionMap,
    getDatacenterRegionMapFromDOM
}) {
    // Results Table Component
const ResultsTable = ({ results }) => {
    // Memoize the table items to prevent unnecessary re-renders
    const tableItems = useMemo(() => {
        if (!results) return [];
        return Object.entries(results).map(([keyspace, data]) => ({
            keyspace,
            keyspace_type: data.type,
            writes_per_second: Math.round(data.writes_per_second),
            reads_per_second: Math.round(data.reads_per_second),
            ttl_deletes_per_second: Math.round(data.ttl_deletes_per_second),
            avg_write_row_size_bytes: Math.round(data.avg_write_row_size_bytes),
            avg_read_row_size_bytes: Math.round(data.avg_read_row_size_bytes),
            total_live_space_gb: Math.round(data.total_live_space_gb),
            ttls_per_second: Math.round(data.ttls_per_second),
            uncompressed_single_replica_gb: Math.round(data.uncompressed_single_replica_gb),
            replication_factor: data.replication_factor
        }));
    }, [results]);

    return (
        <div style={{ 
            height: '400px', 
            width: '100%', 
            position: 'relative',
            overflow: 'hidden'
        }}>
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
                        cell: item => Math.round((item.avg_read_row_size_bytes + item.avg_write_row_size_bytes) / 2),
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
                    },
                    {
                        id: 'replication_factor',
                        header: 'replication factor',
                        cell: item => item.replication_factor,
                        align: 'right'
                    }

                ]}
                sortingDisabled
                variant="bordered"
                empty={
                    <Box textAlign="center" color="text-body-secondary" padding="xl">
                        Click "Estimate" to see sizing details
                    </Box>
                }
            
                
            />
        </div>
    );
};
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

                // Extract datacenters and their node counts from the Map
                const newDatacenters = Array.from(statusData.entries()).map(([dc, nodeCount]) => ({
                    name: dc,
                    nodeCount: nodeCount
                }));

                // Call the parent handler
                onStatusFileChange(file, newDatacenters);

                console.log('Parsed status data:', statusData);
                console.log('New datacenters:', newDatacenters);
            } catch (error) {
                console.error('Error parsing status file:', error);
            }
        }
    };

    const handleRegionChange = (datacenter, { detail }) => {
        onRegionChange(datacenter, detail.selectedOption.value);
    };

    const handleDatacenterFileChange = async (datacenter, fileType, { detail }) => {
        const file = detail.value[0];
        
        // Validate tablestats file specifically
        if (fileType === 'tablestats' && file) {
            try {
                const content = await file.text();
                const parsedData = parse_nodetool_tablestats(content);
                console.log('Parsed tablestats data:', parsedData);
                
                if (!parsedData || Object.keys(parsedData).length === 0) {
                    console.error('No valid tablestats data found in file for datacenter:', datacenter);
                    
                    // Set validation error
                    const validation = {
                        success: false,
                        message: 'No valid tablestats data found in file'
                    };
                    onFileChange(datacenter, fileType, file, null, validation);
                } else {
                    // Count total tables across all keyspaces
                    let totalTables = 0;
                    let totalKeyspaces = 0;
                    Object.values(parsedData).forEach(keyspace => {
                        if (keyspace.type !== 'system') {
                            totalKeyspaces++;
                            totalTables += Object.keys(keyspace).length;
                        }
                    });
                    
                    console.log(`Successfully parsed tablestats for ${datacenter}:`, parsedData);
                    console.log(`Total user tables ${totalTables} and keyspaces ${totalKeyspaces}` );
                    
                    // Set validation success
                    const validation = {
                        success: true,
                        message: `Successfully parsed ${totalTables} user definedtables from ${totalKeyspaces} keyspaces`
                    };
                    onFileChange(datacenter, fileType, file, parsedData, validation);
                }
            } catch (error) {
                console.error(`Error parsing tablestats file for ${datacenter}:`, error);
                
                // Set validation error
                const validation = {
                    success: false,
                    message: `Error parsing file: ${error.message}`
                };
                onFileChange(datacenter, fileType, file, null, validation);
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
                    const validation = {
                        success: false,
                        message: 'No valid info data found in file'
                    };
                    onFileChange(datacenter, fileType, file, null, validation);
                } else {
                    // Check if datacenter matches
                    const fileDatacenter = parsedData.dc;
                    const datacenterMatch = fileDatacenter === datacenter;
                    
                    console.log(`Successfully parsed info for ${datacenter}:`, parsedData);
                    console.log(`File datacenter: ${fileDatacenter}, Expected: ${datacenter}, Match: ${datacenterMatch}`);
                    
                    // Set validation result
                    let validation;
                    if (datacenterMatch) {
                        validation = {
                            success: true,
                            message: `Uptime: ${parsedData.uptime_seconds} seconds, Datacenter: ${fileDatacenter} ✓`
                        };
                    } else {
                        validation = {
                            success: false,
                            message: `Uptime: ${parsedData.uptime_seconds} seconds, Datacenter mismatch: expected "${datacenter}" but found "${fileDatacenter}"`
                        };
                    }
                    onFileChange(datacenter, fileType, file, parsedData, validation);
                }
            } catch (error) {
                console.error(`Error parsing info file for ${datacenter}:`, error);
                
                // Set validation error
                const validation = {
                    success: false,
                    message: `Error parsing file: ${error.message}`
                };
                onFileChange(datacenter, fileType, file, null, validation);
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
                    const validation = {
                        success: false,
                        message: 'No valid schema data found in file'
                    };
                    onFileChange(datacenter, fileType, file, null, validation);
                } else {
                    // Count total tables across all keyspaces
                    let totalTables = 0;
                    Object.values(parsedData).forEach(keyspace => {
                        totalTables += keyspace.tables.length;
                    });
                    
                    console.log(`Successfully parsed schema for ${datacenter}:`, parsedData);
                    console.log(`Total tables found: ${totalTables}`);
                    
                    // Set validation success
                    const validation = {
                        success: true,
                        message: `Successfully parsed ${totalTables} tables from ${Object.keys(parsedData).length} keyspaces`
                    };
                    onFileChange(datacenter, fileType, file, parsedData, validation);
                }
            } catch (error) {
                console.error(`Error parsing schema file for ${datacenter}:`, error);
                
                // Set validation error
                const validation = {
                    success: false,
                    message: `Error parsing file: ${error.message}`
                };
                onFileChange(datacenter, fileType, file, null, validation);
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
                    const validation = {
                        success: false,
                        message: 'No valid row size data found in file'
                    };
                    onFileChange(datacenter, fileType, file, null, validation);
                } else {
                    // Count total tables
                    const totalTables = Object.keys(parsedData).length;
                    
                    console.log(`Successfully parsed row size data for ${datacenter}:`, parsedData);
                    console.log(`Total tables found: ${totalTables}`);
                    
                    // Set validation success
                    const validation = {
                        success: true,
                        message: `Successfully parsed row size data for ${totalTables} tables`
                    };
                    onFileChange(datacenter, fileType, file, parsedData, validation);
                }
            } catch (error) {
                console.error(`Error parsing row size file for ${datacenter}:`, error);
                
                // Set validation error
                const validation = {
                    success: false,
                    message: `Error parsing file: ${error.message}`
                };
                onFileChange(datacenter, fileType, file, null, validation);
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
            
            // Call the parent handler
            const validation = {
                success: true,
                message: `Estimate for this region ${regions[datacenterName]} provided below`
            };
            onEstimate(datacenterName, result, validation);
        } catch (error) {
            console.error(`Error during estimation for this datacenter: ${datacenterName}:`, error);
            
            // Set failure message
            const validation = {
                success: false,
                message: `Estimation failed: ${error.message}`
            };
            onEstimate(datacenterName, null, validation);
        }
    }; 

    // Example usage of the mapping functions
    const getCurrentDatacenterRegion = (datacenterName) => {
        return getDatacenterRegion(datacenterName, regions);
    };

    const getCurrentDatacenterRegionMap = () => {
        return getDatacenterRegionMap(regions);
    };

    // Check if all datacenters have estimation results
    const allDatacentersHaveResults = () => {
        return datacenters.length > 0 && datacenters.every(dc => 
            estimateResults[dc.name] && estimateValidation[dc.name]?.success
        );
    };

    // Calculate pricing estimate
    const calculatePricingEstimate = () => {
        if (!allDatacentersHaveResults()) return null;

        const pricingData = {};
        let total_monthly_provisioned_cost = 0;
        let total_monthly_on_demand_cost = 0;

        datacenters.forEach(dc => {
            const region = regions[dc.name];
            const results = estimateResults[dc.name];
            
            if (results && region) {
                // Get pricing data for this region
                const regionPricing = getRegionPricing(region);
                
                if (!regionPricing) {
                    console.warn(`No pricing data available for region: ${region}`);
                    return;
                }

                // Calculate costs for this datacenter
                let total_datacenter_provisioned_cost = 0;
                let total_datacenter_on_demand_cost = 0;
                const keyspaceCosts = {};
                keyspaceCosts['totals'] = {
                    name: 'region total',
                    storage: 0,
                    backup: 0,
                    reads_provisioned: 0,
                    writes_provisioned: 0,
                    reads_on_demand: 0,
                    writes_on_demand: 0,
                    ttlDeletes: 0,
                    provisioned_total: 0,
                    on_demand_total: 0
                };

                Object.entries(results).forEach(([keyspace, data]) => {

                    console.log('Data:', data);

                    const avg_write_row_size_bytes = data.avg_write_row_size_bytes;
                    const avg_read_row_size_bytes = data.avg_read_row_size_bytes;
                    
                    const write_units_per_operation = Math.ceil(avg_write_row_size_bytes / 1024);
                   
                    const ttl_units_per_operation = Math.ceil(avg_write_row_size_bytes / 1024);

                    const read_units_per_operation = Math.ceil(avg_read_row_size_bytes / 4096);
                   
                    // Calculate monthly costs using real AWS pricing
                    const storageCost = data.uncompressed_single_replica_gb * regionPricing.storagePricePerGB;
                    
                    const backupCost = data.uncompressed_single_replica_gb * regionPricing.pitrPricePerGB;
                    
                    // Convert per-second rates to monthly (seconds in a month)
                    const ondemandReadPrice = data.reads_per_second * read_units_per_operation * SECONDS_PER_MONTH * regionPricing.readRequestPrice
                    
                    const ondemandWritePrice = data.writes_per_second * write_units_per_operation * SECONDS_PER_MONTH * regionPricing.writeRequestPrice
                    
                    const monthlyTtlDeletes = data.ttls_per_second * ttl_units_per_operation * SECONDS_PER_MONTH;
                    
                    // Calculate read/write costs (pricing is per unit, not per million)
                    const provisionReadCost = data.reads_per_second * read_units_per_operation * HOURS_PER_MONTH * regionPricing.readRequestPricePerHour/.70;
                    
                    const provisionWriteCost = data.writes_per_second * write_units_per_operation * HOURS_PER_MONTH * regionPricing.writeRequestPricePerHour/.70;
                    
                    // Calculate TTL delete costs
                    const ttlDeleteCost = monthlyTtlDeletes * regionPricing.ttlDeletesPrice;

                    const provisioned_total = storageCost + backupCost + provisionReadCost + provisionWriteCost + ttlDeleteCost;
                    
                    const on_demand_total = storageCost + backupCost + ondemandReadPrice + ondemandWritePrice + ttlDeleteCost;
                    
                    keyspaceCosts[keyspace] = {
                        name: keyspace,
                        storage: storageCost,
                        backup: backupCost,
                        reads_provisioned: provisionReadCost,
                        writes_provisioned: provisionWriteCost,
                        reads_on_demand: ondemandReadPrice,
                        writes_on_demand: ondemandWritePrice,
                        ttlDeletes: ttlDeleteCost,
                        provisioned_total: provisioned_total,
                        on_demand_total: on_demand_total
                    };
                    keyspaceCosts['totals'].storage+= storageCost;
                    keyspaceCosts['totals'].backup+= backupCost;
                    keyspaceCosts['totals'].reads_provisioned += provisionReadCost;
                    keyspaceCosts['totals'].writes_provisioned += provisionWriteCost;
                    keyspaceCosts['totals'].reads_on_demand += ondemandReadPrice;
                    keyspaceCosts['totals'].writes_on_demand += ondemandWritePrice;
                    keyspaceCosts['totals'].ttlDeletes += ttlDeleteCost;
                    keyspaceCosts['totals'].provisioned_total += provisioned_total;
                    keyspaceCosts['totals'].on_demand_total += on_demand_total;

                    total_datacenter_provisioned_cost += provisioned_total;
                    total_datacenter_on_demand_cost += on_demand_total;
                });

                const totals = keyspaceCosts['totals'];
                delete keyspaceCosts['totals'];
                keyspaceCosts['totals'] = totals;

                pricingData[dc.name] = {
                    region,
                    keyspaceCosts,
                    total_datacenter_provisioned_cost: total_datacenter_provisioned_cost,
                    total_datacenter_on_demand_cost: total_datacenter_on_demand_cost
                };
                
                total_monthly_provisioned_cost += total_datacenter_provisioned_cost;
                total_monthly_on_demand_cost += total_datacenter_on_demand_cost
            }
        });

        console.log('Pricing data:', pricingData);
        console.log('Total monthly provisioned cost:', total_monthly_provisioned_cost);
        console.log('Total monthly on demand cost:', total_monthly_on_demand_cost);

        return {
            total_datacenter_cost: pricingData,
            total_monthly_provisioned_cost: total_monthly_provisioned_cost,
            total_monthly_on_demand_cost: total_monthly_on_demand_cost
        };
    };
    
    // Pricing Estimate Component
    const PricingEstimate = () => {
        const pricing = calculatePricingEstimate();
        if (!pricing) return null;

        return (
            <Container>
                <SpaceBetween size="l">
                    <Header variant="h2">Pricing Estimate</Header>
                    
                    {Object.entries(pricing.total_datacenter_cost).map(([datacenter, data]) => (
                        <Container key={datacenter}>
                            <SpaceBetween size="m">
                                <Header variant="h3">
                                    {datacenter} ({data.region}) - {formatCurrency(data.total_datacenter_provisioned_cost)}/month
                                </Header>
                                
                                <Table
                                    items={Object.entries(data.keyspaceCosts).map(([keyspace, costs]) => ({
                                        keyspace,
                                        name: costs.name,
                                        storage: formatCurrency(costs.storage),
                                        backup: formatCurrency(costs.backup),
                                        reads_provisioned: formatCurrency(costs.reads_provisioned),
                                        writes_provisioned: formatCurrency(costs.writes_provisioned),
                                        reads_on_demand: formatCurrency(costs.reads_on_demand),
                                        writes_on_demand: formatCurrency(costs.writes_on_demand),
                                        ttlDeletes: formatCurrency(costs.ttlDeletes),
                                        provisioned_total: formatCurrency(costs.provisioned_total)
                                    }))}eiifcbfhgnkrghifncntkujthgtjuvurebuhhnvkbicl

                                    columnDefinitions={[
                                        {
                                            id: 'name',
                                            header: 'Keyspace',
                                            cell: item => item.name
                                        },
                                        {
                                            id: 'storage',
                                            header: 'Storage',
                                            cell: item => item.storage
                                        },
                                        {
                                            id: 'backup',
                                            header: 'Point In Time Recovery',
                                            cell: item => item.backup
                                        },
                                        {
                                            id: 'reads_provisioned',
                                            header: 'Provisioned Reads',
                                            cell: item => item.reads_provisioned
                                        },
                                        {
                                            id: 'writes_provisioned',
                                            header: 'Provisioned Writes',
                                            cell: item => item.writes_provisioned
                                        },
                                        {
                                            id: 'ttlDeletes',
                                            header: 'TTL Deletes',
                                            cell: item => item.ttlDeletes
                                        },
                                        {
                                            id: 'total_provisioned',
                                            header: 'Monthly Total',
                                            cell: item => item.provisioned_total
                                        }
                                    ]}
                                    sortingDisabled
                                    variant="bordered"
                                />
                            </SpaceBetween>
                        </Container>
                    ))}
                    
                    <Alert type="info">
                        <strong>Total Estimated Monthly Cost (Provisioned): {formatCurrency(pricing.total_monthly_provisioned_cost)}</strong>
                        <br />
                        <strong>Total Estimated Monthly Cost (On-Demand): {formatCurrency(pricing.total_monthly_on_demand_cost)}</strong>
                        <br />
                        Note: This estimate uses Amazon Keyspaces pricing for the selected regions. Costs are calculated based on usage patterns from your Cassandra cluster data.
                    </Alert>
                </SpaceBetween>
            </Container>
        );
    };
    class SafeRender extends React.Component {
        state = { hasError: false }
        static getDerivedStateFromError() { return { hasError: true }; }
        render() {
          if (this.state.hasError) return <Alert type="error">Error rendering table</Alert>;
          return this.props.children;
        }
    }

    // Simple error boundary for ResizeObserver issues
    class TableErrorBoundary extends React.Component {
        constructor(props) {
            super(props);
            this.state = { hasError: false };
        }

        static getDerivedStateFromError(error) {
            // Only catch ResizeObserver errors
            if (error && error.message && error.message.includes('ResizeObserver')) {
                return { hasError: true };
            }
            return null;
        }

        componentDidCatch(error, errorInfo) {
            // Suppress ResizeObserver errors
            if (error && error.message && error.message.includes('ResizeObserver')) {
                console.warn('ResizeObserver error suppressed');
                return;
            }
            console.error('Table error:', error, errorInfo);
        }

        render() {
            if (this.state.hasError) {
                return (
                    <div style={{ height: '400px', width: '100%', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                        <Box textAlign="center" color="text-body-secondary">
                            Table temporarily unavailable
                        </Box>
                    </div>
                );
            }
            return this.props.children;
        }
    }

    return (
        <Container key="cassandra-input">
            <SpaceBetween size="l">
                <FormField
                    label="Nodetool Status File"
                    description="Upload the output from `nodetool status` command. This file contains the number ofdatacenters and nodes in your Apache Cassandra cluster."
                >
                    <FileUpload
                        onChange={handleStatusFileChange}
                        value={statusFile ? [statusFile] : []}
                        
                        i18nStrings={fileUploadI18nStrings}
                        constraintText="Upload a file containing output of nodetool stauts"
                    />
                </FormField>

                {datacenters.length > 0 && (
                    <SpaceBetween size="l">
                        {datacenters.map((datacenter) => (
                            <Container key={datacenter.name} data-datacenter={datacenter.name}>
                                <SpaceBetween size="l">
                                    <Header variant="h2">
                                        Cassandra Datacenter: {datacenter.name} ({datacenter.nodeCount} nodes) → AWS Region: {regions[datacenter.name] || 'Not selected'}
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
                                            data-region-select="true"
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
                                               
                                                i18nStrings={fileUploadI18nStrings}
                                                constraintText="Upload a file containing output of nodetool tablestats"
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
                                                i18nStrings={fileUploadI18nStrings}
                                                constraintText="Upload a file containing output of nodetool info"
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
                                            description="Upload the Cassandra schema file. (output from cqlsh -e 'DESCRIBE SCHEMA')"
                                        >
                                            <FileUpload
                                                onChange={(detail) => handleDatacenterFileChange(datacenter.name, 'schema', detail)}
                                                value={datacenterFiles[datacenter.name]?.schema ? [datacenterFiles[datacenter.name].schema] : []}
                                                i18nStrings={fileUploadI18nStrings}
                                                constraintText="Upload a file containing output of describe schema"
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
                                            description={<p>Upload the row size sampler output file for this datacenter. The script is available here. <a href='https://raw.githubusercontent.com/aws-samples/sample-pricing-calculator-for-keyspaces/refs/heads/main/row-size-sampler.sh' target='_blank' rel='noopener noreferrer'>row-size-sampler.sh</a></p>}
                                        >
                                            <FileUpload
                                                onChange={(detail) => handleDatacenterFileChange(datacenter.name, 'rowSize', detail)}
                                                value={datacenterFiles[datacenter.name]?.rowSize ? [datacenterFiles[datacenter.name].rowSize] : []}
                                                i18nStrings={fileUploadI18nStrings}
                                                constraintText="Upload a file containing output of row size sampler script"
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
                                        
                                        <FormField
                                            label="Cassandra sizing details"
                                            description="The aggregated numbers will be used for pricing estimates"
                                        >
                                            
                                            <TableErrorBoundary>
                                                <ResultsTable results={estimateResults[datacenter.name]} scrollbar={true} />
                                            </TableErrorBoundary>
                                            
                                        </FormField>
                                        
                                    </SpaceBetween>
                                </SpaceBetween>
                            </Container>
                            
                        ))}
                        <PricingEstimate />
                        <Box>
                                                                <strong>Assumptions:</strong>
                                                                <ul style={{ marginTop: '8px', marginBottom: '16px' }}>
                                                                    <li>Provisioned estimate includes 70% target utilization for the Application Auto Scaling policy</li>
                                                                </ul>
                                                            </Box>
                    </SpaceBetween>
                )}
            </SpaceBetween>
           
        </Container>
    );
}

export default CassandraInput;


