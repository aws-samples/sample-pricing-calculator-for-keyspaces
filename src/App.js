import React, { useState, useEffect } from 'react';
import PricingTable from './components/PricingTable';
import Navigation from './components/Navigation';
import MultiRegionForm from './components/MultiRegionForm';
import KeyspacesHelpPanel from './components/KeyspacesHelpPanel';
import CassandraInput from './components/CassandraInput';
import pricingDataJson from './data/mcs.json';  // Import the JSON directly
import './App.css';

import {
    AppLayout,
    Container,
    Tabs,
    Box,
    SpaceBetween,
    Header
} from '@cloudscape-design/components';
import '@cloudscape-design/global-styles/index.css';

function App() {
    const [currentPricing, setCurrentPricing] = useState({});
    const [provisionedPricing, setProvisionedPricing] = useState({});
    const [onDemandPricing, setOnDemandPricing] = useState({});
    const [selectedRegion, setSelectedRegion] = useState('US East (N. Virginia)');
    const [multiSelectedRegions, setMultiSelectedRegions] = useState([]);
    const [expandedRegions, setExpandedRegions] = useState({});
    const [activeTab, setActiveTab] = useState('calculator');
    const [activeInputMethod, setActiveInputMethod] = useState(() => {
        // Check URL hash on initial load
        const hash = window.location.hash.replace('#', '');
        if (hash === 'cassandra') {
            return 'advanced';
        }
        return 'standard';
    });
    const [formData, setFormData] = useState({
        [selectedRegion]: {
            averageRowSizeInBytes: 1024,
            averageReadRequestsPerSecond: 0,
            averageWriteRequestsPerSecond: 0,
            averageTtlDeletesPerSecond: 0,
            storageSizeInGb: 0,
            pointInTimeRecoveryForBackups: false
        }
    });

    // Cassandra-related state
    const [cassandraStatusFile, setCassandraStatusFile] = useState(null);
    const [cassandraDatacenters, setCassandraDatacenters] = useState([]);
    const [cassandraRegions, setCassandraRegions] = useState({});
    const [cassandraDatacenterFiles, setCassandraDatacenterFiles] = useState({});
    const [cassandraTablestatsData, setCassandraTablestatsData] = useState({});
    const [cassandraInfoData, setCassandraInfoData] = useState({});
    const [cassandraSchemaData, setCassandraSchemaData] = useState({});
    const [cassandraRowSizeData, setCassandraRowSizeData] = useState({});
    const [cassandraTcoData, setCassandraTcoData] = useState({});
    const [cassandraTablestatsValidation, setCassandraTablestatsValidation] = useState({});
    const [cassandraInfoValidation, setCassandraInfoValidation] = useState({});
    const [cassandraSchemaValidation, setCassandraSchemaValidation] = useState({});
    const [cassandraRowSizeValidation, setCassandraRowSizeValidation] = useState({});
    const [cassandraTcoValidation, setCassandraTcoValidation] = useState({});
    const [cassandraEstimateValidation, setCassandraEstimateValidation] = useState({});
    const [cassandraEstimateResults, setCassandraEstimateResults] = useState({});

    // Cassandra handler functions
    const handleCassandraStatusFileChange = (file, datacenters) => {
        setCassandraStatusFile(file);
        setCassandraDatacenters(datacenters);
        
        // Initialize state for each datacenter
        const initialRegions = {};
        const initialFiles = {};
        const initialTablestats = {};
        const initialInfo = {};
        const initialSchema = {};
        const initialRowSize = {};
        const initialTco = {};
        const initialTablestatsValidation = {};
        const initialInfoValidation = {};
        const initialSchemaValidation = {};
        const initialRowSizeValidation = {};
        const initialTcoValidation = {};
        const initialEstimateValidation = {};
        const initialEstimateResults = {};
        
        datacenters.forEach(dc => {
            initialRegions[dc.name] = 'US East (N. Virginia)'; // Set default region
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
            initialTcoValidation[dc.name] = null;
            initialEstimateValidation[dc.name] = null;
            initialEstimateResults[dc.name] = null;
        });
        
        setCassandraRegions(initialRegions);
        setCassandraDatacenterFiles(initialFiles);
        setCassandraTablestatsData(initialTablestats);
        setCassandraInfoData(initialInfo);
        setCassandraSchemaData(initialSchema);
        setCassandraRowSizeData(initialRowSize);
        setCassandraTcoData(initialTco);
        setCassandraTablestatsValidation(initialTablestatsValidation);
        setCassandraInfoValidation(initialInfoValidation);
        setCassandraSchemaValidation(initialSchemaValidation);
        setCassandraRowSizeValidation(initialRowSizeValidation);
        setCassandraTcoValidation(initialTcoValidation);
        setCassandraEstimateValidation(initialEstimateValidation);
        setCassandraEstimateResults(initialEstimateResults);
    };

    const handleCassandraRegionChange = (datacenter, region) => {
        setCassandraRegions(prev => ({
            ...prev,
            [datacenter]: region
        }));
    };

    const handleCassandraFileChange = (datacenter, fileType, file, parsedData, validation) => {
        // Update file state
        setCassandraDatacenterFiles(prev => ({
            ...prev,
            [datacenter]: {
                ...prev[datacenter],
                [fileType]: file
            }
        }));

        // Update parsed data state
        if (parsedData) {
            switch (fileType) {
                case 'tablestats':
                    setCassandraTablestatsData(prev => ({
                        ...prev,
                        [datacenter]: parsedData
                    }));
                    break;
                case 'info':
                    setCassandraInfoData(prev => ({
                        ...prev,
                        [datacenter]: parsedData
                    }));
                    break;
                case 'schema':
                    setCassandraSchemaData(prev => ({
                        ...prev,
                        [datacenter]: parsedData
                    }));
                    break;
                case 'rowSize':
                    setCassandraRowSizeData(prev => ({
                        ...prev,
                        [datacenter]: parsedData
                    }));
                    break;
                case 'tco':
                        setCassandraTcoData(prev => ({
                            ...prev,
                            [datacenter]: parsedData
                        }));
                        break;
                default:
                    break;
            }
        }

        // Update validation state
        if (validation) {
            switch (fileType) {
                case 'tablestats':
                    setCassandraTablestatsValidation(prev => ({
                        ...prev,
                        [datacenter]: validation
                    }));
                    break;
                case 'info':
                    setCassandraInfoValidation(prev => ({
                        ...prev,
                        [datacenter]: validation
                    }));
                    break;
                case 'schema':
                    setCassandraSchemaValidation(prev => ({
                        ...prev,
                        [datacenter]: validation
                    }));
                    break;
                case 'rowSize':
                    setCassandraRowSizeValidation(prev => ({
                        ...prev,
                        [datacenter]: validation
                    }));
                    break;
                case 'tco':
                        setCassandraTcoValidation(prev => ({
                            ...prev,
                            [datacenter]: validation
                        }));
                        break;
                default:
                    break;
            }
        }
    };

    const handleCassandraEstimate = (datacenterName, result, validation) => {
        setCassandraEstimateValidation(prev => ({
            ...prev,
            [datacenterName]: validation
        }));
        setCassandraEstimateResults(prev => ({
            ...prev,
            [datacenterName]: result
        }));
    };

    // Handle tab changes and update URL hash
    const handleTabChange = ({ detail }) => {
        const newTabId = detail.activeTabId;
        setActiveInputMethod(newTabId);
        
        // Update URL hash based on tab
        if (newTabId === 'advanced') {
            window.location.hash = 'cassandra';
        } else {
            window.location.hash = '';
        }
    };

    // Listen for hash changes (for direct URL navigation)
    useEffect(() => {
        const handleHashChange = () => {
            const hash = window.location.hash.replace('#', '');
            if (hash === 'cassandra') {
                setActiveInputMethod('advanced');
            } else {
                setActiveInputMethod('standard');
            }
        };

        window.addEventListener('hashchange', handleHashChange);
        return () => window.removeEventListener('hashchange', handleHashChange);
    }, []);

    // Function to get datacenter-region mappings
    const getCassandraDatacenterRegionMap = () => {
        const mapping = {};
        Object.entries(cassandraRegions).forEach(([datacenter, region]) => {
            if (region) {
                mapping[datacenter] = region;
            }
        });
        return mapping;
    };

    // Function to get datacenter-region mappings by inspecting the DOM
    const getDatacenterRegionMapFromDOM = () => {
        const mapping = {};
        
        // Find all datacenter sections in the DOM
        const datacenterSections = document.querySelectorAll('[data-datacenter]');
        
        datacenterSections.forEach(section => {
            const datacenterName = section.getAttribute('data-datacenter');
            const regionSelect = section.querySelector('select[data-region-select]');
            
            if (regionSelect && regionSelect.value) {
                mapping[datacenterName] = regionSelect.value;
            }
        });
        
        return mapping;
    };

    useEffect(() => {
        setCurrentPricing(processRegion('US East (N. Virginia)'));
    }, []);

    useEffect(() => {
        setFormData(prevFormData => {
            const newFormData = { ...prevFormData };
            const defaultData = {
                averageRowSizeInBytes: prevFormData[selectedRegion]?.averageRowSizeInBytes || 1024,
                averageReadRequestsPerSecond: prevFormData[selectedRegion]?.averageReadRequestsPerSecond || 0,
                averageWriteRequestsPerSecond: prevFormData[selectedRegion]?.averageWriteRequestsPerSecond || 0,
                averageTtlDeletesPerSecond: prevFormData[selectedRegion]?.averageTtlDeletesPerSecond || 0,
                storageSizeInGb: prevFormData[selectedRegion]?.storageSizeInGb || 0,
                pointInTimeRecoveryForBackups: prevFormData[selectedRegion]?.pointInTimeRecoveryForBackups || false
            };
    
            // Ensure default region always has data
            if (!newFormData.default) {
                newFormData.default = { ...defaultData };
            }
    
            // Add data for each selected region
            multiSelectedRegions.forEach(region => {
                if (!newFormData[region.value]) {
                    // Copy values from primary region to new region
                    newFormData[region.value] = {
                        ...defaultData,
                        averageReadRequestsPerSecond: 0 // Reset read requests for new region
                    };
                }
            });
    
            return newFormData;
        });
    }, [multiSelectedRegions, selectedRegion]);

    // Update the pricing calculation useEffect to depend on formData
    useEffect(() => {
        if (formData && Object.keys(formData).length > 0) {
            calculatePricing(formData);
        }
    }, [selectedRegion, multiSelectedRegions, formData]);

    const processRegion = (regionCode) => {
        if (!pricingDataJson || !pricingDataJson.regions || !pricingDataJson.regions[regionCode]) {
            console.log('No pricing data available for region:', regionCode);
            return null;
        }

        const regionPricing = pricingDataJson.regions[regionCode];
        
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

    const handleSubmit = (e) => {
        e.preventDefault();
        calculatePricing(formData);
    };

    const handleKeyUp = (e) => {
        // Debounce the calculation to prevent too many updates
        if (e) {
            e.preventDefault();
        }
        calculatePricing(formData);
    };

    function getAvgProvisionedCapacityUnits(requests, size, cuMultiplier) {
        return Math.ceil((requests * Math.ceil(size * 1 / (cuMultiplier * 1024)))/.70);
    }

    function getOnDemandCUs(requests, size, cuMultiplier) {
        return Math.ceil(requests * Math.ceil(size * 1 / (cuMultiplier * 1024))) * 3600 * 24 * 30.41667;
    }

    function getStrongConsistencyUnits(avgCUs, avgHours, price) {
        return Math.ceil(avgCUs * avgHours * price * 30.41667);
    }

    var getTtlDeletesPrice = (ttlDeletesPerDay, averageRowSizeInBytes) => {
        return (ttlDeletesPerDay * Math.ceil(averageRowSizeInBytes/1024)*60*60*24*365)/12;
    }

    const calculatePricing = (formData) => {
        if (!formData || !selectedRegion || !formData[selectedRegion]) {
            return;
        }

        const isMultiRegion = multiSelectedRegions.length > 0;
        let totalStrongConsistencyReads = 0;
        let totalEventualConsistencyReads = 0;
        let totalStrongConsistencyWrites = 0;
        let totalEventualConsistencyWrites = 0;
        let totalStoragePrice = 0;
        let totalBackupPrice = 0;
        let totalTtlDeletesPrice = 0;
    
        let totalOnDemandReads = 0;
        let totalOnDemandWrites = 0;
        let totalOnDemandEventualConsistencyReads = 0;
        let totalOnDemandEventualConsistencyWrites = 0;
    
        const regions = [selectedRegion, ...multiSelectedRegions.map(r => r.value)];
        
        regions.forEach(region => {
            let regionData = formData[region];
            let regionPricing;
    
            if (!regionData) {
                return; // Skip if region data is not available
            }
    
            if (isMultiRegion) {
                if (region === 'default') {
                    region = selectedRegion;
                }  
            }
            regionPricing = processRegion(region);

            if (regionPricing) {

                const avgReadProvisionedCapacityUnits = getAvgProvisionedCapacityUnits(regionData.averageReadRequestsPerSecond, regionData.averageRowSizeInBytes, 4);
                const strongConsistencyReads = getStrongConsistencyUnits(avgReadProvisionedCapacityUnits, 24, regionPricing.readRequestPricePerHour);
    
                const avgWriteProvisionedCapacityUnits = getAvgProvisionedCapacityUnits(regionData.averageWriteRequestsPerSecond, regionData.averageRowSizeInBytes, 1);
                const strongConsistencyWrites = getStrongConsistencyUnits(avgWriteProvisionedCapacityUnits, 24, regionPricing.writeRequestPricePerHour);
    
                const storagePrice = regionData.storageSizeInGb * regionPricing.storagePricePerGB;
                const backupPrice = regionData.storageSizeInGb * regionPricing.pitrPricePerGB;
    
                const onDemandReadsPrice = getOnDemandCUs(regionData.averageReadRequestsPerSecond, regionData.averageRowSizeInBytes, 4) * regionPricing.readRequestPrice;
                const onDemandWritesPrice = getOnDemandCUs(regionData.averageWriteRequestsPerSecond, regionData.averageRowSizeInBytes, 1) * regionPricing.writeRequestPrice;
    
                const ttlDeletesPrice = getTtlDeletesPrice(regionData.averageTtlDeletesPerSecond, regionData.averageRowSizeInBytes) * regionPricing.ttlDeletesPrice;
    
                totalStrongConsistencyReads += strongConsistencyReads;
                totalEventualConsistencyReads += strongConsistencyReads / 2;
                totalStrongConsistencyWrites += strongConsistencyWrites;
                totalEventualConsistencyWrites += strongConsistencyWrites;

                totalStoragePrice += storagePrice;
                
                totalBackupPrice += backupPrice;
                totalTtlDeletesPrice += ttlDeletesPrice;
    
                totalOnDemandReads += onDemandReadsPrice;
                totalOnDemandEventualConsistencyReads += onDemandReadsPrice / 2;
                totalOnDemandWrites += onDemandWritesPrice;
                totalOnDemandEventualConsistencyWrites += onDemandWritesPrice;
            }
        });
    
        const writesMultiplier = isMultiRegion ? regions.length : 1;
    
        setProvisionedPricing({
            strongConsistencyReads: totalStrongConsistencyReads,
            strongConsistencyWrites: totalStrongConsistencyWrites ,
            eventualConsistencyReads: totalEventualConsistencyReads,
            eventualConsistencyWrites: totalEventualConsistencyWrites ,
            strongConsistencyStorage: totalStoragePrice,
            strongConsistencyBackup: totalBackupPrice,
            eventualConsistencyStorage: totalStoragePrice,
            eventualConsistencyBackup: totalBackupPrice,
            eventualConsistencyTtlDeletesPrice: totalTtlDeletesPrice,
            strongConsistencyTtlDeletesPrice: totalTtlDeletesPrice
        });
    
        setOnDemandPricing({
            strongConsistencyReads: totalOnDemandReads,
            strongConsistencyWrites: totalOnDemandEventualConsistencyWrites ,
            eventualConsistencyReads: totalOnDemandEventualConsistencyReads ,
            eventualConsistencyWrites: totalOnDemandWrites ,
            strongConsistencyStorage: totalStoragePrice,
            strongConsistencyBackup: totalBackupPrice,
            eventualConsistencyStorage: totalStoragePrice,
            eventualConsistencyBackup: totalBackupPrice,
            eventualConsistencyTtlDeletesPrice: totalTtlDeletesPrice,
            strongConsistencyTtlDeletesPrice: totalTtlDeletesPrice
        });
    };

    return (
        <AppLayout
            navigation={<Navigation />}
            tools={<KeyspacesHelpPanel />}
            content={
                <Container>
                    <SpaceBetween size="l">
                        <Tabs
                            activeTabId={activeInputMethod}
                            onChange={handleTabChange}
                            tabs={[
                                {
                                    label: "Keyspaces Input",
                                    id: "standard",
                                    content: (
                                        <Box padding={{ top: "l" }}>
                                            <SpaceBetween size="l">
                                                <MultiRegionForm
                                                    selectedRegion={selectedRegion}
                                                    setSelectedRegion={setSelectedRegion}
                                                    multiSelectedRegions={multiSelectedRegions}
                                                    setMultiSelectedRegions={setMultiSelectedRegions}
                                                    formData={formData}
                                                    setFormData={setFormData}
                                                    onSubmit={handleSubmit}
                                                    onKeyUp={handleKeyUp}
                                                    expandedRegions={expandedRegions}
                                                    setExpandedRegions={setExpandedRegions}
                                                />

                                                <Box padding={{ top: "l" }}>
                                                    <SpaceBetween size="l">
                                                        <Header variant="h2">Pricing Estimate</Header>
                                                        <PricingTable 
                                                            provisionedPricing={provisionedPricing}
                                                            onDemandPricing={onDemandPricing}
                                                            formData={formData}
                                                            selectedRegion={selectedRegion}
                                                            multiSelectedRegions={multiSelectedRegions}
                                                        />
                                                        <Box>
                                                            <strong>Assumptions:</strong>
                                                            <ul style={{ marginTop: '8px', marginBottom: '16px' }}>
                                                                <li>Provisioned estimate includes 70% target utilization for the Application Auto Scaling policy</li>
                                                            </ul>
                                                        </Box>
                                                    </SpaceBetween>
                                                </Box>
                                            </SpaceBetween>
                                        </Box>
                                    )
                                },
                                {
                                    label: "Cassandra Input",
                                    id: "advanced",
                                    content: (
                                        <Box padding={{ top: "l" }}>
                                            <CassandraInput
                                                statusFile={cassandraStatusFile}
                                                datacenters={cassandraDatacenters}
                                                regions={cassandraRegions}
                                                datacenterFiles={cassandraDatacenterFiles}
                                                tablestatsData={cassandraTablestatsData}
                                                infoData={cassandraInfoData}
                                                schemaData={cassandraSchemaData}
                                                rowSizeData={cassandraRowSizeData}
                                                tcoData={cassandraTcoData}
                                                tablestatsValidation={cassandraTablestatsValidation}
                                                infoValidation={cassandraInfoValidation}
                                                schemaValidation={cassandraSchemaValidation}
                                                rowSizeValidation={cassandraRowSizeValidation}
                                                tcoValidation={cassandraTcoValidation}
                                                estimateValidation={cassandraEstimateValidation}
                                                estimateResults={cassandraEstimateResults}
                                                onStatusFileChange={handleCassandraStatusFileChange}
                                                onRegionChange={handleCassandraRegionChange}
                                                onFileChange={handleCassandraFileChange}
                                                onEstimate={handleCassandraEstimate}
                                                getDatacenterRegionMap={getCassandraDatacenterRegionMap}
                                                getDatacenterRegionMapFromDOM={getDatacenterRegionMapFromDOM}
                                            />
                                        </Box>
                                    )
                                }
                            ]}
                        />
                    </SpaceBetween>
                </Container>
            }
        />
    );
}

export default App;