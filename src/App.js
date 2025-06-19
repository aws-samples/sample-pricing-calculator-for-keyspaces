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
    SpaceBetween
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
    const [activeInputMethod, setActiveInputMethod] = useState('standard');
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
                            onChange={({ detail }) => setActiveInputMethod(detail.activeTabId)}
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

                                                <Tabs
                                                    activeTabId={activeTab}
                                                    onChange={({ detail }) => setActiveTab(detail.activeTabId)}
                                                    tabs={[
                                                        {
                                                            label: "Calculator",
                                                            id: "calculator",
                                                            content: (
                                                                <Box padding={{ top: "l" }}>
                                                                    {provisionedPricing && Object.keys(provisionedPricing).length > 0 && (
                                                                        <PricingTable 
                                                                            provisionedPricing={provisionedPricing}
                                                                            onDemandPricing={onDemandPricing}
                                                                            formData={formData}
                                                                            selectedRegion={selectedRegion}
                                                                            multiSelectedRegions={multiSelectedRegions}
                                                                        />
                                                                    )}
                                                                </Box>
                                                            )
                                                        },
                                                        {
                                                            label: "TCO",
                                                            id: "tco",
                                                            content: (
                                                                <Box padding={{ top: "l" }}>
                                                                    {/* TCO content will be added here later */}
                                                                </Box>
                                                            )
                                                        }
                                                    ]}
                                                />
                                            </SpaceBetween>
                                        </Box>
                                    )
                                },
                                {
                                    label: "Cassandra Input",
                                    id: "advanced",
                                    content: (
                                        <Box padding={{ top: "l" }}>
                                            <CassandraInput />
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