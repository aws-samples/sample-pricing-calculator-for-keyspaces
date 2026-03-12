import React, { useState } from 'react';
import { Table, SpaceBetween, Box, Link } from '@cloudscape-design/components';
import { formatPrice } from '../utils/formatters';

function PricingTable({ provisionedPricing, onDemandPricing, formData, selectedRegion, multiSelectedRegions, onGenerateReport }) {
  const [isTransitioning, setIsTransitioning] = useState(false);

  const calculateTotal = (pricing, consistency) => {
    if (!pricing || Object.keys(pricing).length === 0) {
      return 0;
    }
    
    const isStrong = consistency === 'strong';
    return (
      (isStrong ? pricing.strongConsistencyReads : pricing.eventualConsistencyReads) +
      (isStrong ? pricing.strongConsistencyWrites : pricing.eventualConsistencyWrites) +
      (isStrong ? pricing.strongConsistencyStorage : pricing.eventualConsistencyStorage) +
      (formData[selectedRegion]?.pointInTimeRecoveryForBackups ? 
        (isStrong ? pricing.strongConsistencyBackup : pricing.eventualConsistencyBackup) : 0) +
      (isStrong ? pricing.strongConsistencyTtlDeletesPrice : pricing.eventualConsistencyTtlDeletesPrice)
    );
  };
  const calculateSavingsTotal = (pricing, consistency) => {
    if (!pricing || Object.keys(pricing).length === 0) {
      return 0;
    }
    
    const isStrong = consistency === 'strong';
    return (
      (isStrong ? pricing.strongConsistencyReadsSavings : pricing.eventualConsistencyReadsSavings) +
      (isStrong ? pricing.strongConsistencyWritesSavings : pricing.eventualConsistencyWritesSavings) +
      (isStrong ? pricing.strongConsistencyStorage : pricing.eventualConsistencyStorage) +
      (formData[selectedRegion]?.pointInTimeRecoveryForBackups ? 
        (isStrong ? pricing.strongConsistencyBackup : pricing.eventualConsistencyBackup) : 0) +
      (isStrong ? pricing.strongConsistencyTtlDeletesPrice : pricing.eventualConsistencyTtlDeletesPrice)
    );
  };

  const baseColumns = [
    { 
      id: "metric",
      header: "Metric", 
      cell: item => item.metric
    },
    { 
      id: "provisionedEventual",
      header: "Provisioned (Eventual)", 
      cell: item => <Box textAlign="right">{formatPrice(item.provisionedEventual)}</Box>
    },
    { 
      id: "onDemandEventual",
      header: "On-Demand (Eventual)", 
      cell: item => <Box textAlign="right">{formatPrice(item.onDemandEventual)}</Box>
    },
  ];

  const allColumns = [
    { 
      id: "metric",
      header: "Metric", 
      cell: item => item.metric
    },
    { 
      id: "provisionedStrong",
      header: "Provisioned (Strong)", 
      cell: item => <Box textAlign="right">{formatPrice(item.provisionedStrong)}</Box>
    },
    { 
      id: "onDemandStrong",
      header: "On-Demand (Strong)", 
      cell: item => <Box textAlign="right">{formatPrice(item.onDemandStrong)}</Box>
    },
    { 
      id: "provisionedEventual",
      header: "Provisioned (Eventual)", 
      cell: item => <Box textAlign="right">{formatPrice(item.provisionedEventual)}</Box>
    },
    { 
      id: "onDemandEventual",
      header: "On-Demand (Eventual)", 
      cell: item => <Box textAlign="right">{formatPrice(item.onDemandEventual)}</Box>
    },
  ];

  

  const tableItems = [
    { 
      metric: "Read Request Price", 
      provisionedStrong: provisionedPricing?.strongConsistencyReads || 0,
      provisionedEventual: provisionedPricing?.eventualConsistencyReads || 0,
      onDemandStrong: onDemandPricing?.strongConsistencyReads || 0,
      onDemandEventual: onDemandPricing?.eventualConsistencyReads || 0
    },
    { 
      metric: "Write Request Price", 
      provisionedStrong: provisionedPricing?.strongConsistencyWrites || 0,
      provisionedEventual: provisionedPricing?.eventualConsistencyWrites || 0,
      onDemandStrong: onDemandPricing?.strongConsistencyWrites || 0,
      onDemandEventual: onDemandPricing?.eventualConsistencyWrites || 0
    },
    { 
      metric: "Storage Price", 
      provisionedStrong: provisionedPricing?.strongConsistencyStorage || 0,
      provisionedEventual: provisionedPricing?.eventualConsistencyStorage || 0,
      onDemandStrong: onDemandPricing?.strongConsistencyStorage || 0,
      onDemandEventual: onDemandPricing?.eventualConsistencyStorage || 0
    },
    formData[selectedRegion]?.pointInTimeRecoveryForBackups && { 
      metric: "Backup Price", 
      provisionedStrong: provisionedPricing?.strongConsistencyBackup || 0,
      provisionedEventual: provisionedPricing?.eventualConsistencyBackup || 0,
      onDemandStrong: onDemandPricing?.strongConsistencyBackup || 0,
      onDemandEventual: onDemandPricing?.eventualConsistencyBackup || 0
    },
    { 
      metric: "TTL Deletes Price", 
      provisionedStrong: provisionedPricing?.strongConsistencyTtlDeletesPrice || 0,
      provisionedEventual: provisionedPricing?.eventualConsistencyTtlDeletesPrice || 0,
      onDemandStrong: onDemandPricing?.strongConsistencyTtlDeletesPrice || 0,
      onDemandEventual: onDemandPricing?.eventualConsistencyTtlDeletesPrice || 0
    },
    { 
      metric: "Monthly total", 
      provisionedStrong: calculateTotal(provisionedPricing || {}, 'strong'),
      provisionedEventual: calculateTotal(provisionedPricing || {}, 'eventual'),
      onDemandStrong: calculateTotal(onDemandPricing || {}, 'strong'),
      onDemandEventual: calculateTotal(onDemandPricing || {}, 'eventual')
    },
     { metric: "Monthly total (Database Svaings Plan)", 
      provisionedStrong: calculateSavingsTotal(provisionedPricing || {}, 'strong'),
      provisionedEventual: calculateSavingsTotal(provisionedPricing || {}, 'eventual'),
      onDemandStrong: calculateSavingsTotal(onDemandPricing || {}, 'strong'),
      onDemandEventual: calculateSavingsTotal(onDemandPricing || {}, 'eventual')
     }
  ].filter(Boolean);

  return (
    <SpaceBetween size="m">
      <Box float="right">
        <Link onFollow={() => onGenerateReport?.()} href="#">
          Generate Report
        </Link>
      </Box>
      
      <Table
        columnDefinitions={allColumns}
        items={tableItems}
        variant="embedded"
        stickyHeader={false}
        resizableColumns={false}
        wrapLines={false}
        stripedRows={false}
        contentDensity="compact"
        enableKeyboardNavigation={false}
        trackBy="metric"
        empty={
          <Box textAlign="center" color="text-body-secondary" padding="xl">
            Enter values above to see pricing estimates
          </Box>
        }
      />
    </SpaceBetween>
  );
}

export default PricingTable;
