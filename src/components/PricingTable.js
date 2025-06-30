import React, { useState, useCallback } from 'react';
import { Table, Button, SpaceBetween, Box, Link } from '@cloudscape-design/components';
import { formatPrice } from '../utils/formatters';
import jsPDF from 'jspdf';
import 'jspdf-autotable';

function PricingTable({ provisionedPricing, onDemandPricing, formData, selectedRegion, multiSelectedRegions }) {
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
  ].filter(Boolean);

  const exportToPDF = () => {
    const doc = new jsPDF();
    
    // Add title
    doc.setFontSize(16);
    doc.text('Amazon Keyspaces (for Apache Cassandra) pricing calculator results', 14, 20);
    
    // Add timestamp and region information
    const timestamp = new Date().toLocaleString();
    doc.setFontSize(10);
    doc.text(`Generated on: ${timestamp}`, 14, 30);
    doc.text(`Primary region: ${selectedRegion}`, 14, 35);
    
    if (multiSelectedRegions && multiSelectedRegions.length > 0) {
      doc.text('Additional regions:', 14, 40);
      multiSelectedRegions.forEach((region, index) => {
        doc.text(`- ${region.value}`, 20, 45 + (index * 5));
      });
    }

    // Prepare table data with grouped pricing information
    const tableData = [
      ['Metric', 'Provisioned (Strong)', 'Provisioned (Eventual)', 'On-Demand (Strong)', 'On-Demand (Eventual)'],
      ['Read Request Units', 
        formatPrice(provisionedPricing?.strongConsistencyReads || 0),
        formatPrice(provisionedPricing?.eventualConsistencyReads || 0),
        formatPrice(onDemandPricing?.strongConsistencyReads || 0),
        formatPrice(onDemandPricing?.eventualConsistencyReads || 0)
      ],
      ['Write Request Units',
        formatPrice(provisionedPricing?.strongConsistencyWrites || 0),
        formatPrice(provisionedPricing?.eventualConsistencyWrites || 0),
        formatPrice(onDemandPricing?.strongConsistencyWrites || 0),
        formatPrice(onDemandPricing?.eventualConsistencyWrites || 0)
      ],
      ['Storage Price',
        formatPrice(provisionedPricing?.strongConsistencyStorage || 0),
        formatPrice(provisionedPricing?.eventualConsistencyStorage || 0),
        formatPrice(onDemandPricing?.strongConsistencyStorage || 0),
        formatPrice(onDemandPricing?.eventualConsistencyStorage || 0)
      ]
    ];

    // Add backup price row if PITR is enabled
    if (formData[selectedRegion]?.pointInTimeRecoveryForBackups) {
      tableData.push([
        'Point-in-time recovery Price',
        formatPrice(provisionedPricing?.strongConsistencyBackup || 0),
        formatPrice(provisionedPricing?.eventualConsistencyBackup || 0),
        formatPrice(onDemandPricing?.strongConsistencyBackup || 0),
        formatPrice(onDemandPricing?.eventualConsistencyBackup || 0)
      ]);
    }

    // Add TTL Deletes Price row
    tableData.push([
      'TTL Deletes Price',
      formatPrice(provisionedPricing?.strongConsistencyTtlDeletesPrice || 0),
      formatPrice(provisionedPricing?.eventualConsistencyTtlDeletesPrice || 0),
      formatPrice(onDemandPricing?.strongConsistencyTtlDeletesPrice || 0),
      formatPrice(onDemandPricing?.eventualConsistencyTtlDeletesPrice || 0)
    ]);

    // Add Total row
    tableData.push([
      'Total',
      formatPrice(calculateTotal(provisionedPricing || {}, 'strong')),
      formatPrice(calculateTotal(provisionedPricing || {}, 'eventual')),
      formatPrice(calculateTotal(onDemandPricing || {}, 'strong')),
      formatPrice(calculateTotal(onDemandPricing || {}, 'eventual'))
    ]);

    // Calculate starting Y position based on number of regions
    const startY = multiSelectedRegions.length > 0 ? 50 + (multiSelectedRegions.length * 5) : 45;

    // Generate the table
    doc.autoTable({
      head: [tableData[0]],
      body: tableData.slice(1),
      startY: startY,
      theme: 'grid',
      styles: {
        fontSize: 10,
        cellPadding: 5,
        lineColor: [238, 238, 238],
        lineWidth: 0.1,
      },
      headStyles: {
        fillColor: [35, 47, 62],
        textColor: [255, 255, 255],
        fontStyle: 'bold'
      },
      alternateRowStyles: {
        fillColor: [249, 249, 249]
      },
      columnStyles: {
        0: { cellWidth: 60 },
        1: { cellWidth: 30 },
        2: { cellWidth: 30 },
        3: { cellWidth: 30 },
        4: { cellWidth: 30 }
      }
    });

    // Add Input Parameters section after the pricing table
    const inputStartY = doc.lastAutoTable.finalY + 20;
    doc.setFontSize(14);
    doc.text('Input Parameters', 14, inputStartY);

    // Primary Region Input Data
    const inputData = [
      ['Parameter', 'Primary Region Value']
    ];

    // Add primary region data
    inputData.push(
      ['Average read requests per second', formData[selectedRegion].averageReadRequestsPerSecond],
      ['Average write requests per second', formData[selectedRegion].averageWriteRequestsPerSecond],
      ['Average row size (bytes)', formData[selectedRegion].averageRowSizeInBytes],
      ['Storage (GB)', formData[selectedRegion].storageSizeInGb],
      ['Point-in-Time Recovery (PITR)', formData[selectedRegion].pointInTimeRecoveryForBackups ? 'Enabled' : 'Disabled'],
      ['TTL Deletes per second', formData[selectedRegion].averageTtlDeletesPerSecond]
    );

    // Generate input parameters table for primary region
    doc.autoTable({
      head: [inputData[0]],
      body: inputData.slice(1),
      startY: inputStartY + 10,
      theme: 'grid',
      styles: {
        fontSize: 10,
        cellPadding: 5,
        lineColor: [238, 238, 238],
        lineWidth: 0.1,
      },
      headStyles: {
        fillColor: [35, 47, 62],
        textColor: [255, 255, 255],
        fontStyle: 'bold'
      },
      alternateRowStyles: {
        fillColor: [249, 249, 249]
      }
    });

    // Add Additional Regions Input Data if any
    if (multiSelectedRegions && multiSelectedRegions.length > 0) {
      const additionalStartY = doc.lastAutoTable.finalY + 15;
      doc.setFontSize(12);
      doc.text('Additional regions - Read requests per second', 14, additionalStartY);

      const additionalData = [
        ['Region', 'Read requests per second']
      ];

      multiSelectedRegions.forEach(region => {
        additionalData.push([
          region.value,
          formData[region.value]?.averageReadRequestsPerSecond || '0'
        ]);
      });

      doc.autoTable({
        head: [additionalData[0]],
        body: additionalData.slice(1),
        startY: additionalStartY + 10,
        theme: 'grid',
        styles: {
          fontSize: 10,
          cellPadding: 5,
          lineColor: [238, 238, 238],
          lineWidth: 0.1,
        },
        headStyles: {
          fillColor: [35, 47, 62],
          textColor: [255, 255, 255],
          fontStyle: 'bold'
        },
        alternateRowStyles: {
          fillColor: [249, 249, 249]
        }
      });
    }

    // Save the PDF
    doc.save('amazon-keyspaces-pricing-calculator-results.pdf');
  };

  return (
    <SpaceBetween size="m">
      <Box float="right">
        <Link onFollow={exportToPDF} href="#">
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
