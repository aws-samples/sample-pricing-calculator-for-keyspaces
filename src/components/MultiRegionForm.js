import React, { useEffect, useCallback } from 'react';
import { FormField, Select, SpaceBetween, Button, Box, Multiselect, ExpandableSection } from '@cloudscape-design/components';
import InputField from './InputField';
import { awsRegions } from '../constants/regions';

function MultiRegionForm({ 
  selectedRegion, 
  setSelectedRegion, 
  multiSelectedRegions, 
  setMultiSelectedRegions, 
  formData, 
  setFormData, 
  onSubmit,
  expandedRegions,
  setExpandedRegions
}) {
  const handleRegionChange = ({ detail }) => {
    const newRegion = detail.selectedOption.value;
    
    // Update form data for the new region
    setFormData(prevFormData => {
        // Only keep the new region, clear everything else
        return {
            [newRegion]: {
                averageReadRequestsPerSecond: 0,
                averageWriteRequestsPerSecond: 0,
                averageRowSizeInBytes: 0,
                storageInGb: 0,
                pointInTimeRecoveryPITR: false,
                ttlDeletesPerSecond: 0
            }
        };
    });

    setSelectedRegion(newRegion);
    
    // Clear all multi-selected regions
    setMultiSelectedRegions([]);
  };

  const handleMultiRegionChange = ({ detail }) => {
    if (detail.selectedOptions.length <= 5) {
      setMultiSelectedRegions(detail.selectedOptions);
    } else {
      setMultiSelectedRegions(detail.selectedOptions.slice(0, 5));
    }
  };

  const handleInputChange = (event, regionKey) => {
    const { name, value, type, checked } = event.detail;
    
    if (regionKey === selectedRegion) {
        // For primary region, handle all fields normally
        setFormData(prevFormData => ({
            ...prevFormData,
            [regionKey]: {
                ...prevFormData[regionKey],
                [name]: type === 'checkbox' ? checked : value
            }
        }));
    } else {
        // For replicated regions, only update averageReadRequests
        // and set other values to match primary region
        setFormData(prevFormData => ({
            ...prevFormData,
            [regionKey]: {
                ...prevFormData[selectedRegion], // Copy values from primary region
                averageReadRequestsPerSecond: name === 'averageReadRequestsPerSecond' ? value : prevFormData[regionKey]?.averageReadRequestsPerSecond,
                // Force other values to match primary region
                averageWriteRequestsPerSecond: 0,
                averageRowSizeInBytes: prevFormData[selectedRegion].averageRowSizeInBytes,
                storageInGb: prevFormData[selectedRegion].storageInGb,
                pointInTimeRecoveryPITR: prevFormData[selectedRegion].pointInTimeRecoveryPITR,
                ttlDeletesPerSecond: prevFormData[selectedRegion].ttlDeletesPerSecond
            }
        }));
    }
  };

  // Memoize the function to update expanded regions
  const updateExpandedRegions = useCallback(() => {
    setExpandedRegions(prevExpandedRegions => {
      const newExpandedRegions = { ...prevExpandedRegions };
      multiSelectedRegions.forEach(region => {
        if (!(region.value in newExpandedRegions)) {
          newExpandedRegions[region.value] = false;
        }
      });
      // Remove any regions that are no longer selected
      Object.keys(newExpandedRegions).forEach(regionValue => {
        if (!multiSelectedRegions.some(region => region.value === regionValue)) {
          delete newExpandedRegions[regionValue];
        }
      });
      return newExpandedRegions;
    });
  }, [multiSelectedRegions, setExpandedRegions]);

  // Use the memoized function in useEffect
  useEffect(() => {
    updateExpandedRegions();
  }, [updateExpandedRegions]);

  const handleExpandChange = (regionValue, isExpanded) => {
    setExpandedRegions(prev => ({...prev, [regionValue]: isExpanded}));
  };

  return (
    <form onSubmit={onSubmit}>
      <SpaceBetween size="l">
        <FormField label="Primary AWS Region">
          <Select
            options={awsRegions.map(region => ({ value: region, label: region }))}
            selectedOption={{ value: selectedRegion, label: selectedRegion }}
            onChange={handleRegionChange}
          />
        </FormField>
        <FormField label="Multi-region Replication. Choose additional regions for active-active replication (0-5)">
          <Multiselect
            placeholder="Select regions"
            options={awsRegions
              .filter(region => region !== selectedRegion)
              .map(region => ({ value: region, label: region }))}
            selectedOptions={multiSelectedRegions}
            onChange={handleMultiRegionChange}
            deselectAriaLabel={option => `Remove ${option.label}`}
          />
        </FormField>
        <Box variant="h3">Primary Region: {selectedRegion}</Box>
        {Object.entries(formData[selectedRegion] || {}).map(([key, value]) => (
          <InputField
            key={key}
            fieldKey={key}
            value={value}
            handleInputChange={(e) => handleInputChange(e, selectedRegion)}
            regionKey={selectedRegion}
          />
        ))}
        {multiSelectedRegions.map((region) => (
          <ExpandableSection
            key={region.value}
            header={region.label}
            expanded={expandedRegions[region.value] || false}
            onChange={({ detail }) => handleExpandChange(region.value, detail.expanded)}
          >
            <InputField
              key="averageReadRequestsPerSecond"
              fieldKey="averageReadRequestsPerSecond"
              value={formData[region.value]?.averageReadRequestsPerSecond || 0}
              handleInputChange={(e) => handleInputChange(e, region.value)}
              regionKey={region.value}
            />
          </ExpandableSection>
        ))}
        <Button variant="primary" onClick={onSubmit}>Calculate</Button>
      </SpaceBetween>
    </form>
  );
}

export default MultiRegionForm;
