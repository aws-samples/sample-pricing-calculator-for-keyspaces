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
  onKeyUp,
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
                averageReadRequestsPerSecond:  prevFormData[selectedRegion].averageReadRequestsPerSecond,
                averageWriteRequestsPerSecond: prevFormData[selectedRegion].averageWriteRequestsPerSecond,
                averageTtlDeletesPerSecond: prevFormData[selectedRegion].averageTtlDeletesPerSecond,
                averageRowSizeInBytes: prevFormData[selectedRegion].averageRowSizeInBytes,
                storageSizeInGb: prevFormData[selectedRegion].storageSizeInGb,
                pointInTimeRecoveryForBackups: prevFormData[selectedRegion].pointInTimeRecoveryForBackups
            }
        };
    });

    setSelectedRegion(newRegion);
    
    // Clear all multi-selected regions
    setMultiSelectedRegions([]);

    onKeyUp()
  };

  const handleMultiRegionChange = ({ detail }) => {
    
    if (detail.selectedOptions.length <= 5) {
      setMultiSelectedRegions(detail.selectedOptions);
    } else {
      setMultiSelectedRegions(detail.selectedOptions.slice(0, 5));
    }
    
    onKeyUp()
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
        setFormData(prevFormData => {
          const updatedFormData = { ...prevFormData };
      
          Object.keys(updatedFormData).forEach(regionKey => {

            if (regionKey !== selectedRegion){
            
              updatedFormData[regionKey] = {
              ...updatedFormData[regionKey],
              averageWriteRequestsPerSecond: prevFormData[selectedRegion].averageWriteRequestsPerSecond,
              averageTtlDeletesPerSecond: prevFormData[selectedRegion].averageTtlDeletesPerSecond,
              averageRowSizeInBytes: prevFormData[selectedRegion].averageRowSizeInBytes,
              storageSizeInGb: prevFormData[selectedRegion].storageSizeInGb,
              pointInTimeRecoveryForBackups: prevFormData[selectedRegion].pointInTimeRecoveryForBackups
            };
          }
          });
      
          return updatedFormData;
        });
    } else {
        // For replicated regions, only update averageReadRequestsPerSecond
        // and set other values to match primary region
        setFormData(prevFormData => ({
            ...prevFormData,
            [regionKey]: {
                ...prevFormData[selectedRegion], // Copy values from primary region
                averageReadRequestsPerSecond: name === 'multiAverageReadRequestsPerSecond' ? value : prevFormData[regionKey]?.averageReadRequestsPerSecond,
                // Force other values to match primary region
                averageWriteRequestsPerSecond: prevFormData[selectedRegion].averageWriteRequestsPerSecond,
                averageTtlDeletesPerSecond: prevFormData[selectedRegion].averageTtlDeletesPerSecond,
                averageRowSizeInBytes: prevFormData[selectedRegion].averageRowSizeInBytes,
                storageSizeInGb: prevFormData[selectedRegion].storageSizeInGb,
                pointInTimeRecoveryForBackups: prevFormData[selectedRegion].pointInTimeRecoveryForBackups
            }
        }));
         
        
    }
  
    onKeyUp()
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
    //handleInputChange({ detail: { name: 'multiAverageReadRequestsPerSecond', value: formData[regionValue].averageReadRequestsPerSecond } }, regionValue);
  };
  
  return (
      
    <form onSubmit={onSubmit} onKeyUp={onKeyUp} key="inputform">
      <SpaceBetween direction='vertical' size="xl">
    
        <FormField label="Choose a region" key="inputformInner">
          <Select
            key="regionSelection"
            options={awsRegions.map(region => ({ value: region, label: region }))}
            selectedOption={{ value: selectedRegion, label: selectedRegion }}
            onChange={handleRegionChange}
          />
        </FormField>
        <FormField label="Choose additional regions for active-active multi-region replication (0-5)">
          <Multiselect
            key="replicaMultiSelect"
            placeholder="Select regions"
            options={awsRegions
              .filter(region => region !== selectedRegion)
              .map(region => ({ value: region, label: region }))}
            selectedOptions={multiSelectedRegions}
            onChange={handleMultiRegionChange}
            deselectAriaLabel={option => `Remove ${option.label}`}
          />
        </FormField>
        <ExpandableSection 
            header={selectedRegion}
            expanded={true}
        >
          <SpaceBetween direction='vertical' size="xl">
           
          {Object.entries(formData[selectedRegion] || {}).map(([key, value]) => (
            <InputField 
              key={key}
              fieldKey={key}
              value={value}
              handleInputChange={(e) => handleInputChange(e, selectedRegion)}
              regionKey={selectedRegion}
            />
         ))}
         </SpaceBetween>
        </ExpandableSection>
        {multiSelectedRegions.map((region) => (
          <ExpandableSection 
            key={region.label}
            header={region.label}
            expanded={expandedRegions[region.value] || false}
            onChange={({ detail }) => handleExpandChange(region.value, detail.expanded)}
          >
            <SpaceBetween direction='vertical' size="xl">
          
           
            <InputField
              key="multiAverageReadRequestsPerSecond"
              fieldKey="multiAverageReadRequestsPerSecond"
              value={formData[region.value]?.averageReadRequestsPerSecond}
              handleInputChange={(e) => handleInputChange(e, region.value)}
              regionKey={region.value}
            />
          
           
          <ExpandableSection
             headerText ="Relicated"
              expanded={true}
              variant="container"
              >
            <InputField
             key="multiAverageWriteRequestsPerSecond"
              
              fieldKey="multiAverageWriteRequestsPerSecond"
              value={
                formData[region.value]?.averageWriteRequestsPerSecond 
              }
              regionKey={region.value}
            />
            
            <InputField
              key="multistorageSizeInGb"
              fieldKey="multistorageSizeInGb"
              value={
                formData[region.value]?.storageSizeInGb 
              }
              handleInputChange={(e) => handleInputChange(e, region.value)}
              readonly={true}
              regionKey={region.value}
              disabled={true}
            />
            
            <InputField
              key="multiaverageTtlDeletesPerSecond"
              fieldKey="multiaverageTtlDeletesPerSecond"
              value={
                formData[region.value]?.averageTtlDeletesPerSecond 
              }
              handleInputChange={(e) => handleInputChange(e, region.value)}
              regionKey={region.value}
            />
            </ExpandableSection>
          </SpaceBetween>
          </ExpandableSection>
        ))}
        
        </SpaceBetween>
    </form>
   
  );
}

export default MultiRegionForm;
