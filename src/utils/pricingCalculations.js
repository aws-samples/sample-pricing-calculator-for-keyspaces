export function getAvgProvisionedCapacityUnits(requests, size, cuMultiplier) {
  return Math.ceil((requests * Math.ceil(size * 1 / (cuMultiplier * 1024))) / 0.70);
}

export function getOnDemandCUs(requests, size, cuMultiplier) {
  return requests * Math.ceil(size * 1 / (cuMultiplier * 1024));
}

export function calculatePricing(formData, isMultiRegion, currentPricing, selectedRegion, multiSelectedRegions) {
  const requests = Number(formData.requests);
  const size = Number(formData.size);
  const cuMultiplier = Number(formData.cuMultiplier);
  let totalCost = 0;
  console.log('formData', formData);

  // Calculate provisioned capacity units
  const provisionedCUs = getAvgProvisionedCapacityUnits(requests, size, cuMultiplier);

  // Calculate on-demand capacity units
  const onDemandCUs = getOnDemandCUs(requests, size, cuMultiplier);

  if (isMultiRegion) {
    // Multi-region pricing calculation
    multiSelectedRegions.forEach(region => {
      const regionPricing = currentPricing[region];
      const provisionedCost = provisionedCUs * regionPricing.provisioned;
      const onDemandCost = onDemandCUs * regionPricing.onDemand;
      totalCost += provisionedCost + onDemandCost;
    });
  } else {
    // Single-region pricing calculation
    const regionPricing = currentPricing[selectedRegion];
    const provisionedCost = provisionedCUs * regionPricing.provisioned;
    const onDemandCost = onDemandCUs * regionPricing.onDemand;
    totalCost = provisionedCost + onDemandCost;
  }

  return {
    provisionedCUs,
    onDemandCUs,
    totalCost: totalCost.toFixed(2),
  };
}
