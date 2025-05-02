export function getAvgProvisionedCapacityUnits(requests, size, cuMultiplier) {
  return Math.ceil((requests * Math.ceil(size * 1 / (cuMultiplier * 1024))) / 0.70);
}

export function getOnDemandCUs(requests, size, cuMultiplier) {
  return requests * Math.ceil(size * 1 / (cuMultiplier * 1024));
}

