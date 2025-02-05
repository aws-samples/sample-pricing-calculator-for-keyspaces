import pricingDataJson from '../data/mcs.json';

// Get all available regions from the pricing data
export const awsRegions = Object.keys(pricingDataJson.regions)

// Map for region codes
export const regionCodeMap = Object.keys(pricingDataJson.regions).reduce((acc, region) => {
    acc[region] = region;
    return acc;
}, {});

export const getRegionCode = (regionName) => {
    return regionCodeMap[regionName] || regionName;
};