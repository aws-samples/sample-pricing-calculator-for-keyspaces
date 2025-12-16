
import savingsPlansDataJson from '../data/savings-plans.json';
import keyspacesPricingDataJson from '../data/keyspaces-pricing.json';
import regionsDataJson from '../data/regions.json';


export const savingsPlansData = savingsPlansDataJson;
export const keyspacesPricingData = keyspacesPricingDataJson;

function normalizeUsageType(usagetype) {
    if (!usagetype) return null;
  
    // Split on dashes, drop the first token (region abbrev),
    // then join the rest without dashes.
    const parts = usagetype.split("-");
    if (parts.length <= 1) {
      // No region prefix, just strip dashes
      return usagetype.replace(/-/g, "");
    }
  
    return parts.slice(1).join(""); // e.g. ["TimedStorage", "ByteHrs"] -> "TimedStorageByteHrs"
  }
  
  function buildRegionPricingMap() {
    const regionMap = {};
  
    const priceList = keyspacesPricingData.PriceList || [];

    for (const itemStr of priceList) {
      let item;
      try {
        // Each element of PriceList is itself a JSON string
        item = JSON.parse(itemStr);
      } catch (e) {
        // Skip invalid entries
        continue;
      }
  
      const product = item.product || {};
      const attrs = product.attributes || {};
      const region = attrs.regionCode;
      const usagetype = attrs.usagetype;
  
      if (!region || !usagetype) continue;
  
      const fieldName = normalizeUsageType(usagetype);
      if (!fieldName) continue;
  
      // Walk into terms -> OnDemand -> first term -> first priceDimension
      const onDemand = item.terms && item.terms.OnDemand;
      if (!onDemand) continue;
  
      const term = Object.values(onDemand)[0];
      if (!term) continue;
  
      const priceDimensions = term.priceDimensions;
      if (!priceDimensions) continue;
  
      const dim = Object.values(priceDimensions)[0];
      if (!dim || !dim.pricePerUnit) continue;
  
      const usdStr = dim.pricePerUnit.USD;
      if (!usdStr) {
        // No USD price (e.g., CNY-only) — skip if you only care about USD
        continue;
      }
  
      const price = parseFloat(usdStr);
      if (Number.isNaN(price)) continue;
  
      if (!regionMap[region]) {
        regionMap[region] = {};
      }
  
      // Last one wins if duplicated usagetype in same region
      regionMap[region][fieldName] = price;
    }
  
    return regionMap;
  }

  function saveingsPlansMap() {

    const savingsPlansDataMap = {};

    for (const savingsPlan of savingsPlansData.searchResults) {
      
     const usageType = savingsPlan.unit.replace(/-/g, "");

     const rate = savingsPlan.rate;

     const properties = savingsPlan.properties;

     let region = null;
     for (const property of properties) {
        if (property.name === 'region') {
          region = property.value;
        }
     }
     if (!region) continue;

     const longRegionName = regionsDataJson[region];

     let regionSavingsPlans = {};
     if (longRegionName in savingsPlansDataMap) {
       
       regionSavingsPlans = savingsPlansDataMap[longRegionName];
       
     }
    
     regionSavingsPlans[usageType] = {usageType: usageType, rate: rate, region: region, longRegionName: longRegionName};
     
     savingsPlansDataMap[longRegionName] = regionSavingsPlans;
    }

    console.log(JSON.stringify(savingsPlansDataMap));

    return savingsPlansDataMap;
  }
  
  export default saveingsPlansMap();
  // Example usage:
  // const data = JSON.parse(yourJsonString);
  // const regionPricing = buildRegionPricingMap(data);
  // console.log(JSON.stringify(regionPricing, null, 2));
  