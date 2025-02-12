export const formatPrice = (value) => {
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    minimumFractionDigits: 0,
    maximumFractionDigits: 0
  }).format(value);
};
  
  export const formatNumber = (number) => {
    return number.toLocaleString();
  };

export const formatLabel = (key) => {
    if (!key) return '';
    const specialTerms = {
        'ttl': 'TTL',
        'gb': '(GB)',
        // Add other special terms here
    };
    return key
        .replace('multi','')
        .replace(/([A-Z])/g, ' $1')
        .trim()
        .replace(/^\w/, c => c.toUpperCase())
        .replace(/\b(\w+)\b/g, match => specialTerms[match.toLowerCase()] || match);
};

export const getFieldDescription = (key) => {
  const descriptions = {
    averageReadRequestsPerSecond: "Enter the average number of read requests per second ",
    averageWriteRequestsPerSecond: "Enter the average number of write requests per second ",
    averageRowSizeInBytes: "Enter the average row size in bytes ",
    storageSizeInGb: "Enter the amount of storage in gigabytes (GB). ",
    pointInTimeRecoveryForBackups: "Enable or disable Point-in-Time Recovery (PITR) ",
    averageTtlDeletesPerSecond: "Enter the average number of TTL deletes per second ",
    multiAverageWriteRequestsPerSecond: "Displays the replicated write requests in this region ",
    multistorageSizeInGb: "Displays the replicated storage in this region ",
    multiaverageTtlDeletesPerSecond: "Displays the replicated TTL deletes in this region ",
    // Add descriptions for other fields here
  };
  return descriptions[key] || "";
};

export const getFieldInfoContent = (key) => {
  const infoContent = {
    averageReadRequestsPerSecond: 
      "Estimate the average read operations per second occur in this region. Different regions can have different read rates. In Cassandra and Keyspaces read request are SELECT queries. ",
    
    averageWriteRequestsPerSecond: 
      "Estimate the average write operations per second across all regions. Write operations include INSERT, UPDATE, and DELETE. Each region might have a different write rate, but due to active-active replication all regions will see the same number of writes. For example, if region A sees 100 writes per second, and region B sees 200 writes per second, enter 300 writes per second.",

    averageRowSizeInBytes: 
      "Your average row size determines how many capacity units each request consumes. This feild will be used to estimate read, writes, and TTL capcuty for all regions.",

    storageSizeInGb: 
      "Enter the uncompressed size of your data for a single reglica. In Cassandra, you will often have multiple replicas (three replicas in a datacenter * multiple datacenters). Amazon Keyspaces storage is based on the raw size of a single replica in a region. This calculator will apply the same storage amount enterd to every region. If you enter in 1000GB (1TB) and have three regions, the calculator will estimate a total of 3000 GB (3TB) total storage size.",

    pointInTimeRecoveryForBackups: 
      "Point-in-Time Recovery (PITR) continuously backs up your table data to help protect against accidental writes or deletes. If you enable PITR, its storage usage matches your total storage. PITR is based on the storage size. If you have 3000 GB (3TB) of storage, PITR storage will be 3000 GB (TB)",

    averageTtlDeletesPerSecond: 
      "TTL delete operations remove expired data automatically.  TTL pricing is based on the size of the rows being deleted or updated by using TTL. TTL operations are metered in units of TTL deletes. One TTL delete is consumed per KB of data per row that is deleted or updated. For example, to update a row that stores 2.5 KB of data and delete one or more columns within the row at the same time requires 3 TTL deletes. Or, to delete an entire row that contains 3.5 KB of data requires 4 TTL deletes. You will see TTL Delete chanrge per region where the row is replicated",

    multiAverageWriteRequestsPerSecond: 
      "This read-only field shows the number of writes performed in this region. Writes in any region replicate to all regions. To modify the number of writes, use the 'Average Write Requests Per Second' field. For example, if Region A has 100 writes/second and Region B has 200 writes/second, enter 300 in the 'Average Write Requests Per Second' field.",

    multistorageSizeInGb: 
      "This read-only field shows the calculated storage for this region. Storage is replicated across all regions. To update the total storage, use the 'Storage (GB)' field above.",

    multiaverageTtlDeletesPerSecond: 
      "This read-only field shows the TTL deletes in this region. TTL deletes happen across all regions. For example, if you write data with a 1-day (86,400 seconds) TTL to three regions, the data replicates three times, and TTL deletes occur in each region after one day.",
    
    // Add info content for other fields here
  };
  return infoContent[key] || "";
};

