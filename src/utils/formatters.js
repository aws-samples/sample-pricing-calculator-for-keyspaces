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
        'PITR': '(PITR)',
        'pitr': '(PITR)',
        'Pitr': '(PITR)',
        // Add other special terms here
    };
    return key
        .replace(/([A-Z])/g, ' $1')
        .trim()
        .replace(/^\w/, c => c.toUpperCase())
        .replace(/\b(\w+)\b/g, match => specialTerms[match.toLowerCase()] || match);
};

export const getFieldDescription = (key) => {
    const descriptions = {
        averageReadRequestsPerSecond: "Enter the average number of read requests per second",
        averageWriteRequestsPerSecond: "Enter the average number of write requests per second that your workload needs.",
        averageRowSizeInBytes: "Enter the average size of a row in bytes.",
        storageInGb: "Enter the amount of storage in GB.",
        pointInTimeRecoveryPITR: "Enable or disable Point-in-Time Recovery.",
        ttlDeletesPerSecond: "Enter the average number of TTL deletes per second.",
        // Add descriptions for other fields here
    };
    return descriptions[key] || "";
};

export const getFieldInfoContent = (key) => {
    const infoContent = {
        averageReadRequestsPerSecond: "This helps estimate your read capacity needs. Each read request can read upto 4KB of data. Read operations include SELECT queries.",
        averageWriteRequestsPerSecond: "This helps estimate your write capacity needs.  Write operations include INSERT, UPDATE, and DELETE operations Each write request can write upto 1KB of data.",
        averageRowSizeInBytes: "This helps estimate the number of capacity units used per operation",
        storageInGb: "This determines your storage costs.",
        pointInTimeRecoveryPITR: "Point-in-time recovery (PITR) helps protect your Amazon Keyspaces tables from accidental write or delete operations by providing you continuous backups of your table data. If you enable it, the amount of PITR storage is exactly the same as your total storage.",
        ttlDeletesPerSecond: "DELETE operations triggered by the TTL process which deletes expired data. If you do not plan to enable TTL, use zero for number of delete operations.",
        // Add info content for other fields here
    };
    return infoContent[key] || "";
};
