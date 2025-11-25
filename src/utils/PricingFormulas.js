import pricingDataJson from '../data/mcs.json';
import { system_keyspaces, REPLICATION_FACTOR, SECONDS_PER_MONTH, GIGABYTE } from './Constants';
import { HOURS_PER_MONTH } from '../components/ParsingHelpers';

/**
 * Build a normalized Cassandra dataset from raw samples and status data.
 *
 * @param {Object} samples - Object mapping datacenter name to node map with payloads.
 * @param {Object} samples[dcName] - Map of nodeId -> payload bundle for a datacenter.
 * @param {Object} samples[dcName][nodeId] - Bundle with parsed inputs for one node.
 * @param {Object} samples[dcName][nodeId].tablestats_data - Keyed by keyspace -> table -> metrics.
 * @param {Object} samples[dcName][nodeId].schema - Keyspace replication info: { [keyspace]: { datacenters: { [dcName]: rf } } }.
 * @param {Object} samples[dcName][nodeId].info_data - Includes `uptime_seconds`.
 * @param {Object} samples[dcName][nodeId].row_size_data - Keyed by fully-qualified table name.
 * @param {Map<string, number>} statusData - Map of datacenterName -> node count.
 * @returns {{data: {keyspaces: Object}}} Aggregated structure keyed by keyspace -> dc -> tables with monthly metrics.
 */
export const buildCassandraLocalSet = (samples, statusData) => {

    /*
    'data': {
            'keyspaces': {
                'keyspace_name': {
                    'type': 'system' or 'user',
                    'dcs': {
                        'dc_name': {
                            'number_of_nodes': Decimal,
                            'replication_factor': Decimal,
                            'tables': {
                                'table_name': {
                                    'total_compressed_bytes': Decimal,
                                    'total_uncompressed_bytes': Decimal,
                                    'avg_row_size_bytes': Decimal,
                                    'writes_monthly': Decimal,
                                    'reads_monthly': Decimal,
                                    'has_ttl': Boolean,
                                    'sample_count': Decimal,
                                }
                            }
                        }
                    }
                }
            }
        }
    */
    const result = {
        data: {
            keyspaces: {}
        }
    };

    for (const [dcName, dcData] of Object.entries(samples)) {
        const numberOfNodes = statusData.get(dcName);
        console.log('numberOfNodes', numberOfNodes);
        console.log('dcData', dcData);
        for (const [_nodeId, nodeData] of Object.entries(dcData)) {
            console.log('nodeData', nodeData);
            const tablestatsData = nodeData.tablestats_data;
            const schema = nodeData.schema;
            const infoData = nodeData.info_data;
            const rowSizeData = nodeData.row_size_data;
            const uptimeSeconds = infoData.uptime_seconds;

            for (const [keyspaceName, keyspaceData] of Object.entries(tablestatsData)) {
                if (schema && schema[keyspaceName]) {
                    if (!schema[keyspaceName].datacenters[dcName]) {
                        continue;
                    }
                }

                if (!result.data.keyspaces[keyspaceName]) {
                    result.data.keyspaces[keyspaceName] = {
                        type: isSystemKeyspace(keyspaceName),
                        dcs: {}
                    };
                }

                let replicationFactor = REPLICATION_FACTOR;
                if (schema && schema[keyspaceName]) {
                    replicationFactor = schema[keyspaceName].datacenters[dcName];
                }

                if (!result.data.keyspaces[keyspaceName].dcs[dcName]) {
                    result.data.keyspaces[keyspaceName].dcs[dcName] = {
                        number_of_nodes: numberOfNodes,
                        replication_factor: replicationFactor,
                        tables: {}
                    };
                }

                for (const [tableName, tableData] of Object.entries(keyspaceData)) {
                    if (!result.data.keyspaces[keyspaceName].dcs[dcName].tables[tableName]) {
                        const fullyQualifiedTableName = `${keyspaceName}.${tableName}`;
                        let hasTtl = false;
                        let averageBytes = 1;

                        if (rowSizeData[fullyQualifiedTableName]) {
                            const avgNumber = rowSizeData[fullyQualifiedTableName].average || '1';
                            const parsedBytes = parseInt(avgNumber);
                            if (isNaN(parsedBytes) || parsedBytes <= 0) {
                                averageBytes = 1;
                            } else {
                                averageBytes = parsedBytes;
                            }

                            const ttlStr = rowSizeData[fullyQualifiedTableName]['default-ttl'] || 'y';
                            hasTtl = ttlStr.trim() === 'n';
                        }

                        result.data.keyspaces[keyspaceName].dcs[dcName].tables[tableName] = {
                            table_name: tableName,
                            total_compressed_bytes: 0,
                            total_uncompressed_bytes: 0,
                            avg_row_size_bytes: averageBytes,
                            writes_monthly: 0,
                            reads_monthly: 0,
                            has_ttl: hasTtl,
                            sample_count: 0
                        };
                    }

                    let spaceUsed = tableData.space_used || 0;
                    if (isNaN(spaceUsed) || spaceUsed === null || spaceUsed === undefined) {
                        spaceUsed = 0;
                    }
                    const ratio = spaceUsed > 0 ? tableData.compression_ratio : 1;
                    let readCount = tableData.read_count || 0;
                    let writeCount = tableData.write_count || 0;

                    if (isNaN(readCount) || readCount === null || readCount === undefined) {
                        readCount = 0;
                    }
                    if (isNaN(writeCount) || writeCount === null || writeCount === undefined) {
                        writeCount = 0;
                    }

                    const table = result.data.keyspaces[keyspaceName].dcs[dcName].tables[tableName];
                    table.total_compressed_bytes += spaceUsed;
                    table.total_uncompressed_bytes += calculateUncompressedStoragePerNode(spaceUsed, ratio);
                    table.writes_monthly += calculateWriteOperationsPerNodePerMonth(writeCount, uptimeSeconds);
                    table.reads_monthly += calculateReadOperationsPerNodePerMonth(readCount, uptimeSeconds);
                    table.sample_count += 1;
                }
            }
        }
    }

    return result;
};

/**
 * Aggregate table-level metrics to keyspace-level for a specific datacenter.
 *
 * @param {{data:{keyspaces:Object}}} cassandra_set - Output of buildCassandraLocalSet.
 * @param {string} datacenter - Datacenter name to aggregate.
 * @returns {Object} keyspace -> aggregated metrics (reads/writes per second, storage GB, averages, TTLs).
 */
export const getKeyspaceCassandraAggregate = (cassandra_set, datacenter) => {
    /*
    'data': {
            'keyspaces': {
                'keyspace_name': {
                    'type': 'system' or 'user'
                    'total_live_space_gb': Decimal,
                    'uncompressed_single_replica_gb': Decimal,
                    'avg_row_size_bytes': Decimal,
                    'writes_per_second': Decimal,
                    'reads_per_second': Decimal,
                    'ttls_per_second': Boolean,
                    'sample_count': Decimal
                    tables: {
                        'table_name': {
                            'total_compressed_bytes': Decimal,
                            'total_uncompressed_bytes': Decimal,
                            'avg_row_size_bytes': Decimal,
                            'writes_per_second': Decimal,
                            'reads_per_second': Decimal,
                            'ttls_per_second': Decimal,
                            'sample_count': Decimal
                        }
                    }
                }
            }
        }
    }
    */

    const keyspace_aggregate = {};

    for (const [keyspace, keyspaceData] of Object.entries(cassandra_set.data.keyspaces)) {
        if (keyspaceData.type === 'system') {
            continue;
        }

        const number_of_nodes = keyspaceData.dcs[datacenter].number_of_nodes;
        const replication_factor = keyspaceData.dcs[datacenter].replication_factor;

        let keyspace_writes_total = 0;
        let keyspace_reads_total = 0;
        let total_live_space = 0;
        let uncompressed_single_replica = 0;
        let write_row_size_bytes = 0;
        let read_row_size_bytes = 0;
        let keyspace_ttls_total = 0;

        for (const [_table, tableData] of Object.entries(keyspaceData.dcs[datacenter].tables)) {

            keyspace_writes_total += tableData.writes_monthly / tableData.sample_count;
            total_live_space += tableData.total_compressed_bytes / tableData.sample_count;
            uncompressed_single_replica += tableData.total_uncompressed_bytes / tableData.sample_count;
            write_row_size_bytes += (tableData.writes_monthly * tableData.avg_row_size_bytes) / tableData.sample_count;
            read_row_size_bytes += (tableData.reads_monthly * tableData.avg_row_size_bytes) / tableData.sample_count;
            keyspace_reads_total += tableData.reads_monthly / tableData.sample_count;
            keyspace_ttls_total += tableData.has_ttl ? tableData.writes_monthly / tableData.sample_count : 0;
        }

        const average_read_row_size_bytes = read_row_size_bytes / (keyspace_reads_total > 0 ? keyspace_reads_total : 1);
        const average_write_row_size_bytes = write_row_size_bytes / (keyspace_writes_total > 0 ? keyspace_writes_total : 1);
        keyspace_aggregate[keyspace] = {
            keyspace_name: keyspace,
            keyspace_type: keyspaceData.type,
            replication_factor: replication_factor,
            total_live_space_gb: (total_live_space * number_of_nodes) / GIGABYTE,
            uncompressed_single_replica_gb: (uncompressed_single_replica * number_of_nodes) / replication_factor / GIGABYTE,
            avg_write_row_size_bytes: average_write_row_size_bytes,
            avg_read_row_size_bytes: average_read_row_size_bytes,
            writes_per_second: (keyspace_writes_total / SECONDS_PER_MONTH) * (number_of_nodes / replication_factor),
            reads_per_second: (keyspace_reads_total / SECONDS_PER_MONTH) * (number_of_nodes / (replication_factor - 1 > 0 ? replication_factor - 1 : 1)),
            ttls_per_second: (keyspace_ttls_total / SECONDS_PER_MONTH) * (number_of_nodes / replication_factor)
        };
    }

    return keyspace_aggregate;
};

/**
 * Resolve pricing primitives for a region from pricing JSON.
 * @param {string} regionName - Human-readable region name that matches keys in pricing JSON.
 * @returns {null|{readRequestPrice:number, writeRequestPrice:number, writeRequestPricePerHour:number, readRequestPricePerHour:number, storagePricePerGB:number, pitrPricePerGB:number, ttlDeletesPrice:number}}
 */
const getRegionPricing = (regionName) => {
    if (!pricingDataJson || !pricingDataJson.regions || !pricingDataJson.regions[regionName]) {
        return null;
    }

    const regionPricing = pricingDataJson.regions[regionName];

    return {
        readRequestPrice: regionPricing['MCS-ReadUnits'].price,
        writeRequestPrice: regionPricing['MCS-WriteUnits'].price,
        writeRequestPricePerHour: regionPricing['Provisioned Write Units'].price,
        readRequestPricePerHour: regionPricing['Provisioned Read Units'].price,
        storagePricePerGB: regionPricing['AmazonMCS - Indexed DataStore per GB-Mo'].price,
        pitrPricePerGB: regionPricing['Point-In-Time-Restore PITR Backup Storage per GB-Mo'].price,
        ttlDeletesPrice: regionPricing['Time to Live'].price
    };
};

/**
 * Compute per-datacenter and total monthly costs (provisioned and on-demand) from keyspace aggregates.
 * @param {Array<{name:string}>} datacenters - List of Cassandra datacenters.
 * @param {Object} regions - Map of datacenterName -> region name.
 * @param {Object} estimateResults - Map of datacenterName -> keyspace aggregates from getKeyspaceCassandraAggregate.
 * @returns {{ total_datacenter_cost:Object, total_monthly_provisioned_cost:number, total_monthly_on_demand_cost:number }}
 */
export const calculatePricingEstimate = (datacenters, regions, estimateResults) => {
    if (!Array.isArray(datacenters) || datacenters.length === 0) return null;

    const pricingData = {};
    let total_monthly_provisioned_cost = 0;
    let total_monthly_on_demand_cost = 0;

    datacenters.forEach(dc => {
        const region = regions[dc.name];
        const results = estimateResults[dc.name];

        if (results && region) {
            const regionPricing = getRegionPricing(region);
            if (!regionPricing) {
                return;
            }

            let total_datacenter_provisioned_cost = 0;
            let total_datacenter_on_demand_cost = 0;
            const keyspaceCosts = {};
            keyspaceCosts['totals'] = {
                name: 'region total',
                storage: 0,
                backup: 0,
                reads_provisioned: 0,
                writes_provisioned: 0,
                reads_on_demand: 0,
                writes_on_demand: 0,
                ttlDeletes: 0,
                provisioned_total: 0,
                on_demand_total: 0
            };

            Object.entries(results).forEach(([keyspace, data]) => {

                const oneDemandWriteCost = calculateOnDemandWriteUnitsPerMonthCost(data.writes_per_second, data.avg_write_row_size_bytes, regionPricing);
                const oneDemandReadCost = calculateOnDemandReadUnitsPerMonthCost(data.reads_per_second, data.avg_read_row_size_bytes, regionPricing);
                const ttlDeleteCost = calculateTtlUnitsPerMonthCost(data.ttls_per_second, data.avg_write_row_size_bytes, regionPricing);

                const provisionedWriteCost = calculateProvisionedWriteCostPerMonth(data.writes_per_second, data.avg_write_row_size_bytes, regionPricing, .70);
                const provisionedReadCost = calculateProvisionedReadCostPerMonth(data.reads_per_second, data.avg_read_row_size_bytes, regionPricing, .70);
                
                const storageCost = calculateStorageCostPerMonth(data.uncompressed_single_replica_gb, regionPricing);
                const backupCost = calculateBackupCostPerMonth(data.uncompressed_single_replica_gb, regionPricing);
                
                const provisioned_total = provisionedCapacityTotalMonthlyCost(
                    data.reads_per_second, data.avg_read_row_size_bytes, .70, 
                     data.writes_per_second, data.avg_write_row_size_bytes, .70, 
                     data.ttls_per_second, data.avg_write_row_size_bytes, 
                      data.uncompressed_single_replica_gb, data.uncompressed_single_replica_gb,
                      regionPricing);

                const on_demand_total = calculateOnDemandCapcityTotalMonthlyCost(  
                    data.reads_per_second, data.avg_read_row_size_bytes, 
                    data.writes_per_second, data.avg_write_row_size_bytes, 
                    data.ttls_per_second, data.avg_write_row_size_bytes, 
                    data.uncompressed_single_replica_gb, data.uncompressed_single_replica_gb,
                    regionPricing);
                    
                keyspaceCosts[keyspace] = {
                    name: keyspace,
                    storage: storageCost,
                    backup: backupCost,
                    reads_provisioned: provisionedReadCost,
                    writes_provisioned: provisionedWriteCost,
                    reads_on_demand: oneDemandReadCost,
                    writes_on_demand: oneDemandWriteCost,
                    ttlDeletes: ttlDeleteCost,
                    provisioned_total: provisioned_total,
                    on_demand_total: on_demand_total
                };

                keyspaceCosts['totals'].storage += storageCost;
                keyspaceCosts['totals'].backup += backupCost;
                keyspaceCosts['totals'].reads_provisioned += provisionedReadCost;
                keyspaceCosts['totals'].writes_provisioned += provisionedWriteCost;
                keyspaceCosts['totals'].reads_on_demand += oneDemandReadCost;
                keyspaceCosts['totals'].writes_on_demand += oneDemandWriteCost;
                keyspaceCosts['totals'].ttlDeletes += ttlDeleteCost;
                keyspaceCosts['totals'].provisioned_total += provisioned_total;
                keyspaceCosts['totals'].on_demand_total += on_demand_total;

                total_datacenter_provisioned_cost += provisioned_total;
                total_datacenter_on_demand_cost += on_demand_total;
            });

            const totals = keyspaceCosts['totals'];
            delete keyspaceCosts['totals'];
            keyspaceCosts['totals'] = totals;

            pricingData[dc.name] = {
                region,
                keyspaceCosts,
                total_datacenter_provisioned_cost: total_datacenter_provisioned_cost,
                total_datacenter_on_demand_cost: total_datacenter_on_demand_cost
            };

            total_monthly_provisioned_cost += total_datacenter_provisioned_cost;
            total_monthly_on_demand_cost += total_datacenter_on_demand_cost;
        }
    });

    return {
        total_datacenter_cost: pricingData,
        total_monthly_provisioned_cost: total_monthly_provisioned_cost,
        total_monthly_on_demand_cost: total_monthly_on_demand_cost
    };
};
/***
 * CASSANDRA FORMULAS
 * /
/***
 * CASSANDRA FORMULAS
 * isSystemKeyspace
 * @param {string} keyspace - Keyspace name.
 * @returns {boolean}
 */
export const isSystemKeyspace = (keyspaceName) => {
    return system_keyspaces.has(keyspaceName) ? 'system' : 'user';
}

export const calculateWriteOperationsPerNodePerMonth = (total_writes_per_node, node_uptime_seconds) => {
   return (total_writes_per_node / node_uptime_seconds) * SECONDS_PER_MONTH;
}
export const calculateReadOperationsPerNodePerMonth = (total_reads_per_node, node_uptime_seconds) => {
    return (total_reads_per_node / node_uptime_seconds) * SECONDS_PER_MONTH;
}
export const calculateUncompressedStoragePerNode = (table_live_space_gb, compression_ratio) => {
    return table_live_space_gb / compression_ratio;
}

/**
 * isSystemKeyspace
 */


/****
 * 
 * PRICE FORMULAS
 * 
 * 
 */

/**
 * Total monthly cost using provisioned capacity and storage/backup.
 * @param {number} reads_per_second - Regional read operations per second.
 * @param {number} avg_read_row_size_bytes - Average row size in bytes.
 * @param {number} reads_target_utilization - Target read utilization [0..1].
 * @param {number} writes_per_second - Regional write operations per second.
 * @param {number} avg_write_row_size_bytes - Average row size in bytes.
 * @param {number} writes_target_utilization - Target write utilization [0..1].
 * @param {number} ttls_per_second - TTL deletes per second.
 * @param {number} avg_ttl_row_size_bytes - Average row size in bytes for TTL.
 * @param {number} uncompressed_storage_size_gb - Uncompressed single-replica storage in GB.
 * @param {number} uncompressed_backup_size_gb - Uncompressed backup size in GB for PITR.
 * @param {Object} regionPricing - Region pricing primitives from pricing JSON.
 * @returns {number}
 */
export const provisionedCapacityTotalMonthlyCost = (reads_per_second, avg_read_row_size_bytes, reads_target_utilization, writes_per_second, avg_write_row_size_bytes, writes_target_utilization, ttls_per_second, avg_ttl_row_size_bytes, uncompressed_storage_size_gb, uncompressed_backup_size_gb, regionPricing) => {
    const provisionedReadCost = calculateProvisionedReadCostPerMonth(reads_per_second, avg_read_row_size_bytes, regionPricing, reads_target_utilization);
    const provisionedWriteCost = calculateProvisionedWriteCostPerMonth(writes_per_second, avg_write_row_size_bytes, regionPricing, writes_target_utilization);
    const ttlDeleteCost = calculateTtlUnitsPerMonthCost(ttls_per_second, avg_ttl_row_size_bytes, regionPricing);
    const storageCost = calculateStorageCostPerMonth(uncompressed_storage_size_gb, regionPricing);
    const backupCost = calculateBackupCostPerMonth(uncompressed_backup_size_gb, regionPricing);
    return calculateProvisionedCapacityTotalMonthlyCostWithAggregates(provisionedReadCost, provisionedWriteCost, ttlDeleteCost, storageCost, backupCost);
} 

/**
 * Total monthly cost using on-demand capacity and storage/backup.
 * @param {number} reads_per_second - Read operations per second.
 * @param {number} avg_read_row_size_bytes - Average row size in bytes.
 * @param {number} writes_per_second - Write operations per second.
 * @param {number} avg_write_row_size_bytes - Average row size in bytes.
 * @param {number} ttls_per_second - TTL ops/s.
 * @param {number} avg_ttl_row_size_bytes - Average row size in bytes for TTL.
 * @param {number} uncompressed_storage_size_gb - Uncompressed storage size in GB.
 * @param {number} uncompressed_backup_size_gb - Uncompressed backup size in GB.
 * @param {Object} regionPricing - Pricing primitives.
 * @returns {number}
 */
export const calculateOnDemandCapcityTotalMonthlyCost = (reads_per_second, avg_read_row_size_bytes, writes_per_second, avg_write_row_size_bytes, ttls_per_second, avg_ttl_row_size_bytes, uncompressed_storage_size_gb, uncompressed_backup_size_gb, regionPricing) => {
    
    const onDemandReadCost = calculateOnDemandReadUnitsPerMonthCost(reads_per_second, avg_read_row_size_bytes, regionPricing);
    const onDemandWriteCost = calculateOnDemandWriteUnitsPerMonthCost(writes_per_second, avg_write_row_size_bytes, regionPricing);
    const onDemandTtlDeleteCost = calculateTtlUnitsPerMonthCost(ttls_per_second, avg_ttl_row_size_bytes, regionPricing);
    const storageCost = calculateStorageCostPerMonth(uncompressed_storage_size_gb, regionPricing);
    const backupCost = calculateBackupCostPerMonth(uncompressed_backup_size_gb, regionPricing);

    return calculateOnDemandCapcityTotalMonthlyCostWithAggregates(onDemandReadCost, onDemandWriteCost, onDemandTtlDeleteCost, storageCost, backupCost);
} 
/**
 * Sum helper for on-demand cost components.
 * @param {number} onDemandReadCost
 * @param {number} onDemandWriteCost
 * @param {number} onDemandTtlDeleteCost
 * @param {number} storageCost
 * @param {number} backupCost
 * @returns {number}
 */
export const calculateOnDemandCapcityTotalMonthlyCostWithAggregates = (onDemandReadCost, onDemandWriteCost, onDemandTtlDeleteCost, storageCost, backupCost) => {
    return onDemandReadCost + onDemandWriteCost + onDemandTtlDeleteCost + storageCost + backupCost;
} 
/**
 * Sum helper for provisioned cost components.
 * @param {number} provisionedReadCost
 * @param {number} provisionedWriteCost
 * @param {number} ttlDeleteCost
 * @param {number} storageCost
 * @param {number} backupCost
 * @returns {number}
 */
export const calculateProvisionedCapacityTotalMonthlyCostWithAggregates = (provisionedReadCost, provisionedWriteCost, ttlDeleteCost, storageCost, backupCost) => {
   return provisionedReadCost + provisionedWriteCost + ttlDeleteCost + storageCost + backupCost;
} 
/**
 * Provisioned monthly read cost using default target utilization (70%).
 * @param {number} reads_per_second - Read operations per second.
 * @param {number} avg_read_row_size_bytes - Average row size in bytes.
 * @param {Object} regionPricing
 * @returns {number}
 */
export const calculateProvisionedReadCostPerMonthWithDefaultProvisioning = (reads_per_second, avg_read_row_size_bytes, regionPricing) => {
    return calculateProvisionedReadCostPerMonth(reads_per_second, avg_read_row_size_bytes, regionPricing, .70);
};
/**
 * Provisioned monthly read cost.
 * @param {number} reads_per_second - Read operations per second.
 * @param {number} avg_read_row_size_bytes - Bytes.
 * @param {Object} regionPricing
 * @param {number} target_utilization - [0..1].
 * @returns {number}
 */
export const calculateProvisionedReadCostPerMonth = (reads_per_second, avg_read_row_size_bytes, regionPricing, target_utilization) => {
    return reads_per_second * calculateReadUnitsPerOperation(avg_read_row_size_bytes) * HOURS_PER_MONTH * regionPricing.readRequestPricePerHour / target_utilization;
};
/**
 * Provisioned monthly write cost using default target utilization (70%).
 * @param {number} writes_per_second - Write operations per second.
 * @param {number} avg_write_row_size_bytes - Average row size in bytes.
 * @param {Object} regionPricing
 * @returns {number}
 */
export const calculateProvisionedWriteCostPerMonthWithDefaultProvisioning = (writes_per_second, avg_write_row_size_bytes, regionPricing) => {
    return calculateProvisionedWriteCostPerMonth(writes_per_second, avg_write_row_size_bytes, regionPricing, .70);
};

/**
 * Provisioned monthly write cost.
 * @param {number} writes_per_second - Write operations per second.
 * @param {number} avg_write_row_size_bytes - Average row size in bytes.
 * @param {Object} regionPricing
 * @param {number} target_utilization - [0..1].
 * @returns {number}
 */
export const calculateProvisionedWriteCostPerMonth = (writes_per_second, avg_write_row_size_bytes, regionPricing, target_utilization) => {
    return writes_per_second * calculateWriteUnitsPerOperation(avg_write_row_size_bytes) * HOURS_PER_MONTH * regionPricing.writeRequestPricePerHour / target_utilization;
};

/**
 * Storage monthly cost for uncompressed single-replica GB.
 * @param {number} uncompressed_single_replica_gb - GB.
 * @param {Object} regionPricing
 * @returns {number}
 */
export const calculateStorageCostPerMonth = (uncompressed_single_replica_gb, regionPricing) => {
    return uncompressed_single_replica_gb * regionPricing.storagePricePerGB;
};

/**
 * PITR backup monthly cost, proportional to uncompressed GB.
 * @param {number} uncompressed_single_replica_gb - GB.
 * @param {Object} regionPricing
 * @returns {number}
 */
export const calculateBackupCostPerMonth = (uncompressed_single_replica_gb, regionPricing) => {
    return uncompressed_single_replica_gb * regionPricing.pitrPricePerGB;
};

/**
 * On-demand monthly cost for reads.
 * @param {number} reads_per_second - Read operations per second.
 * @param {number} avg_read_row_size_bytes - Bytes.
 * @param {Object} regionPricing
 * @returns {number}
 */
export const calculateOnDemandReadUnitsPerMonthCost = (reads_per_second, avg_read_row_size_bytes, regionPricing) => {
    return calcualteOnDemandReadUnitsPerMonth(reads_per_second, avg_read_row_size_bytes) * regionPricing.readRequestPrice;
};

/**
 * On-demand monthly cost for writes.
 * @param {number} writes_per_second - WU/s.
 * @param {number} avg_write_row_size_bytes - Bytes.
 * @param {Object} regionPricing
 * @returns {number}
 */
export const calculateOnDemandWriteUnitsPerMonthCost = (writes_per_second, avg_write_row_size_bytes, regionPricing) => {
    return calculateOnDemandWriteUnitsPerMonth(writes_per_second, avg_write_row_size_bytes) * regionPricing.writeRequestPrice;
};

/**
 * On-demand monthly cost for TTL deletes.
 * @param {number} ttls_per_second - TTL operations/s.
 * @param {number} avg_write_row_size_bytes - Bytes.
 * @param {Object} regionPricing
 * @returns {number}
 */
export const calculateTtlUnitsPerMonthCost = (ttls_per_second, avg_write_row_size_bytes, regionPricing) => {
    return calculateOnDemandTtlUnitsPerMonth(ttls_per_second, avg_write_row_size_bytes) * regionPricing.ttlDeletesPrice;
};

/**
 * Convenience: monthly read units assuming 1KB rows.
 * @param {number} reads_per_second - Read operations per second.
 * @returns {number}
 */
export const calcualteReadUnitsPerMonthUsing1KBRowSize = (reads_per_second) => {
    return calculateReadUnitsPerMonth(reads_per_second, 1024);
};

/**
 * Convenience: monthly write units assuming 1KB rows.
 * @param {number} writes_per_second - WU/s.
 * @returns {number}
 */
export const calcualteWriteUnitsPerMonthUsing1KBRowSize = (writes_per_second) => {
    return calculateWriteUnitsPerMonth(writes_per_second, 1024);
};

/**
 * Convenience: monthly TTL units assuming 1KB rows.
 * @param {number} ttls_per_second - TTL operations/s.
 * @returns {number}
 */
export const calcualteTtlUnitsPerMonthUsing1KBRowSize = (ttls_per_second) => {
    return calculateTtlUnitsPerMonth(ttls_per_second, 1024);
};

/**
 * Monthly read units at on-demand scale.
 * @param {number} reads_per_second - Read operations per second.
 * @param {number} avg_read_row_size_bytes - Bytes.
 * @returns {number}
 */
export const calcualteOnDemandReadUnitsPerMonth = (reads_per_second, avg_read_row_size_bytes) => {
    return reads_per_second * calculateReadUnitsPerOperation(avg_read_row_size_bytes) * SECONDS_PER_MONTH;
};

/**
 * Monthly write units at on-demand scale.
 * @param {number} writes_per_second - Write operations per second.
 * @param {number} avg_write_row_size_bytes - Bytes.
 * @returns {number}
 */
export const calculateOnDemandWriteUnitsPerMonth = (writes_per_second, avg_write_row_size_bytes) => {
    return writes_per_second * calculateWriteUnitsPerOperation(avg_write_row_size_bytes) * SECONDS_PER_MONTH;
};

/**
 * Monthly TTL delete units at on-demand scale.
 * @param {number} ttls_per_second - TTL operations/s.
 * @param {number} avg_write_row_size_bytes - Bytes.
 * @returns {number}
 */
export const calculateOnDemandTtlUnitsPerMonth = (ttls_per_second, avg_write_row_size_bytes) => {
    return ttls_per_second * calculateTtlUnitsPerOperation(avg_write_row_size_bytes) * SECONDS_PER_MONTH;
};

/**
 * Monthly read units calculation.
 * @param {number} reads_per_second - Read operations per second.
 * @param {number} avg_read_row_size_bytes - Average row size in bytes.
 * @returns {number}
 */
export const calculateReadUnitsPerMonth = (reads_per_second, avg_read_row_size_bytes) => {
    return reads_per_second * calculateReadUnitsPerOperation(avg_read_row_size_bytes) * SECONDS_PER_MONTH;
};

/**
 * Monthly write units calculation.
 * @param {number} writes_per_second - Write operations per second.
 * @param {number} avg_write_row_size_bytes - Average row size in bytes.
 * @returns {number}
 */
export const calculateWriteUnitsPerMonth = (writes_per_second, avg_write_row_size_bytes) => {
    return writes_per_second * calculateWriteUnitsPerOperation(avg_write_row_size_bytes) * SECONDS_PER_MONTH;
};

/**
 * Monthly TTL units calculation.
 * @param {number} ttls_per_second - TTL operations per second.
 * @param {number} avg_write_row_size_bytes - Average row size in bytes.
 * @returns {number}
 */
export const calculateTtlUnitsPerMonth = (ttls_per_second, avg_write_row_size_bytes) => {
    return ttls_per_second * calculateTtlUnitsPerOperation(avg_write_row_size_bytes) * SECONDS_PER_MONTH;
};

/**
 * Write units per operation given row size.
 * @param {number} avg_write_row_size_bytes - average row is in bytes.
 * @returns {number}
 */
export const calculateWriteUnitsPerOperation = (avg_write_row_size_bytes) => {
    return Math.ceil(avg_write_row_size_bytes / 1024);
};

/**
 * Read units per operation given row size.
 * @param {number} avg_read_row_size_bytes - Bytes.
 * @returns {number}
 */
export const calculateReadUnitsPerOperation = (avg_read_row_size_bytes) => {
    return Math.ceil(avg_read_row_size_bytes / 4096);
};

/**
 * TTL delete units per operation given row size.
 * @param {number} avg_write_row_size_bytes - Bytes.
 * @returns {number}
 */
export const calculateTtlUnitsPerOperation = (avg_write_row_size_bytes) => {
    return Math.ceil(avg_write_row_size_bytes / 1024);
};

export default calculatePricingEstimate;


