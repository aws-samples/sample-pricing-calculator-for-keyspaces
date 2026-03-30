# Amazon Keyspaces Pricing Calculator

Calculate the estimated monthly cost for Amazon Keyspaces (for Apache Cassandra).

## Instructions

The user may provide inputs as arguments (e.g. `/keyspaces-pricing us-east-1 1000 500 4096 100 0 true`) or you should ask them for:

1. **AWS Region** (e.g. `us-east-1` or `US East (N. Virginia)`)
2. **Reads per second** (average)
3. **Writes per second** (average)
4. **Average row size in bytes** (used for both reads and writes; default: 1024)
5. **Storage size in GB** (uncompressed, single replica)
6. **TTL deletes per second** (0 if TTL not used)
7. **PITR backup enabled?** (true/false)

## Step 1 — Read the pricing formulas

Read `src/utils/PricingFormulas.ts`. This is the authoritative source for all pricing logic. Use the exported functions directly — do **not** duplicate or re-derive the formulas. Key functions you will use:

- `calculateWriteUnitsPerOperation(avg_write_row_size_bytes)`
- `calculateReadUnitsPerOperation(avg_read_row_size_bytes)`
- `calculateTtlUnitsPerOperation(avg_write_row_size_bytes)`
- `calculateOnDemandWriteUnitsPerMonth(writes_per_second, avg_write_row_size_bytes)`
- `calcualteOnDemandReadUnitsPerMonth(reads_per_second, avg_read_row_size_bytes)`
- `calculateOnDemandTtlUnitsPerMonth(ttls_per_second, avg_write_row_size_bytes)`
- `calculateOnDemandWriteUnitsPerMonthCost(writes_per_second, avg_write_row_size_bytes, writePrice)`
- `calculateOnDemandReadUnitsPerMonthCost(reads_per_second, avg_read_row_size_bytes, readPrice)`
- `calculateTtlUnitsPerMonthCost(ttls_per_second, avg_write_row_size_bytes, ttlDeletesPrice)`
- `calculateProvisionedWriteCostPerMonth(writes_per_second, avg_write_row_size_bytes, writeRequestPricePerHour, target_utilization)`
- `calculateProvisionedReadCostPerMonth(reads_per_second, avg_read_row_size_bytes, readRequestPricePerHour, target_utilization)`
- `calculateStorageCostPerMonth(uncompressed_single_replica_gb, storagePricePerGB)`
- `calculateBackupCostPerMonth(uncompressed_single_replica_gb, pitrPricePerGB)`
- `calculateOnDemandCapcityTotalMonthlyCostWithAggregates(...)`
- `calculateProvisionedCapacityTotalMonthlyCostWithAggregates(...)`

Also note the constants used: `SECONDS_PER_MONTH` and `HOURS_PER_MONTH` (from `Constants.js` and `ParsingHelpers`), and `target_utilization = 0.70`.

## Step 2 — Look up region pricing

Read `src/data/mcs.json`. The file has a `regions` key whose sub-keys are **long region names** (e.g. `"US West (Oregon)"`).

If the user supplied a short region code (e.g. `us-east-1`), read `src/data/regions.json` to resolve it to the long name first.

Extract these price fields from `mcs.json` for the resolved region (matching the `getRegionPricing` function in `PricingFormulas.ts`):

| mcs.json key | Maps to |
|---|---|
| `MCS-ReadUnits` → `price` | `readRequestPrice` |
| `MCS-WriteUnits` → `price` | `writeRequestPrice` |
| `Provisioned Write Units` → `price` | `writeRequestPricePerHour` |
| `Provisioned Read Units` → `price` | `readRequestPricePerHour` |
| `AmazonMCS - Indexed DataStore per GB-Mo` → `price` | `storagePricePerGB` |
| `Point-In-Time-Restore PITR Backup Storage per GB-Mo` → `price` | `pitrPricePerGB` |
| `Time to Live` → `price` | `ttlDeletesPrice` |

## Step 3 — Check for Savings Plans

Read `src/components/PricingData.js` (the `savingsPlansMap`). If a savings plan exists for the resolved long region name, compute a second set of costs using the savings plan rates:

- `WriteRequestUnits` → `rate` replaces `writeRequestPrice`
- `ReadRequestUnits` → `rate` replaces `readRequestPrice`
- `WriteCapacityUnitHrs` → `rate` replaces `writeRequestPricePerHour`
- `ReadCapacityUnitHrs` → `rate` replaces `readRequestPricePerHour`

If no savings plan exists for the region, omit the savings plan column from the output.

## Step 4 — Apply the formulas and compute costs

Using the functions from `PricingFormulas.ts` and the prices from Step 2, compute:

- On-demand: reads, writes, TTL deletes, storage, backup → total
- Provisioned (70% target utilization): reads, writes, TTL deletes, storage, backup → total
- Eventual consistency reads = strong consistency reads / 2 (both modes)
- If savings plan exists: repeat on-demand and provisioned with savings plan rates

## Step 5 — Display results

Present a clear pricing summary table. Format dollar amounts to 2 decimal places.

```
## Amazon Keyspaces Pricing Estimate
Region: US East (N. Virginia) (us-east-1)

### Inputs
| Parameter | Value |
|---|---|
| Reads/sec | 1,000 |
| Writes/sec | 500 |
| Avg row size | 4,096 bytes |
| Storage | 100 GB |
| TTL deletes/sec | 0 |
| PITR backup | Yes |

### Monthly Cost Estimate
| Cost Component | On-Demand | Provisioned | On-Demand (Savings Plan) | Provisioned (Savings Plan) |
|---|---|---|---|---|
| Reads (strong consistency) | $X.XX | $X.XX | $X.XX | $X.XX |
| Reads (eventual consistency) | $X.XX | $X.XX | $X.XX | $X.XX |
| Writes | $X.XX | $X.XX | $X.XX | $X.XX |
| TTL Deletes | $X.XX | $X.XX | $X.XX | $X.XX |
| Storage | $X.XX | $X.XX | $X.XX | $X.XX |
| PITR Backup | $X.XX | $X.XX | $X.XX | $X.XX |
| **TOTAL** | **$X.XX** | **$X.XX** | **$X.XX** | **$X.XX** |
```

Omit the savings plan columns if no savings plan is available for the region.
