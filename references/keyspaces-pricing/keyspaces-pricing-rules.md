# Keyspaces Pricing Rules Reference

This note summarizes how Amazon Keyspaces bills workloads (official pricing), feature-specific metering called out in the Developer Guide ([Amazon Keyspaces pricing](https://aws.amazon.com/keyspaces/pricing/), [Developer Guide PDF](./AmazonKeyspacesDocs.pdf)), and how this repo’s Cassandra-derived estimator maps signals into those rules (`src/calculator/PricingFormulas.ts`; constants in `src/calculator/Constants.js`).

Use `{region}` wherever AWS publishes list prices per Region (this calculator resolves `{region}` from `src/calculator/data/mcs.json` keys such as `US West (Oregon)`).

---

## Table of Contents

1. [Pricing Dimensions](#1-pricing-dimensions)
2. [Write Capacity](#2-write-capacity)
3. [Read Capacity](#3-read-capacity)
4. [TTL Delete Units](#4-ttl-delete-units)
5. [Backup](#5-backup)
6. [Restore](#6-restore)
7. [CDC Streams](#7-cdc-streams)
8. [Common Mistakes](#8-common-mistakes)
9. [Feature-specific metering (Developer Guide)](#9-feature-specific-metering-developer-guide)
10. [Calculator rules from Cassandra (`PricingFormulas.ts`)](#10-calculator-rules-from-cassandra-pricingformulasts)

---

## 1. Pricing Dimensions

- Regional pricing. Charges vary by AWS Region; model prices as functions of `{region}` (see the live pricing tables on [Amazon Keyspaces pricing](https://aws.amazon.com/keyspaces/pricing/)).
- Throughput capacity mode (per table).
  - On-demand/Pay-per-request: billed per RRU (read request unit) and WRU (write request unit).
  - Provisioned: billed per RCU (read capacity unit) and WCU (write capacity unit), expressed per hour for sustained provisioned capacity; optional auto scaling adjusts WCUs/RCUs.
- Database Savings Plans. Separate flexible commitment pricing (`$/hour` of eligible usage, 1-year term). Official overview links from [Amazon Keyspaces pricing](https://aws.amazon.com/keyspaces/pricing/) (eligible SKUs are on AWS Database Savings Plans pricing—not duplicated here).
- Consistency affects reads only. All writes use LOCAL_QUORUM. Reads may use LOCAL_QUORUM, LOCAL_ONE, or ONE (supported subset varies by documentation—pricing halves RRUs/RCUs for LOCAL_ONE vs LOCAL_QUORUM on the same payload size).
- Billable storage. Continuous measurement of table size (no advance provisioning).
- PITR (continuous backups). Billed on size of tables with PITR enabled, monitored through the month.
- Restore. One-time charge by amount of data restored per restore operation.
- TTL deletes. Metered in TTL delete units (per KB of affected row data).
- Keyspaces Streams. Consumer reads billed as streams read request units (distinct from table RRUs).
- Data transfer. No inbound charge from the internet; same-Region traffic between Keyspaces and other AWS services is typically $0; cross-Region charges apply where documented (Multi-Region Replication has specific transfer rules on the pricing page).

---

## 2. Write Capacity

From [Amazon Keyspaces pricing](https://aws.amazon.com/keyspaces/pricing/) (single-Region keyspace):

- Write capacity unit (WCU) is used when speaking of write capacity in provisioned capacity mode. 1 WCU provides 1 Write capacity per second for one hour. You can consume 1 WCU by setting provisioned capacity to 1 WCU. The application can consume 1 write capacity (1024) once per second for 3600 seconds.  
- Write request unit (WRU) is used when speaking of write capacity in On-Demand.
- Single-Region: 1 WRU (on-demand) / 1 WCU per second (provisioned) covers up to 1 KB per row or 1024 bytes at LOCAL_QUORUM. Larger rows round up in 1 KB increments (e.g. 3 KB row ⇒ 3 WRUs per write). Rows are rounded up (e.g 1.1 KB rounds up to WRUs). 
- Multi-Region keyspace: Same per-row WRU/WCU math as single-Region, but each write is billed in every Region in the multi-Region keyspace (e.g. 3 KB row in two Regions ⇒ 3 × 2 = 6 WRUs for that logical write).
- Static columns / mixed static + non-static writes: billing can exceed the naive “row size in KB” formula—see [§9](#9-feature-specific-metering-developer-guide).
Official pricing also notes additional write operations when both static and non-static data appear in the same write ([pricing page](https://aws.amazon.com/keyspaces/pricing/), cross-linked “Learn more” in AWS docs).

### Python example (aligned with `PricingFormulas.ts`; provisioned hours rounded up)

High-level inputs: sustained logical **writes per second**, average billable **row size in bytes** (LOCAL_QUORUM 1 KB rounding), **region count** for multi-Region replication (`regions_mr == 1` for a single-Region keyspace), regional **list prices** from `src/calculator/data/mcs.json`, and optional **`total_seconds`** windows (defaults to one billing-month horizon—same `SECONDS_PER_MONTH` as `PricingFormulas.ts`; use `3600` for a one-hour sanity check, etc.). **On-demand** scales WRUs with exact elapsed seconds. **Provisioned** converts elapsed time to **whole billed hours** via `ceil(total_seconds / 3600)` (partial clock hours count as a full hour for this sketch).

```python
import math

SECONDS_PER_MONTH = (365 / 12) * 86400  # same as Constants / PricingFormulas
TARGET_UTILIZATION = 0.70


def write_throughput_cost_on_demand_usd(
    writes_per_second: float,
    avg_row_size_bytes: float,
    regions_mr: int,
    price_per_wru_usd: float,
    *,
    total_seconds: float = SECONDS_PER_MONTH,
) -> float:
    """WRUs scale with elapsed seconds; price_per_wru_usd from MCS-WriteUnits."""
    units = math.ceil(avg_row_size_bytes / 1024)
    wrus_per_second = writes_per_second * units * regions_mr
    return wrus_per_second * total_seconds * price_per_wru_usd


def write_throughput_cost_provisioned_usd(
    writes_per_second: float,
    avg_row_size_bytes: float,
    regions_mr: int,
    price_per_wcu_hour_usd: float,
    *,
    total_seconds: float = SECONDS_PER_MONTH,
    target_utilization: float = TARGET_UTILIZATION,
) -> float:
    """WCU-hours = WCUs/sec × billed_hours × price; billed_hours = ceil(elapsed clock hours)."""
    units = math.ceil(avg_row_size_bytes / 1024)
    wcus_per_second = writes_per_second * units * regions_mr
    billed_hours = math.ceil(total_seconds / 3600.0)
    return wcus_per_second * billed_hours * price_per_wcu_hour_usd / target_utilization
```

**Repo mapping:** Unit math matches `calculateWriteUnitsPerOperation` and provisioned `calculateProvisionedWriteCostPerMonth`; **this snippet differs** by rounding provisioned duration **up** to whole hours (`ceil(seconds/3600)`). `PricingFormulas.ts` uses fractional `HOURS_PER_MONTH` for a full estimator month (no hourly ceiling). Multi-Region replication (`regions_mr > 1`) is **not** inferred automatically in that module—multiply by Region count when modeling MRK yourself.


---

## 3. Read Capacity

From [Amazon Keyspaces pricing](https://aws.amazon.com/keyspaces/pricing/):

- 1 RRU / 1 RCU per second covers up to 4 KB of data read with LOCAL_QUORUM.
- LOCAL_ONE (and pricing examples using ONE): half an RRU/RCU per 4 KB (same payload costs half versus LOCAL_QUORUM).
- Aggregates: Multi-row results bill on total bytes processed for the read (not “per returned row” only).

Estimator display: this repo’s UI maps “strong” reads to LOCAL_QUORUM-priced totals and shows half for “eventual” (LOCAL_ONE) using simple division—see `mapPricingEstimateToKeyspacesTable` in `PricingFormulas.ts`.

---

## 4. TTL Delete Units

From [Amazon Keyspaces pricing](https://aws.amazon.com/keyspaces/pricing/):

- TTL-driven deletes/updates consume TTL delete units: 1 unit per KB per row affected (ceil semantics—e.g. 2.5 KB row ⇒ 3 TTL deletes).

---

## 5. Backup

- PITR: ongoing monthly charge based on size of each table with PITR enabled until disabled ([pricing page](https://aws.amazon.com/keyspaces/pricing/)).

---

## 6. Restore

- Restore from continuous backup: charged by total restored data size per restore request ([pricing page](https://aws.amazon.com/keyspaces/pricing/)).

---

## 7. CDC Streams

From [Amazon Keyspaces pricing](https://aws.amazon.com/keyspaces/pricing/):

- Reading change streams is billed in read request units as streams read request units.
- Each `GetRecords` call is billed as one streams read request unit for up to 1 MB of stream payload (separate meter from table reads).

Implementation details (retention, schema, permissions) are in the Developer Guide PDF (`./AmazonKeyspacesDocs.pdf`).

---

## 8. Common Mistakes

- Assuming billing equals rows returned. Range queries, LIMIT, ALLOW FILTERING / table scans, and internal bounds evaluation can read more bytes than the client receives—capacity follows data processed.
- Ignoring LOCAL_ONE vs LOCAL_QUORUM when comparing Cassandra QUORUM/ONE workloads to Keyspaces SKUs.
- Multi-Region writes counted once. Logical writes multiply by Region count for WRUs/WCUs.
- Static columns. Treating mixed static + non-static writes as a single ceil(row_kb) write—AWS documents separate metering and a 2× + 2× provisioned planning formula (see §9).
- LWT “failed” conditions still consume writes. Failed conditional writes still consume WRUs/WCUs based on evaluated row size; TTL tables without client-side timestamps can add read consumption on failed IF EXISTS / IF NOT EXISTS paths (Developer Guide PDF).
- LIMIT without PAGE SIZE. Default 1 MB read budget per page can massively inflate RRUs for `LIMIT 1`; tune driver PAGE SIZE (PDF).

---

## 9. Feature-specific metering (Developer Guide)

Summarized from `./AmazonKeyspacesDocs.pdf` (capacity estimation chapters; aligns with [pricing page](https://aws.amazon.com/keyspaces/pricing/) themes):

| Feature | Metering / planning rule |
| -------- | ------------------------- |
| Encoded row size / billing weight | Partition keys & clustering keys contribute metadata; partition key and clustering column values are stored twice for indexing—row-size estimates must include that effect (PDF “Estimate the encoded size of columns”). |
| Range / inequality reads | RRUs/RCUs based on rows read to satisfy the predicate, including boundary rows that are not returned. |
| LIMIT queries | Without a small PAGE SIZE, reads may scan up to 1 MB before filters/`LIMIT` apply—capacity scales with bytes scanned, not rows returned. |
| Table scans / ALLOW FILTERING | Scans bill empty partitions too (example: three data rows + three empty partitions ⇒ extra RRUs for empty partitions). |
| LWT (conditional writes) | No extra LWT surcharge vs normal writes for successful operations at LOCAL_QUORUM; failed condition checks still consume WRUs/WCUs from evaluated row size (nonexistent row ⇒ example baseline of 1 WRU/WCU in docs); monitor `ConditionalCheckFailed`. |
| LWT + TTL (no client-side timestamps) | Failed IF EXISTS / IF NOT EXISTS: extra RRUs/RCUs—if row exists, based on existing row size; if missing, 1 RRU/RCU. Successful writes bill WRUs/WCUs from new row size. |
| Static columns | Static vs non-static writes are metered separately; a statement touching both is modeled as two write operations. Provisioned planning formula for mixed writes: `2 × WCUs(nonstatic) + 2 × WCUs(static)` (equivalent WRU framing on-demand). Example in PDF: 25.5 KB nonstatic + 1.5 KB static ⇒ 56 WCUs (`2×26 + 2×2`). Reads combining static + non-static bill on aggregate processed size. |
| Multi-Region tables | Provisioned: each Region must sustain sum of write rates originating in all Regions (every local write is replicated to each replica). Example: 5 + 10 + 5 writes/s across three Regions ⇒ 20 WCUs required in each Region; total billed WCUs across Regions equals per-Region requirement × Region count (PDF example ⇒ 60 WCUs billed across three Regions). Capacity mode changes apply to all replicas. |

---

## 10. Calculator rules from Cassandra (`PricingFormulas.ts`)

The repo does not parse every AWS feature above; it infers average row sizes and ops/sec from Cassandra diagnostics and applies Keyspaces-like unit math. Important mappings:

### Regional price lookup

`getRegionPricing({region})` reads `src/calculator/data/mcs.json`:

| Concept | JSON product key |
| -------- | ----------------- |
| On-demand read unit price | `MCS-ReadUnits` |
| On-demand write unit price | `MCS-WriteUnits` |
| Provisioned read per hour | `Provisioned Read Units` |
| Provisioned write per hour | `Provisioned Write Units` |
| Storage ($/GB-month) | `AmazonMCS - Indexed DataStore per GB-Mo` |
| PITR backup storage ($/GB-month) | `Point-In-Time-Restore PITR Backup Storage per GB-Mo` |
| TTL deletes ($/unit) | `Time to Live` |

Database Savings Plan discounts use `src/calculator/PricingData` (`savingsPlanMap[{region}]`) when present.

### Unit formulas (LOCAL_QUORUM-style throughput)

For each keyspace aggregate:

- Write units per operation: `ceil(avg_write_row_size_bytes / 1024)` → matches 1 KB WRU/WCU rounding.
- Read units per operation: `ceil(avg_read_row_size_bytes / 4096)` → matches 4 KB RRU/RCU rounding.
- TTL deletes per operation: `ceil(avg_row_size_bytes / 1024)` (TTL rate uses the same average row size driver as writes in aggregates).

### Cassandra aggregation → Keyspaces-oriented rates

`buildCassandraLocalSet` / `getKeyspaceCassandraAggregate`:

- Skips keyspaces in `system_keyspaces` (`Constants.js`).
- Replication factor from schema when present, else default `REPLICATION_FACTOR = 3`.
- writes_per_second: scales monthly write totals by `number_of_nodes / replication_factor` (per-logical-replica view).
- reads_per_second: scales by `number_of_nodes / (replication_factor - 1)` when `replication_factor > 1`, else `1`—Heuristic for Cassandra read paths vs replicas (not an AWS-documented formula).
- Storage (`uncompressed_single_replica_gb`): sums uncompressed bytes per node / RF across sampled nodes.
- TTL rate (`ttls_per_second`): for tables with TTL evidence (`default_time_to_live` marker or prepared-statement TTL hints), 100% of that table’s write rate is treated as TTL-delete throughput for costing.

### Cost assembly

- On-demand throughput: `(ops/sec × units_per_op × SECONDS_PER_MONTH) × unit_price` (`SECONDS_PER_MONTH = (365/12)×86400`).
- Provisioned throughput: `(ops/sec × units_per_op × HOURS_PER_MONTH × price_per_hour) / target_utilization` with `HOURS_PER_MONTH = (365/12)×24` and default `target_utilization = 0.70` for reads and writes.
- Storage & PITR: `uncompressed_single_replica_gb × price_per_gb` when backup flag set on aggregates.
- Multi-DC bundles: `scaleKeyspaceAggregates(factor)` multiplies rates/storage when one diagnostic bundle represents multiple DCs.

### Not modeled (gap vs AWS docs §9)

Static-column 2×+2× write amplification, LWT failure reads/writes, stream `GetRecords` metering, restore charges, cross-Region data transfer, burst/pre-warmed throughput, logged batch overhead, and work-dependent read amplification (scans, LIMIT page size) are not subtracted or simulated—row sizes and op counts are averages from Cassandra stats.

---

### References

- [Amazon Keyspaces (for Apache Cassandra) pricing](https://aws.amazon.com/keyspaces/pricing/)
- Amazon Keyspaces Developer Guide PDF (local): `./AmazonKeyspacesDocs.pdf`
- Estimator implementation: `src/calculator/PricingFormulas.ts`, `src/calculator/Constants.js`, `src/calculator/data/mcs.json`
