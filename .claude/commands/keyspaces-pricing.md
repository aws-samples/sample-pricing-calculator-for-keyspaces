# Amazon Keyspaces pricing — section guide (formulas)

Use this command when explaining or estimating Keyspaces monthly cost from **high-level throughput and storage**. Align numbers with `src/calculator/PricingFormulas.ts` and the fuller narrative in `references/keyspaces-pricing/keyspaces-pricing-rules.md`.

For interactive estimates and tooling, invoke the **`amazon-keyspaces-skill`** (`.claude/skills/amazon-keyspaces-skill/SKILL.md`).

---

## Shared constants (this repo’s estimator)

These match `PricingFormulas.ts` / `ParsingHelpers.ts`:

- `SECONDS_PER_MONTH = (365 / 12) × 86400` — used for **on-demand** request units per month.
- `HOURS_PER_MONTH = (365 / 12) × 24` — used for **provisioned** capacity billed per hour.
- **Provisioned headroom:** reads and writes both use `target_utilization = 0.70` (divide by 0.70 so billed capacity exceeds steady-state ops/sec).

**High-level example inputs** (per Region, single keyspace slice — adjust for multi-Region replication separately):

| Symbol | Example |
|--------|---------|
| `R` | 100 reads/sec |
| `W` | 100 writes/sec |
| `S_gb` | stored data for billing basis in GB (e.g. 10 TB ≈ `10 × 1024` GiB-style GB, or use your TB→GB convention consistently with AWS docs) |
| `B_row` | average row/payload size in **bytes** (reads and writes often share one average in the form-based estimator) |
| `T` | TTL-driven deletes/sec (0 if no TTL workload) |
| `P_on-demand read`, `P_on-demand write` | $ per RRU / WRU from regional price table (`mcs.json` keys `MCS-ReadUnits`, `MCS-WriteUnits`) |
| `P_rcu_hr`, `P_wcu_hr` | $ per RCU-hour / WCU-hour (`Provisioned Read Units`, `Provisioned Write Units`) |
| `P_storage`, `P_pitr` | $ per GB-month table storage / PITR backup storage |
| `P_ttl` | $ per TTL delete unit (`Time to Live`) |

**Unit rounding (LOCAL_QUORUM-style, same as code):**

- Write units per operation: `WRU_per_op = ceil(B_row / 1024)`  
- Read units per operation (strong / LOCAL_QUORUM): `RRU_per_op = ceil(B_row / 4096)`  
- TTL units per operation: `TTL_per_op = ceil(B_row / 1024)`

---

## 1. Pricing dimensions

**Description:** Regional list prices; capacity mode (**on-demand** vs **provisioned**); reads halved for LOCAL_ONE vs LOCAL_QUORUM on the same bytes; optional **Database Savings Plans** (replace list unit rates when modeling discounts — see `PricingData` / `savingsPlanMap` in code).

**Monthly subtotal (conceptual):**

```text
monthly_total ≈ monthly_reads_cost + monthly_writes_cost + monthly_ttl_cost + monthly_storage + monthly_pitr (+ one-off restore + streams, if applicable)
```

Pick **either** on-demand **or** provisioned formulas below for throughput (compare both modes like `calculatePricingEstimate`).

---

## 2. Write capacity (throughput)

**Description:** Each logical write consumes WRUs (on-demand) or WCUs (provisioned) proportional to payload size, rounded up per KB.

**On-demand — monthly write charge:**

```text
monthly_writes_on_demand = W × WRU_per_op × SECONDS_PER_MONTH × P_on-demand write
```

**Provisioned — monthly write charge (70% utilization assumption):**

```text
monthly_writes_provisioned = (W × WRU_per_op × HOURS_PER_MONTH × P_wcu_hr) / 0.70
```

**Numeric sanity example:** `W = 100`, `B_row = 1024` ⇒ `WRU_per_op = 1`.  
On-demand scales with `100 × SECONDS_PER_MONTH` billable WRUs before multiplying price.

---

## 3. Read capacity (throughput)

**Description:** Strong consistency reads use 4 KB rounding per RRU/RCU; LOCAL_ONE-style reads bill at **half** the RRUs/RCUs for the same scanned bytes (this repo’s UI shows eventual as half of strong via `mapPricingEstimateToKeyspacesTable`).

**On-demand — monthly read charge (LOCAL_QUORUM / “strong”):**

```text
monthly_reads_on_demand_strong = R × RRU_per_op × SECONDS_PER_MONTH × P_on-demand read

monthly_reads_on_demand_eventual ≈ monthly_reads_on_demand_strong × 0.5   // LOCAL_ONE-style
```

**Provisioned — monthly read charge:**

```text
monthly_reads_provisioned_strong = (R × RRU_per_op × HOURS_PER_MONTH × P_rcu_hr) / 0.70

monthly_reads_provisioned_eventual ≈ monthly_reads_provisioned_strong × 0.5
```

**Numeric sanity example:** `R = 100`, `B_row = 1024` ⇒ `RRU_per_op = 1`.

---

## 4. TTL delete units

**Description:** Expired-row handling consumes TTL delete units (KB-rounded, analogous to write rounding in the estimator).

**Monthly TTL charge:**

```text
monthly_ttl = T × TTL_per_op × SECONDS_PER_MONTH × P_ttl
```

(`calculateTtlUnitsPerMonthCost` in `PricingFormulas.ts`.)

---

## 5. Backup (PITR)

**Description:** Continuous backup bills monthly on backup storage for tables with PITR enabled — modeled as **single-replica uncompressed GB × rate** when the backup flag is on (`calculateBackupCostPerMonth`).

**Monthly PITR charge:**

```text
monthly_pitr = S_gb × P_pitr    // only if PITR enabled for that table scope
```

Storage basis should match how you define `S_gb` (the estimator uses `uncompressed_single_replica_gb`).

---

## 6. Restore

**Description:** Restore from continuous backup is billed **per restore operation** by amount restored — **not** a steady monthly line item unless you amortize scenarios yourself.

**Pseudo formula (per restore):**

```text
restore_charge_once ≈ restored_gb × P_restore_per_gb    // use current AWS pricing for restore
```

---

## 7. CDC / Keyspaces Streams

**Description:** Stream consumers use **streams read request units**, metered separately from table reads (`GetRecords` billed per AWS streams rules — up to 1 MB per streams RRU in pricing examples).

**Monthly streams cost (conceptual):**

```text
monthly_streams ≈ f(stream_poll_rate, payload_mb, P_streams_rru)   // not implemented in PricingFormulas.ts
```

---

## 8. Common mistakes (no single formula)

**Description:** Billing follows **bytes processed**, not only rows returned; scans, `LIMIT`, and paging size can dominate RRUs; multi-Region writes multiply per Region; static-column writes can exceed naive `ceil(row_kb)` — see `keyspaces-pricing-rules.md` §8–§9.

---

## 9. Feature-specific metering (Developer Guide)

**Description:** Encoded row size, range predicates, LWT, static columns, and multi-Region replication planning adjust effective `WRU_per_op`, `RRU_per_op`, or Region fan-out — qualitative rules in `keyspaces-pricing-rules.md` §9; **not** fully simulated in `PricingFormulas.ts`.

---

## 10. End-to-end monthly estimate (this calculator)

**Description:** `calculatePricingEstimate` sums per keyspace, per datacenter: provisioned path uses `calculateProvisionedCapacityTotalMonthlyCostWithAggregates`; on-demand path uses `calculateOnDemandCapcityTotalMonthlyCostWithAggregates`.

**Combined pseudo formula (one Region, one keyspace, LOCAL_QUORUM reads, PITR optional):**

```text
// Throughput: compare modes side-by-side like calculatePricingEstimate; production tables pick one mode.

monthly_on_demand_core =
    R × RRU_per_op × SECONDS_PER_MONTH × P_on-demand read
  + W × WRU_per_op × SECONDS_PER_MONTH × P_on-demand write
  + T × TTL_per_op × SECONDS_PER_MONTH × P_ttl
  + S_gb × P_storage
  + (pitr_enabled ? S_gb × P_pitr : 0)

monthly_provisioned_core =
    (R × RRU_per_op × HOURS_PER_MONTH × P_rcu_hr) / 0.70
  + (W × WRU_per_op × HOURS_PER_MONTH × P_wcu_hr) / 0.70
  + T × TTL_per_op × SECONDS_PER_MONTH × P_ttl
  + S_gb × P_storage
  + (pitr_enabled ? S_gb × P_pitr : 0)
```

**10 TB storage example:** Set `S_gb = 10 × 1024` (if using binary TB→GB) or `10 × 1000` if matching decimal marketing TB — stay consistent with your GB definition for `P_storage`.

---

### References

- Implementation: `src/calculator/PricingFormulas.ts`
- Rules narrative: `references/keyspaces-pricing/keyspaces-pricing-rules.md`
- Regional JSON: `src/calculator/data/mcs.json`
