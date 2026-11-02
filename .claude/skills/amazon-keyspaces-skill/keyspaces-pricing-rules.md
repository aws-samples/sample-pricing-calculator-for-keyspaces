# Amazon Keyspaces pricing and conversion rules (as implemented)

The rules this calculator applies, taken from the code, not from the AWS pricing page. When this file and the code disagree, the code wins: fix this file.

This file is a **reference**. Estimates always come from the TypeScript scripts. Use the file to answer pricing questions, to explain an estimate alongside its `--explain` lines, and when developing a feature. To add or change a rule:
1. Change the TypeScript.
2. Add an `--explain` line.
3. Add a test in `transform-agent/tests/` (Part 5 is pinned by one).
4. Update this file. To see these rules applied to a specific estimate, run `calculate.ts`, `estimate-workload.ts` or `parse-cassandra.ts` with `--explain`. It writes out every formula with the estimate's numbers (`formulas` in the JSON).

The file has five parts. Parts 1 and 5 overlap on purpose: they're where pricing and the conversions meet.

| Part | Scope | Read it when |
|---|---|---|
| [1. Shared: the Keyspaces workload](#part-1--shared-the-keyspaces-workload) | The workload fields pricing needs, and the constants every side uses | Always |
| [2. Keyspaces pricing](#part-2--keyspaces-pricing-any-source) | Workload → monthly cost. Independent of the source. | Every estimate |
| [3. Converting other sources](#part-3--converting-other-sources--keyspaces-workload) | Another database, monitoring data, a SQL model option or round numbers → workload | Modes 1 and 4, any non-Cassandra input |
| [4. Apache Cassandra conversion](#part-4--apache-cassandra--keyspaces-workload) | Cassandra diagnostics → workload | Mode 2 |
| [5. End to end](#part-5--end-to-end-two-sources--one-workload--price) | The same workload from Cassandra and from PostgreSQL tables, priced once | Explaining an estimate |

Conversion (Parts 3 and 4) never sets a price. Pricing (Part 2) never looks at the source.

| Area | Source |
|---|---|
| Pricing formulas, cost assembly, savings plans | `calculatePricingEstimate` and the `calculate*` formulas in `src/calculator/PricingFormulas.ts` |
| Cassandra conversion | `buildCassandraLocalSet`, `getKeyspaceCassandraAggregate` and the multi-datacenter helpers in `PricingFormulas.ts`; `scripts/parse-cassandra.ts`; parsers in `src/calculator/ParsingHelpers.ts` |
| Price data | `src/calculator/data/mcs.json`, `savings-plans.json`, `regions.json` (refreshed by `tools/get-pricing.sh`) |
| Other-source conversion | `scripts/estimate-workload.ts` (workload spec), `scripts/calculate.ts` (round numbers) |
| Self-managed TCO | `rollupCassandraTco` in `src/calculator/CreatePDFReport.ts` |
| CloudFormation capacity | `deriveTemplateOptionsFromEstimate` in `src/calculator/CloudFormationGenerator.ts` |

---

## Part 1 — Shared: the Keyspaces workload

### 1.1 Workload fields

Pricing (Part 2) takes one record per **keyspace per Region**. Every source produces these fields: other sources through Part 3, Cassandra through Part 4. In the code, a Region replica is called a "datacenter" (`KeyspaceAggregate` in `PricingFormulas.ts`).

| Field | Meaning | From a workload spec (Part 3) | From Cassandra (Part 4) |
|---|---|---|---|
| `reads_per_second` | Logical client reads/s in this Region, i.e. requests sent to Keyspaces, not replica operations | Σ table reads ÷ request copies | sampled read rate × N ÷ (RF − 1) |
| `writes_per_second` | Logical client writes/s in this Region. For a multi-Region keyspace, each Region carries the full replicated write rate. | Σ table writes ÷ request copies | sampled write rate × N ÷ RF |
| `ttls_per_second` | Rows expired by TTL per second | Σ table TTL deletes | writes of TTL tables |
| `avg_read_row_size_bytes` / `avg_write_row_size_bytes` | Average row size per read / write | table row size, weighted by operations | row-size sampler, weighted by operations |
| `avg_rows_per_read` | Average rows returned per read request (range or partition reads). Default 1. | table rows per read, weighted by reads | `--rows-per-read` (tablestats counts requests, not rows) |
| `uncompressed_single_replica_gb` | Logical data size: one uncompressed copy. Keyspaces stores three copies but bills for one. | rows × row bytes, or stored bytes ÷ compression ÷ copies | uncompressed live space × N ÷ RF |
| `use_backup` | Point-in-time recovery (PITR) enabled | on when more than one Region | on when more than one datacenter |

Each Region is priced at that Region's prices. A multi-Region keyspace appears once per Region, and each copy is billed in full.

What's the same in every Region and what can differ:

| Same in every Region (replicated) | Can differ per Region |
|---|---|
| writes/s, TTL deletes/s, storage, row size | **reads/s** (clients read from their local Region), **PITR** (`use_backup`, set per Region) |

Each Region's record is priced on its own values.

### 1.2 Shared constants and defaults

Only the constants more than one part uses are listed here. Pricing-only constants are in 2.1; Cassandra-only ones are in 4.1.

| Name | Value | Pricing (Part 2) | Other sources (Part 3) | Cassandra (Part 4) |
|---|---|---|---|---|
| `SECONDS_PER_MONTH` | (365 / 12) × 86,400 = 2,628,000 | on-demand units per month | — | counters → monthly operations → per second |
| Default row size | 1,024 bytes | — | spec / `calculate.ts` default | table with no row-size sample |
| `GIGABYTE` | 1024³ = 1,073,741,824 bytes | — | rows × row bytes → GB | tablestats bytes → GB |

### 1.3 Rules that apply on both sides

- **One row size for reads and writes:** every source in this calculator sets the read and write row size to the same value.
- **Writes are per row; reads are per request.** A write is billed on the row it writes. A read is billed on all the bytes it returns, summed across the rows it reads (`avg_rows_per_read`). Rows per read defaults to 1 everywhere.
- **Batches are billed per row written.** An `UNLOGGED BATCH` costs the same as its individual writes. A `LOGGED BATCH` costs **twice** that, because Keyspaces writes every row twice: once to prepare and once to commit (2.2). Count writes/s as rows written, not batch statements.
- **Same aggregation rule:** both conversions combine tables into a keyspace the same way: rates and storage are summed, and row size is weighted by operations.
- **PITR default:** on when more than one Region / datacenter is priced (`--dc-count` > 1, several spec Regions, or several Cassandra datacenters). Otherwise off. `--pitr` / `--no-pitr` (or `pitr` in a spec) overrides it for every Region.
- **Per-Region PITR:** `--pitr-override 'dc-1=on,dc-2=off'` on `calculate.ts` and `parse-cassandra.ts`, or `{"name": "<region>", "pitr": false}` in a spec's `regions`. It wins over the default and over `--pitr` / `--no-pitr`. Each estimate reports `pitr_by_datacenter`.
- **Per-Region reads:** `--reads-override 'dc-1=500,dc-2=100'` on both CLIs, each datacenter's own sample for Cassandra (4.5), or `reads_per_second_by_region` on a spec table (3.2).
- **Region per record:** `calculate.ts` uses its `region` argument, and a spec uses its `regions` list. For Cassandra, the order is `--dc-region dc=region`, then the datacenter name when it's a Region code (e.g. `us-east-1`), then `--region`.

### 1.4 Negative knowledge (what isn't true or possible)

These are common wrong assumptions. Don't state them, and correct them when a customer or a source assumes them.

**Keyspaces product facts**
- **No Region twice:** a keyspace or table can't be replicated to the same AWS Region twice. A multi-Region keyspace has at most one replica per Region. In this calculator:
  - `parse-cassandra.ts` merges Cassandra datacenters that map to the same Region into one record: reads are summed; writes, TTL deletes and storage count once (the largest datacenter); node counts are added together. The output lists them in `merged_datacenters`.
  - Workload specs drop repeated Regions.
  - CloudFormation generates a single-Region keyspace when the datacenters collapse to fewer than two Regions.
  - `calculate.ts --dc-count N` means N **different** Regions priced at one Region's prices, not N copies in one Region.
- **No replication factor:** Keyspaces stores three copies of the data in each Region but bills one, so you can't choose or pay for a replication factor. RF exists only in Cassandra conversion (Part 4).
- **Per-row writes:** a write isn't billed per statement or per batch, but per row written. A `LOGGED BATCH` isn't billed like an `UNLOGGED BATCH`: each row is written twice (prepare + commit), so it costs 2× (2.2). Batching doesn't reduce write units.
- **Per-request reads:** a multi-row read isn't billed per row. Its rows' bytes are added up per 1 MB page, then rounded up to 4 KB (2.2).
- **No data-transfer charge** for inbound traffic, for traffic between Keyspaces and other AWS services in the same Region, or for multi-Region replication (2.6.1).
- **Free tier not in estimates:** the AWS Free Tier isn't deducted (2.8). It covers the first three months only.
- **Savings plans don't discount everything:** storage, PITR and TTL deletes stay at list price (2.4).
- **Compatibility is binary:** an unsupported feature (secondary indexes, triggers, materialized views, UDFs, UDAs, LWT in unlogged batches, aggregate queries) isn't "supported with restrictions".

**Calculator facts**
- **Strong reads only:** eventually consistent (`LOCAL_ONE`) reads are never part of a total. They're shown as half the strong cost for information.
- **On-demand templates by default:** a CloudFormation template isn't provisioned just because provisioned is cheaper (2.5).
- **No manual pricing:** prices aren't computed by the AI. Every number comes from the scripts (`--explain` writes them out).

---

## Part 2 — Keyspaces pricing (any source)

Scope: workload (Part 1) → monthly cost. Nothing here depends on where the workload came from.

### 2.1 Pricing constants and price inputs (per Region)

| Name | Value | Used for |
|---|---|---|
| `HOURS_PER_MONTH` | (365 / 12) × 24 = 730 | Provisioned capacity hours per month |
| `DEFAULT_TARGET_UTILIZATION` | 0.70 (70%) | The Amazon Keyspaces auto scaling default target utilization: the share of provisioned capacity that steady-state traffic is expected to consume. Provisioned capacity = steady-state rate ÷ `DEFAULT_TARGET_UTILIZATION`, leaving 30% headroom for peaks. Keyspaces accepts targets from 20% to 90%. Defined once in `src/calculator/Constants.js`. |

Prices are looked up by the Region's long name (e.g. `US East (N. Virginia)`) in `mcs.json`. Where the price files come from and what their fields mean: 2.7.

| Dimension | `mcs.json` key | Unit |
|---|---|---|
| On-demand read | `MCS-ReadUnits` | per read request unit (RRU) |
| On-demand write | `MCS-WriteUnits` | per write request unit (WRU) |
| Provisioned read | `Provisioned Read Units` | per RCU-hour |
| Provisioned write | `Provisioned Write Units` | per WCU-hour |
| Storage | `AmazonMCS - Indexed DataStore per GB-Mo` | per GB-month |
| PITR backup | `Point-In-Time-Restore PITR Backup Storage per GB-Mo` | per GB-month |
| TTL deletes | `Time to Live` | per TTL delete unit |

A Region missing from `mcs.json` is skipped (no cost). The CLIs reject unknown Region codes.

### 2.2 Billable units per operation

| Operation | Units per operation |
|---|---|
| Write (WRU / WCU) | ceil(row_bytes / 1024), per row written |
| Write in an `UNLOGGED BATCH` | same as an individual write: ceil(row_bytes / 1024) per row |
| Write in a `LOGGED BATCH` | **2 ×** ceil(row_bytes / 1024) per row (prepare + commit). E.g. one batch of three 1 KB rows = 6 WRUs, against 3 unlogged. |
| TTL delete | ceil(row_bytes / 1024), per row expired |
| Read, strongly consistent (RRU / RCU) | per read request, summed over its pages (below) |

**Reads aggregate the bytes of every row they return.** With `bytes = row_bytes × rows_per_read`:
- **Pages:** a read returns up to **1 MB per page**. A larger result is paginated, and each page is a new request.
- **Units per page:** ceil(page_bytes / 4096). A full 1 MB page is 256 units.
- **Units per read:** 256 × (full pages) + ceil(remaining bytes / 4096). The last, partial page is rounded up on its own: 1 MB + 100 bytes is 256 + 1 = **257** units.

| Row size × rows per read | Bytes | Read units | Per-row billing would give |
|---|---|---|---|
| 1 KB × 1 | 1,024 | 1 | 1 |
| 1 KB × 4 | 4,096 | 1 | 4 |
| 1,500 B × 3 | 4,500 | 2 | 3 |
| 1 KB × 10 | 10,240 | 3 | 10 |
| 1 KB × 1,100 | 1,126,400 | 256 + 19 = 275 | 1,100 |

Reads are always priced as **strongly consistent** (`LOCAL_QUORUM`). Eventually consistent reads (`reads_eventual`) are shown as **half** the strong cost, for information only. They're never part of a total.

### 2.3 Monthly cost per keyspace, per Region

**On-demand**
- reads = reads/s × read units × SECONDS_PER_MONTH × price per RRU
- writes = writes/s × write units × SECONDS_PER_MONTH × price per WRU

**Provisioned** (continuous; capacity isn't rounded to whole units)
- reads = reads/s × read units × HOURS_PER_MONTH × price per RCU-hour ÷ DEFAULT_TARGET_UTILIZATION
- writes = writes/s × write units × HOURS_PER_MONTH × price per WCU-hour ÷ DEFAULT_TARGET_UTILIZATION

**Both capacity modes**
- TTL deletes = ttls/s × TTL units × SECONDS_PER_MONTH × TTL price (on-demand TTL price in both modes)
- storage = uncompressed_single_replica_gb × storage price
- PITR = uncompressed_single_replica_gb × PITR price when `use_backup` is true, otherwise 0

Totals:
- **On-demand total** = on-demand reads + on-demand writes + TTL + storage + PITR
- **Provisioned total** = provisioned reads + provisioned writes + TTL + storage + PITR

Keyspace totals add up to a Region total, and Region totals add up to the estimate total.

**Recommendation:** the cheaper of the on-demand and provisioned totals.

### 2.4 Savings plans

Rates come from `savings-plans.json` (fields and refresh steps: 2.7), keyed by Region and usage type:

| Usage type | Replaces |
|---|---|
| `ReadRequestUnits` | on-demand read price |
| `WriteRequestUnits` | on-demand write price |
| `ReadCapacityUnitHrs` | provisioned read price |
| `WriteCapacityUnitHrs` | provisioned write price |

Only reads and writes are discounted. Storage, PITR and TTL deletes stay at list price in the savings-plan totals. If a usage type is missing in a Region, that dimension falls back to list price.

`savings_plan_available` is true when a savings-plan total differs from its list-price total.

**Known issue:** when a Region has **no** savings-plan rates at all (currently only `ap-southeast-7`, Asia Pacific (Thailand)), the provisioned savings-plan read and write costs are set to **0**, not to the list price. The "provisioned + savings plan" total then shows only storage, TTL and PITR (e.g. $25.65 against $511.83 provisioned), and `savings_plan_available` is wrongly `true`.

### 2.5 CloudFormation capacity from an estimate

`generate-cfn.ts --estimate` turns the priced workload into table settings:
- **Billing mode:** `ON_DEMAND` unless `--billing-mode provisioned` is given. The estimate's cheaper mode is reported as a recommendation (2.3) but doesn't change the template. The capacity rules below apply only to `PROVISIONED` templates.
- **RCU per keyspace:** ceil(reads/s × read units ÷ DEFAULT_TARGET_UTILIZATION), where read units include rows per read (2.2).
- **WCU per keyspace:** ceil(writes/s × write units ÷ DEFAULT_TARGET_UTILIZATION).
- **Across Regions:** the largest Region's value is used.
- **Per table:** the keyspace value is split evenly across its tables (minimum 1, maximum 40,000). Keyspaces are matched by name (`estimate-workload.ts` and Cassandra estimates use the real names). A `default` keyspace (from `calculate.ts`) is spread over every table in the CQL.
- **Auto scaling:** target = `DEFAULT_TARGET_UTILIZATION` (70%), minimum 1, maximum 2× the starting capacity (at least 10).
- **PITR:** on for keyspaces where the estimate priced backups.
- **Multi-Region keyspaces:** the `RegionList` comes from each Region in the estimate. Fewer than two distinct Regions means single-Region.

### 2.6 Not modeled in pricing

- **Savings-plan terms:** only the 1-year, No Upfront Database Savings Plan rates in `savings-plans.json` (2.7) are used. Other terms aren't priced.

- **AWS Free Tier:** not deducted from estimates; see 2.8.
- **PITR restore** (`Backup Restore Size per GB`) and on-demand backup restores.
- **CDC streams** (including CDC used to keep a materialized-view replacement table in sync, 4.6) and data import/export.
- **Data transfer out:** traffic between Keyspaces and clients in another Region, or on the internet (see 2.6.1).
- **Eventually consistent reads** in totals (see 2.2).
- **Lightweight transactions and counters** as separate cost factors; they're billed as ordinary reads and writes at the average row size.
- **Logged batches:** every write is priced at 1× (unlogged). Logged-batch writes cost 2× (2.2). If a workload uses logged batches, count each logged-batch row as two writes in writes/s.
- **Static + non-static writes:** a write that includes both static and non-static columns requires additional write operations. These aren't added.
- **Driver page size (fetch size):** pages are assumed to fill to 1 MB. When a driver's fetch size ends a page earlier (e.g. 5,000 rows), there are more pages, each rounded up on its own, so real read units can be slightly higher.
- **Minimum charge for empty reads:** a read returning no rows is billed 0 units here.
- **Per-row storage metadata overhead:** storage is billed on `uncompressed_single_replica_gb` as supplied.

#### 2.6.1 Data transfer

Data transfer means traffic into and out of Amazon Keyspaces. The calculator prices none of it, because none of the cases below is part of the workload (Part 1):

| Traffic | Charge |
|---|---|
| Inbound to Keyspaces | **Free** |
| Between Keyspaces and other AWS services in the **same** Region | **Free** ($0.00 per GB) |
| Multi-Region replication between Regions of a multi-Region keyspace | **Free.** Replicated writes are billed as write units in each Region (Part 1); the transfer itself isn't charged. |
| Across Regions outside replication (e.g. Keyspaces in US East (N. Virginia) ↔ EC2 in EU (Ireland)) | **Charged on both sides** of the transfer, at AWS data-transfer rates. Not priced here. |
| Out to the internet | AWS data-transfer-out rates. Not priced here. |

- **Free allowance:** the AWS Free Tier includes 1 GB of data transfer out per month (2.8).
- **Large volumes:** above 500 TB per month, contact AWS.
- **No price data:** `mcs.json` has no data-transfer prices. For an application that reads from another Region, size that traffic separately.

### 2.7 Price data files

Pricing reads three JSON files in `src/calculator/data/`. `tools/get-pricing.sh` regenerates all three. Run it from the repo root, then rebuild anything that bundles the data (`transform-agent/build.sh`, `package-skill.sh`):

```bash
./tools/get-pricing.sh    # needs curl, jq; AWS credentials for savings plans and regions
```

`mcs.json` records its publication date in `manifest.hawkFilePublicationDate`. Check it when prices look out of date.

#### `mcs.json`: list prices

| | |
|---|---|
| Source | Public AWS price list, no credentials: `https://b0.p.awsstatic.com/pricing/2.0/meteredUnitMaps/mcs/USD/current/mcs.json` |
| `manifest` | Publication metadata: `hawkFilePublicationDate` (price date), `currencyCode` (`USD`) |
| `regions` | Map of Region **long name** (e.g. `US East (N. Virginia)`) → price entries |
| `regions[<long name>][<product>]` | One price entry per product. The code reads only the named products listed in 2.1 (e.g. `MCS-ReadUnits`, `Provisioned Write Units`). The same entries also appear under hash keys, which the code ignores. |
| `MCS-ReadUnits Over 30000000` / `MCS-WriteUnits Over 30000000` | On-demand rates after the AWS Free Tier's 30 million monthly units (2.6). They're the same as `MCS-ReadUnits` / `MCS-WriteUnits` and aren't used. |
| `Backup Restore Size per GB` | Restore price; not used (2.6) |
| `.price` | Price as a string, in USD per unit (per RRU, per RCU-hour, per GB-month, …) |
| `.rateCode` / `.RegionlessRateCode` | AWS price-list identifiers; not used |

#### `savings-plans.json`: savings-plan rates

| | |
|---|---|
| Source | `aws savingsplans describe-savings-plans-offering-rates --region us-east-1 --savings-plan-types Database --products Keyspaces` (all pages), saved as `{"searchResults": [...]}`. Needs AWS credentials with `savingsplans:DescribeSavingsPlansOfferingRates`; it's a read-only call. |
| `searchResults[]` | One record per Region × usage type. The current file has 84 records (21 Regions × 4 usage types). |
| `.unit` | What the rate is for: `ReadRequestUnits`, `WriteRequestUnits` (on-demand), `ReadCapacityUnit-Hrs`, `WriteCapacityUnit-Hrs` (provisioned). The code removes the dashes to get the usage types in 2.4 (`ReadCapacityUnit-Hrs` → `ReadCapacityUnitHrs`). |
| `.rate` | Savings-plan price as a string, in USD per `unit`. It replaces the matching list price. |
| `.properties[]` | `region` = Region **code** (e.g. `us-east-1`), mapped to the long name through `regions.json`. `productDescription` = the matching list-price product (e.g. `Provisioned Read Units`). |
| `.savingsPlanOffering` | The plan: `planType` (`Database`), `durationSeconds` (31,536,000 = 1 year), `paymentOption` (`No Upfront`), `planDescription`, `currency`. All current records are the same plan. |
| `.usageType`, `.operation`, `.productType`, `.serviceCode` | Billing identifiers (e.g. `UGE1-ReadCapacityUnit-Hrs`, `CommittedThroughput` / `PayPerRequestThroughput`, `Keyspaces`, `AmazonMCS`); not used |

- **Plan selection:** the code doesn't filter by `savingsPlanOffering`. If the file ever held several plans (e.g. 1-year and 3-year) for the same Region and unit, the **last** record would win, so keep one plan per file.
- **Missing Regions:** a Region with no records has no savings-plan pricing; see the known issue in 2.4.

#### `regions.json`: Region names

| | |
|---|---|
| Source | `aws ec2 describe-regions --all-regions` plus `aws ssm get-parameter --name /aws/service/global-infrastructure/regions/<code>/longName` for each Region (needs AWS credentials). The GovCloud Regions are added by hand. |
| Shape | A flat map in **both directions**: `"us-east-1": "US East (N. Virginia)"` and `"US East (N. Virginia)": "us-east-1"` |
| Naming | Long names that start with `Europe` are rewritten to `EU` to match `mcs.json` (e.g. `EU (Ireland)`) |

### 2.8 AWS Free Tier (knowledge only, not used in estimates)

Estimates are **steady-state monthly** costs, so the free tier is never deducted, and no script models it. Know it so you can answer questions, and mention it when a customer asks about trial or first-month costs.

| Free allowance | Amount | Period |
|---|---|---|
| On-demand write request units | 30 million WRUs per month | First **3 months** |
| On-demand read request units | 30 million RRUs per month | First **3 months** |
| Storage | 1 GB per month | First **3 months** |
| Data transfer out | 1 GB per month, aggregated across all AWS services (not in the AWS GovCloud (US) Regions) | AWS Free Tier |

- **When it starts:** the three months start in the first month the account creates an Amazon Keyspaces resource. It's not tied to a calendar quarter or to each new table.
- **Who gets it:** one free tier per **payer account**. Linked accounts in an AWS Organization share it.
- **On-demand only:** the read and write allowances are on-demand request units. Provisioned capacity (RCU/WCU-hours), PITR, TTL deletes and restores aren't included.
- **Scale:** for scale, 30 million units a month is about 11.4 one-unit requests per second (30,000,000 ÷ 2,628,000 s). Most migrated production workloads exceed it in the first days of a month.
- **In the price data:** `MCS-ReadUnits Over 30000000` / `MCS-WriteUnits Over 30000000` in `mcs.json` are the rates after the free allowance. They equal the base rates (2.7).

If a customer asks what the first months would cost, say the free tier applies, then show the regular estimate. Describe the reduction in words only (e.g. "up to 30 million on-demand reads and writes and 1 GB of storage are free each month for the first three months"); don't subtract it from the totals.

---

## Part 3 — Converting other sources → Keyspaces workload

Scope: any source that isn't Cassandra diagnostics (another database, monitoring data, a proposed data model such as a SQL-translation option, numbers the customer gives) → the workload fields in Part 1. Nothing here sets a price; the workload is priced by Part 2. Cassandra diagnostics have their own rules in Part 4.

### 3.1 Choosing the input

| You have | Use | Conversion |
|---|---|---|
| Per-table (or per-collection) metrics from any database, or a data model's tables | `estimate-workload.ts --spec workload.json` | 3.2–3.4 |
| A relational SQL model (Mode 4) | Express each modeling option as a spec (its tables, row counts, row sizes, reads/writes) and run `estimate-workload.ts` once per option | 3.2–3.4 |
| One set of round numbers (Mode 1) | `calculate.ts` | 3.5, no conversion |
| Apache Cassandra diagnostics (Mode 2) | `parse-cassandra.ts` | Part 4 |

What to convert from the source:
- **Requests:** **logical** client requests, not replica operations. If the metrics count each replica (e.g. per-member operation counters), set `request_copies`.
- **Storage:** one uncompressed copy of the data. Don't include the source's replicas, secondary indexes or materialized views. Model those as separate tables only if they become Keyspaces tables.
- **Row size:** bytes per row as stored in Keyspaces.

### 3.2 Per table (`estimate-workload.ts`)

| Workload input | Formula |
|---|---|
| reads/s | `reads_per_second` ÷ `request_copies` (default 1). `reads_per_second_by_region` replaces `reads_per_second` in the Regions it names. |
| writes/s | `writes_per_second` ÷ `request_copies` |
| TTL deletes/s | `ttl_deletes_per_second` ÷ `request_copies` |
| storage GB, from row count | `rows` × row size ÷ 1,073,741,824 |
| storage GB, from stored size | `stored_bytes` ÷ `compression_ratio` ÷ `storage_copies` ÷ 1,073,741,824 (ratio and copies default 1) |
| row size | `avg_row_bytes`; else the sum of `columns` type sizes (below); else 1,024 |
| rows per read | `rows_per_read` (default 1) |

**Column type sizes** (`columns`; also used for the materialized-view ratio in 4.6). These are estimates for modeling, and the same table is used for SQL and CQL types:

| Type | Bytes |
|---|---|
| BOOL / BOOLEAN, TINYINT | 1 |
| SMALLINT | 2 |
| INT / INTEGER / SERIAL / DATE / FLOAT / REAL | 4 |
| BIGINT / BIGSERIAL / DOUBLE / TIMESTAMP / DATETIME / TIME / DECIMAL / NUMERIC / COUNTER | 8 |
| UUID / TIMEUUID / INET / VARINT / DURATION | 16 |
| VARCHAR(n) / CHAR(n) / VARBINARY(n) | n |
| VARCHAR / TEXT / CLOB / ASCII (no length) | 64 |
| BLOB / BINARY / BYTEA | 512 |
| collections, frozen, tuples, UDTs, unknown | 128 |

Code: `estimateColumnBytes` in `src/calculator/ParsingHelpers.ts`.

### 3.3 Per keyspace

The tables in a spec keyspace are combined into one workload record (Part 1):
- **reads/s, writes/s, TTL deletes/s, storage:** sums over the tables.
- **Row size:** Σ((reads + writes) × row bytes) ÷ Σ(reads + writes), weighted by operations. This is the same rule Cassandra conversion uses (4.4).
- **Rows per read:** Σ(reads × rows per read) ÷ Σ reads, weighted by reads.

### 3.4 Regions and PITR

- **Regions:** `regions` lists the AWS Regions, either as codes or as `{"name": "<code>", "pitr": <bool>}` objects. Every keyspace is replicated to every listed Region, and each Region carries and is billed for the full replicated workload (Part 1). The default is `["us-east-1"]`.
- **Per-Region reads:** a table's `reads_per_second_by_region` (e.g. `{"eu-west-1": 30}`) sets its reads in those Regions. Every other Region uses `reads_per_second`. Each Region is converted separately, so its keyspace sums and read-weighted rows per read use that Region's reads.
- **PITR:** the keyspace's `pitr`, else that Region entry's `pitr`, else the spec's `pitr`, else the shared default (1.3): on when more than one Region.

Spec example (two tables, one Region):

```json
{"source": "postgres", "regions": ["us-east-1"],
 "keyspaces": [{"name": "shop", "tables": [
   {"name": "orders",    "reads_per_second": 6, "writes_per_second": 1.5, "avg_row_bytes": 1024, "rows": 47185920},
   {"name": "customers", "reads_per_second": 3, "writes_per_second": 0.5, "avg_row_bytes": 1024, "rows": 15728640}]}]}
```

### 3.5 Round numbers (`calculate.ts`, Mode 1)

`calculate.ts` arguments: `region reads/s writes/s row_bytes storage_gb [ttl/s] [pitr]`. They become one keyspace, `default`, with no conversion:
- **Row size:** the same row size is used for reads and writes.
- **Storage:** `storage_gb` is used as `uncompressed_single_replica_gb` unchanged.
- **`--dc-count N`:** the same workload in N Regions (`dc-1` … `dc-N`). All of them are priced at the given Region's prices, and each is billed in full.
- **`--reads-override 'dc-1=500,dc-2=100'`:** sets reads/s per Region.
- **`--rows-per-read N`:** average rows returned per read (default 1). Reads/s stays the number of read queries; read units come from section 2.2.
- **PITR:** `--pitr` / `--no-pitr`, else the legacy 7th argument `true`/`false`, else the shared default (1.3). `--pitr-override 'dc-2=off'` then sets PITR per Region.

`calculate.ts` handles one keyspace and one Region's prices per run. For several keyspaces, or Regions with different prices, use `estimate-workload.ts`.

---

## Part 4 — Apache Cassandra → Keyspaces workload

Scope: Cassandra diagnostics → the workload fields in Part 1. Nothing here sets a price; the workload is priced by Part 2.

### 4.1 Inputs and conversion constants

| Name | Value | Used for |
|---|---|---|
| Default replication factor | 3 | A keyspace with no RF for the datacenter in `DESCRIBE SCHEMA` (or no schema at all) |

The replication factor exists only here. It converts the cluster's RF copies and replica operations into the logical workload (4.3), and it isn't passed on to pricing. `calculate.ts` writes `replication_factor: 3` into its records, but pricing ignores the field.

| Capture | Provides |
|---|---|
| `nodetool status` | Datacenters and node count per datacenter (N) |
| `nodetool tablestats` (one node per datacenter) | Per-table `Local read count`, `Local write count`, `Space used (live)`, `SSTable Compression Ratio` |
| `nodetool info` (same node) | Uptime (seconds) and the node's datacenter |
| `DESCRIBE SCHEMA` | Replication factor (RF) per keyspace per datacenter |
| Row-size sampler | Average row size per table; whether the table has a default TTL |
| Prepared-statements sampler | `USING TTL` writes per table |

### 4.2 Per sampled node

- **Monthly writes per table** = `Local write count` ÷ uptime seconds × SECONDS_PER_MONTH. Reads are calculated the same way from `Local read count`.
- **Without an info capture**, uptime defaults to one month, so the counters are treated as one month of traffic.
- **Uncompressed bytes per table** = `Space used (live)` ÷ `SSTable Compression Ratio`. The ratio is 1 when the table has no live space.
- **Several sampled nodes:** when one datacenter has several, their per-table values are averaged.

### 4.3 Per keyspace, per datacenter → workload fields

With N = the datacenter's node count from `nodetool status`, and RF = the keyspace's replication factor in that datacenter:

| Workload field (Part 1) | Formula |
|---|---|
| `writes_per_second` | (average monthly writes per sampled node ÷ SECONDS_PER_MONTH) × N ÷ RF |
| `reads_per_second` | (average monthly reads per sampled node ÷ SECONDS_PER_MONTH) × N ÷ (RF − 1); divides by 1 when RF ≤ 2 |
| `ttls_per_second` | same as writes, counting only tables flagged as TTL (4.4) |
| `uncompressed_single_replica_gb` | average uncompressed bytes per sampled node × N ÷ RF ÷ GIGABYTE |
| `total_live_space_gb` (informational, not priced) | average compressed bytes per sampled node × N ÷ GIGABYTE |

Why these divisors:
- **Writes ÷ RF:** each client write lands on RF replicas, so ÷ RF gives logical writes.
- **Storage ÷ RF:** the cluster holds RF copies, so ÷ RF gives one logical copy.
- **Reads ÷ (RF − 1):** this divisor equals the `LOCAL_QUORUM` replica count only at RF = 3.

Exclusions:
- **System keyspaces** (`system*`, `dse_*`, `OpsCenter`, `reaper_db`, …) are excluded.
- **No replica, no workload:** a keyspace is converted only for datacenters where the schema gives it a replica (RF > 0). Datacenter names are matched to schema keys case-insensitively, and `*-seeds` matches `*-seeds`.

### 4.4 Row size and TTL

- **Table row size:** the row-size sampler's `average` for the table, else 1,024 bytes. The sampler measures each sampled row's text length and adds 106 bytes plus 2 bytes per column as an overhead estimate.
- **Keyspace row size:** each table's row size, weighted by its operations (reads + writes). The same value is used for reads and writes.
- **Rows per read:** tablestats counts read requests, not rows returned, so `avg_rows_per_read` is 1 unless `--rows-per-read N` is passed. That value applies to every keyspace. Ask the customer when their queries read ranges or whole partitions.
- **TTL tables:** a table is fully TTL-driven (every write also becomes a TTL delete) when either:
  - the row-size sampler reports `default-ttl: y` (the sampler writes `n` only for tables with `default_time_to_live = 0`; an entry missing the field counts as `y`), or
  - a prepared statement writes to it with `USING TTL`.

### 4.5 Datacenters → Regions (`parse-cassandra.ts`)

Each Cassandra datacenter becomes one Keyspaces Region record (Part 1).

| What was sampled | Rule |
|---|---|
| One sample (one datacenter folder, or one `tablestats` + `info` flat in `--dir`) | **Shared**: the sampled datacenter's per-keyspace workload is copied to every `nodetool status` datacenter the keyspace replicates to |
| Every datacenter has its own folder | **Per datacenter**: each uses its own sample, scaled by its own node count |
| Some datacenters have folders | Sampled datacenters use their own. Each unsampled one copies each keyspace from the first sampled datacenter in `nodetool status` order that has it (`fallback_datacenters` in the JSON) |

- **No node-count scaling on copies:** a copied workload is the source datacenter's workload, not rescaled by the target datacenter's node count. Each Region carries the same replicated workload.
- **Reads (`--infer-reads`):** the default `same` copies reads along with writes. `zero` gives reads to only datacenters with their own `info` file; `measured` keeps reads on only the sampled datacenter. `--reads-override` then sets reads/s for named datacenters.
- **Reads override with zero measured reads:** when a datacenter has no measured reads, the override is assigned to its single user keyspace, or split evenly when there are several.
- **Datacenters left out:** a datacenter with no user keyspaces after replication filtering (e.g. seed-only) gets no workload record.
- **Region and PITR:** see the shared rules in 1.3. `--pitr-override 'dc1=on,dc2=off'` sets PITR per datacenter.

### 4.6 Materialized views (`--views`, `--view-writes`)

Keyspaces doesn't support materialized views. A migration carries each view over as a **regular table** that the application writes itself. Tablestats lists views like tables, so each view is converted as a table (4.2–4.4) and summed into its keyspace.

**Which views:** all of them by default. `--views ks.view1,ks.view2` limits it to the listed views; unlisted views are removed from tablestats and aren't priced. The output lists every view in `materialized_views` (base table, `priced`, `writes_source`, `storage_source`).

| View field | Source |
|---|---|
| writes/s | `--view-writes own` (**default**): the view's **own** tablestats write counters, i.e. straight writes. `--view-writes unlogged-batch`: the **base table's** write rate, because the application writes the view row with every base row in an `UNLOGGED BATCH`, which costs the same as the individual writes (2.2). |
| reads/s | the view's **own** `Local read count`: the queries that read the view |
| storage | the view's **own** tablestats `Space used (live)` ÷ compression ratio (preferred). If the view has no size, the fallback is the **base table's size scaled by the row-size ratio**: base storage × (view row bytes ÷ base row bytes). For example, a view whose rows are 20% smaller gets 80% of the base table's storage (`storage_source: "base table (fallback)"`). |
| row size | the view's own row-size sample (the sampler samples views as well as tables). Otherwise the base table's row size × the row-size ratio. |
| TTL | inherited from the base table (TTL rows in the base expire from the view too); the sampler reports the base table's `default-ttl` for a view |

**Row-size ratio** (`row_size_ratio` and `row_size_ratio_source` in the output). The first available source is used:
1. **`row-size samples`:** the view's and base table's sampled `average` (preferred).
2. **`schema column types`:** both rows estimated from their `DESCRIBE SCHEMA` column types with the type table in 3.2. Used only as a ratio, never as an absolute size.
3. **`none`:** ratio 1.

- **Fallback when the view is missing:** if a listed view isn't in tablestats at all, its writes and size come from the base table (`note` in the output).
- **More resilient on Keyspaces:** the view becomes a normal table, so it can have PITR. Cassandra backups don't cover materialized views; a view is rebuilt from its base table.
- **Updates that move a view row:** when an update changes the view's primary key columns, the application deletes the old view row and writes the new one, so that's two view writes. `unlogged-batch` counts one view write per base write.
- **CDC alternative:** keeping the view table in sync with a CDC stream consumer is another option. CDC stream costs aren't modeled yet (2.6).
- **CloudFormation:** with `--estimate`, the template creates tables only for the views the estimate priced (all of them by default).
- **Compatibility:** views are still reported as unsupported (binary); this is the migration pattern, not support.

### 4.7 Self-managed Cassandra TCO (comparison only)

Shown next to the Keyspaces estimate in the PDF, computed from `tools/cassandra_tco_helper.py` output:
- **Cluster sample (`--tco`, stored as `_cluster`):** each per-node monthly cost (instance, storage, backup, network in + out, license) × the total node count across all datacenters, plus operations (operator hours) counted **once**.
- **Per-datacenter TCO maps** (web UI uploads): the same per-node sum × that datacenter's node count, plus that datacenter's operations cost.

### 4.8 Not converted

- **Storage overhead:** per-row storage metadata overhead isn't added to `uncompressed_single_replica_gb`. Only the row size used for request units includes the sampler's overhead estimate.
- **Unmeasured traffic patterns:** counters, lightweight transactions and batch sizes aren't converted separately; they arrive as ordinary reads and writes in tablestats.
- **Unsupported features:** secondary indexes, triggers and other unsupported features don't add workload; they're reported by the compatibility check instead. Materialized views are converted as tables by default (4.6).

---

## Part 5 — End to end: two sources → one workload → price

The same workload arrives from two different sources and gets the same price. One keyspace in `us-east-1`, 1 KB rows, no TTL, PITR off.

**Source A — Cassandra (Part 4).** One datacenter, N = 6 nodes, RF = 3, one node sampled:

| Sampled node | Value | Per second |
|---|---|---|
| `Local write count` over 2,628,000 s of uptime | 2,628,000 | 1 write/s |
| `Local read count` over 2,628,000 s of uptime | 7,884,000 | 3 reads/s |
| `Space used (live)`, compression ratio 0.5 | 15 GB | 30 GB uncompressed |

- `writes_per_second` = 1 × 6 ÷ 3 = **2**
- `reads_per_second` = 3 × 6 ÷ (3 − 1) = **9**
- `uncompressed_single_replica_gb` = 30 × 6 ÷ 3 = **60**

**Source B — PostgreSQL tables (Part 3).** The spec example in 3.4:

| Table | reads/s | writes/s | Storage |
|---|---|---|---|
| `orders` | 6 | 1.5 | 47,185,920 rows × 1,024 B ÷ 1,073,741,824 = 45 GB |
| `customers` | 3 | 0.5 | 15,728,640 rows × 1,024 B ÷ 1,073,741,824 = 15 GB |
| **`shop`** | 6 + 3 = **9** | 1.5 + 0.5 = **2** | 45 + 15 = **60 GB** |

**Part 1: one workload** — 9 reads/s, 2 writes/s, 60 GB, 1,024 B rows: 1 write unit and 1 read unit per operation.

**Part 2: price** (us-east-1)

| Dimension | On-demand | Provisioned |
|---|---|---|
| Reads | 9 × 2,628,000 × $0.000000125 = **$2.96** | 9 × 730 × $0.00013 ÷ 0.70 target utilization = **$1.22** |
| Writes | 2 × 2,628,000 × $0.000000625 = **$3.29** | 2 × 730 × $0.00065 ÷ 0.70 target utilization = **$1.36** |
| Storage | 60 × $0.25 = **$15.00** | **$15.00** |
| **Total** | **$21.24** | **$17.58** (recommended) |

Both sources, and the same numbers typed in directly (`calculate.ts us-east-1 9 2 1024 60 0`), give these totals, since Part 2 doesn't care about the source.
