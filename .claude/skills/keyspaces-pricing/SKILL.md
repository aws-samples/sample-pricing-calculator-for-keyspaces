---
name: keyspaces-pricing
description: Calculate the estimated monthly cost for Amazon Keyspaces (for Apache Cassandra). Use when the user asks for Keyspaces pricing, cost estimates, or wants to evaluate on-demand vs provisioned capacity. Also use when the user provides Cassandra diagnostic files (nodetool info, status, tablestats, row size sampler, schema). Also use when the user provides a SQL data model and wants to translate it to Keyspaces and compare pricing across 3 NoSQL modeling strategies.
argument-hint: [region] [reads/s] [writes/s] [row-size-bytes] [storage-gb] [ttl/s] [pitr]
---

## Amazon Keyspaces Pricing Calculator

Three modes. Modes 1 and 2 pipe output to `generate-pdf.ts` and then `cat` the JSON. Mode 3 calls `calculate.ts` once per strategy option.

### Mode 1 — Manual inputs

Ask for any missing values: region (default `us-east-1`), reads/s, writes/s, row size bytes (default `1024`), storage GB, TTL deletes/s (default `0`), PITR (default `false`).

```bash
npx ts-node --require tsconfig-paths/register --project tsconfig.scripts.json "${CLAUDE_SKILL_DIR}/scripts/calculate.ts" <region> <reads/s> <writes/s> <row-size-bytes> <storage-gb> <ttl/s> <pitr> | tee /tmp/keyspaces-calc.json | npx ts-node --require tsconfig-paths/register --project tsconfig.scripts.json "${CLAUDE_SKILL_DIR}/scripts/generate-pdf.ts" 2>&1
cat /tmp/keyspaces-calc.json
```

### Mode 2 — Cassandra diagnostic files

If the user provides a directory, pass `--dir <path>` and the script will auto-classify every file in it using the built-in detectors in `ParsingHelpers.ts` (`isTablestatsFile`, `isStatusFile`, `isInfoFile`, `isRowSizeFile`, `isSchemaFile`, `isTcoFile`). If the user provides individual files, pass each with its explicit flag instead.

Infer the region from the status output if not specified. Always pass `--region` explicitly to override the inferred region.

Before running, check what was detected and ask the user for anything missing:

- **No status file detected:** ask how many Cassandra datacenters (or target AWS regions) and how many nodes per datacenter/region.
- **No nodetool info file detected:** tablestats can only provide live space, not reads/writes per second — ask the user for total cluster reads/s and writes/s.
- **No rowsize sampler file detected:** ask the user for the average row size in bytes (default: 1024). Proceed with 1024 if they don't provide one.

```bash
# Directory auto-detection
npx ts-node --require tsconfig-paths/register --project tsconfig.scripts.json "${CLAUDE_SKILL_DIR}/scripts/parse-cassandra.ts" --dir <directory> --region <region> [--pitr] | tee /tmp/keyspaces-calc.json | npx ts-node --require tsconfig-paths/register --project tsconfig.scripts.json "${CLAUDE_SKILL_DIR}/scripts/generate-pdf.ts" 2>&1

# Individual files
npx ts-node --require tsconfig-paths/register --project tsconfig.scripts.json "${CLAUDE_SKILL_DIR}/scripts/parse-cassandra.ts" --region <region> --tablestats <path> [--status <path>] [--info <path>] [--rowsize <path>] [--schema <path>] [--pitr] | tee /tmp/keyspaces-calc.json | npx ts-node --require tsconfig-paths/register --project tsconfig.scripts.json "${CLAUDE_SKILL_DIR}/scripts/generate-pdf.ts" 2>&1

cat /tmp/keyspaces-calc.json
```

Multiple `--info` flags accepted (one per node). `--dir` and explicit flags can be combined — explicit flags take precedence.

### Display results (Modes 1 & 2)

Show a cluster/inputs summary, a keyspace breakdown (Mode 2), and an on-demand vs provisioned cost table. Include a savings plan row if `savings_plan_available` is true. Recommend the cheaper option. Note the PDF was saved.

---

### Mode 3 — SQL to Keyspaces model translation

Triggered when the user provides SQL DDL (`CREATE TABLE`) statements. Translates the SQL into 3 Keyspaces data model strategies, prices each, and recommends the best fit.

#### Step 1 — Parse the SQL

Extract:
- **Tables**: name, columns (name + SQL type), primary key column(s), UNIQUE constraints
- **Foreign keys**: `(source_table, source_col)` → `(target_table, target_col)`
- **Access queries**: any SELECT statements (drive partition key decisions)

#### Step 2 — Estimate field sizes

| SQL Type | Bytes |
|---|---|
| BOOL / BOOLEAN | 1 |
| SMALLINT | 2 |
| INT / INTEGER / SERIAL / DATE / FLOAT / REAL | 4 |
| BIGINT / DOUBLE / TIMESTAMP / DATETIME / DECIMAL / NUMERIC | 8 |
| UUID | 16 |
| VARCHAR(n) / CHAR(n) | n |
| VARCHAR / TEXT / CLOB (no length) | 64 |
| BLOB / BINARY | 512 |

`row_size_bytes` per table = sum of all column byte sizes.

#### Step 3 — Gather workload inputs

If not provided, ask for:
- **Rows per table**
- **Reads/s** and **writes/s** per table or combined
- **AWS region** (default: `us-east-1`)

#### Step 4 — Apply the 3 strategies and compute sizing

**Option A — Full Denormalization**
Merge all FK-related tables into one table.
- `merged_row_size_bytes` = sum of all unique column sizes across all tables (FK columns deduplicated)
- `merged_row_count` = product of all table row counts
- `storage_gb` = `(merged_row_count × merged_row_size_bytes) / (1024^3)`
- `reads_per_sec` = sum of reads across all original tables
- `writes_per_sec` = sum of writes across all original tables
- **CQL**: single merged table; partition key = FK column matching the access query; clustering key = child table PK

**Option B — Normalized with Lookup Tables**
Keep original tables; add a lookup table per FK for application-side joins.
- Original tables: each maps 1:1 to CQL; `storage_gb` = `(row_count × row_size_bytes) / (1024^3)` per table
- Lookup table per FK `(source.col → target.pk)`: named `target_by_source`
  - Columns: FK col + target PK col only
  - `lookup_row_size_bytes` = size(FK col) + size(target PK col)
  - `lookup_storage_gb` = `(target_row_count × lookup_row_size_bytes) / (1024^3)`
- `total_storage_gb` = sum of all original + all lookup table storage
- `reads_per_sec` = sum of original reads + (FK lookups required per query × original reads)
- `writes_per_sec` = sum of original writes + (1 write per lookup table per insert)
- **CQL**: original tables unchanged + one lookup table per FK

**Option C — Denormalized with Reverse Index**
Same merged table as Option A; add a reverse index table for each non-PK FK column.
- Merged table: identical to Option A
- Reverse index per non-PK FK column: partition key = FK col, clustering key = merged PK, all merged columns included (full copy)
  - `reverse_row_size_bytes` = `merged_row_size_bytes`
  - `reverse_row_count` = `merged_row_count`
- `total_storage_gb` = `merged_storage_gb × (1 + number_of_reverse_indexes)`
- `reads_per_sec` = same as Option A (no extra read; correct table used per query)
- `writes_per_sec` = `Option A writes_per_sec × (1 + number_of_reverse_indexes)`
- **CQL**: merged table + one reverse index table per non-PK FK column

#### Step 5 — Price each option

```bash
npx ts-node --require tsconfig-paths/register --project tsconfig.scripts.json "${CLAUDE_SKILL_DIR}/scripts/calculate.ts" <region> <reads_per_sec> <writes_per_sec> <avg_row_size_bytes> <storage_gb> 0 false | tee /tmp/keyspaces-sql-optionA.json
# Repeat for B → optionB.json, C → optionC.json
```

Extract `provisioned.total`, `on_demand.total`, `provisioned_savings_plan.total` from each JSON.

#### Step 6 — Present results

1. **Three-model summary table** — one row per metric, one column per option:

| | Option A — Denorm | Option B — Normalized | Option C — Reverse Index |
|---|---|---|---|
| Storage | X TB | X TB | X TB |
| Reads/s | X | X | X |
| Writes/s | X | X | X |
| Bytes/row (avg) | X | X | X |
| Backup | off / on | off / on | off / on |
| Lookups per query | X | X | X |
| **Provisioned + Savings Plan/mo** | **$X** | **$X** | **$X** |
| **On-Demand + Savings Plan/mo** | **$X** | **$X** | **$X** |

- **Lookups per query**: number of separate Keyspaces reads required to satisfy a single user-facing query (e.g. 1 = single-table read, 2 = lookup table + data table, N = lookup returns N keys each needing a separate read).
- **Backup**: reflect the `pitr_enabled` input; show `PITR on` or `off`.

2. **CQL** — full generated table definitions for each option
3. **Recommendation** — best option based on cost, query fit, write amplification, and storage trade-offs.
