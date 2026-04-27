---
name: amazon-keyspaces-skill
description: Skill file for Amazon Keyspaces. Calculate the estimated monthly cost for Amazon Keyspaces (for Apache Cassandra). Use when the user asks for Keyspaces pricing, cost estimates, or wants to evaluate on-demand vs provisioned capacity. Also use when the user provides Cassandra diagnostic files (nodetool info, status, tablestats, row size sampler, schema, prepared statements). Also use when the user provides a SQL data model and wants to translate it to Keyspaces and compare pricing across 3 NoSQL modeling strategies. Also use when the user asks whether a Cassandra schema is compatible with Amazon Keyspaces or wants to check for unsupported features (secondary indexes, triggers, materialized views, user-defined functions, aggregates, lightweight transactions in unlogged batches, aggregate calls in queries).
argument-hint: [region] [reads/s] [writes/s] [row-size-bytes] [storage-gb] [ttl/s] [pitr]
---

## Amazon Keyspaces Pricing Calculator

Four modes:

1. **Manual inputs** — call `calculate.ts` and emit pricing JSON.
2. **Cassandra diagnostic files** — call `parse-cassandra.ts` and emit pricing JSON (auto-includes a `compatibility` block when a schema is present).
3. **Compatibility check** — call `check-compatibility.ts` and emit a JSON compatibility report.
4. **SQL → Keyspaces translation** — call `calculate.ts` once per NoSQL modeling strategy and compare.

PDF generation is **optional** for any cost-bearing mode (1, 2, 4) — see "PDF reporting (optional)" below. By default, modes emit JSON only; the agent should ask the user whether a PDF is wanted after the JSON has been displayed. Multiple estimates can be consolidated into a single PDF in one `generate-pdf.ts` invocation.

### Mode 1 — Manual inputs

1. Ask the user if they are currently running Cassandra and can collect any of the inputs in the **Required diagnostic files** table under Mode 2 (`tablestats`, `info`, `status`, `schema`, `rowsize`, `prepared`, `tco`). Explain that diagnostic output improves estimate accuracy.
2. If the user answers **yes**, switch to Mode 2 and use the diagnostic data.
3. Otherwise, ask for any missing values:
   - region (default `us-east-1`)
   - reads/s
   - writes/s
   - row size bytes (default `1024`)
   - storage GB
   - TTL deletes/s (default `0`)
   - Backups / PITR (default `false`)
4. Run the command block below to produce the JSON. After displaying the results, ask the user whether they would like a PDF report. If yes, follow "PDF reporting (optional)" below.

```bash
npx ts-node --require tsconfig-paths/register --project tsconfig.scripts.json "${CLAUDE_SKILL_DIR}/../../../scripts/calculate.ts" <region> <reads/s> <writes/s> <row-size-bytes> <storage-gb> <ttl/s> <pitr> | tee /tmp/keyspaces-calc.json
cat /tmp/keyspaces-calc.json
```

### Mode 2 — Cassandra diagnostic files

**Pipeline (always in this order):** (1) **Gather** — Step 0 + **Required diagnostic files** (below) → (2) **Parse** — Step 1+ through the `parse-cassandra.ts` command → (3) **Present JSON** → (4) **Optional PDF** — **PDF reporting (optional)**.

#### Step 0 — Intake and capture

Work through these in order:

1. **Directory already provided?** If the user supplies a folder, confirm it contains at least one detected **`tablestats`** file and at least one **`info`** file (use the same detectors as Step 1+). If both are present, go to **Step 1+** with `--dir`. If either is missing or the folder is incomplete, continue with steps 2–5 — capture or escalate per the table before invoking `parse-cassandra.ts`, or use **Mode 1** if **`tablestats`** / **`info`** cannot be obtained.
2. **Partial upload or incomplete bundle:** If they only supplied one file — or the directory is missing mandatory captures — identify each file’s **ID** using the detectors in `ParsingHelpers.ts` (same names as in Step 1+), then help them collect the remaining rows from the **Required diagnostic files** table.
3. **What to collect:** Use the table: seven **IDs**, **If missing — ask**, and **Default / escalation**. **`tablestats` and `info` are mandatory** for Mode 2; without both on disk (after capture attempts), use **Mode 1**. **`tco`** is optional (TCO comparison only).
4. **Connection details (once, before any remote capture):** Host, port, optional `-u` / `-p`, optional `--ssl` — reuse for every `cqlsh` and `./tools/...` command in the table. If the workload is already on Amazon Keyspaces, use `cassandra.<region>.amazonaws.com 9142 --ssl`.
5. **If captures are still missing:** Run the **Command** for each needed row on the cluster, writing to the **Output** filenames. Put every file in **one** working directory (same folder you will pass as `--dir`).

#### Required diagnostic files

Each **ID** matches a `parse-cassandra.ts` flag (`--<id>`) when you pass paths explicitly. **If missing — ask** is the prompt when the capture cannot be produced; **Default / escalation** is what to do next (see Step 1+).

| ID | Captures | Run | Output | Command | If missing — ask | Default / escalation |
|---|---|---|---|---|---|---|
| `tablestats` | Live space + column-family details | every node | `tablestats.txt` | `nodetool tablestats > tablestats.txt` | — | **Mandatory.** Recapture from the cluster. **When absent:** use **Mode 1** if it still cannot be obtained — `parse-cassandra.ts` exits without `--tablestats`. |
| `info` | DC, host id, uptime → derives reads/writes per second from cumulative counters | every node | `info.txt` | `nodetool info > info.txt` | — | **Mandatory.** **When absent:** use **Mode 1** — diagnostic pricing requires `nodetool info` alongside `tablestats` (RPS from cumulative counters). |
| `status` | DC list and node count per DC | once on any node | `status.txt` | `nodetool status > status.txt` | How many **datacenters** (or target regions) are in the cluster? How many **Cassandra nodes** in each? | Prefer capturing `nodetool status`. If the file is missing, use answered topology **or** infer from **`info`** files grouped by DC (see `parse-cassandra.ts`). If topology still cannot be established, use **Mode 1**. |
| `schema` | DDL — source of truth for compatibility (Mode 3 and the auto-compat block in Mode 2) | once | `schema.cql` | `cqlsh <host> <port> [<auth>] -e 'DESCRIBE SCHEMA' > schema.cql` | What **replication factor** do you use for application keyspaces (per DC)? | **When absent:** parser uses replication factor **3** internally (`REPLICATION_FACTOR`); ask the question so the customer’s intent matches the estimate. |
| `rowsize` | Average row size per table (low-rate live sample) | once | `rowsize.txt` | `./tools/row-size-sampler.sh <host> <port> [<auth>] > rowsize.txt` | — | **1024** bytes (1 KB). No further questions. |
| `prepared` | Prepared statements — drives compatibility (LWT-in-batch, aggregations) and the `USING TTL` pricing signal (recommended) | once | `prepared_statements.ndjson` | `./tools/prepared-statements-sampler.sh <host> <port> [<auth>] > prepared_statements.ndjson` | — | Omit `--prepared`. No further questions. |
| `tco` | EC2 instance + storage costs for self-managed Cassandra (optional — only if the user wants a TCO comparison) | each EC2 Cassandra host | `tco.json` | `python tools/cassandra_tco_helper.py <instance-id> --region <region> > tco.json` | — | Omit TCO inputs. No further questions. |

`<auth>` shorthand = `[-u user] [-p pass] [--ssl]`. Omit when the cluster has no authentication or TLS.

**Mode 2 vs Mode 1:** Diagnostic pricing requires **`tablestats` and `info`** (`nodetool tablestats` / `nodetool info`). Without either capture, use **Mode 1**. Without **`nodetool status`**, ask the topology questions in the **`status`** row; if the customer cannot answer and you cannot infer DC/node counts from **`info`**, use **Mode 1**.

#### Step 1+ — Run the parser, fill gaps, emit JSON

1. **Prepare paths:** Prefer `--dir <path>` so every file in the folder is auto-classified — one detector per **ID** (`isTablestatsFile`, `isStatusFile`, `isInfoFile`, `isRowSizeFile`, `isSchemaFile`, `isTcoFile`, `isPreparedStatementsFile` in `ParsingHelpers.ts`). Otherwise pass explicit `--tablestats`, `--status`, `--info`, `--rowsize`, `--schema`, `--prepared` (repeat `--info` per node if needed).
2. **Mode 1 gate and gaps:** Before running the parser, apply the **Required diagnostic files** table. **Without `tablestats` or without any `info` file, stop — use Mode 1** (do not invoke `parse-cassandra.ts`). For every other **ID**, use **If missing — ask** and **Default / escalation** (topology/`status`, **`schema`** RF / internal default 3, **`rowsize`** 1024 bytes, omit **`prepared`** / **`tco`** with no extra questions).
3. **Region:** Choose `--region` for the command line: map from the DC name in **`status`** when you have it; otherwise from the **Datacenter** field in **`nodetool info`**; otherwise default **`us-east-1`**. Pass `--region` explicitly whenever inference is wrong or unknown.
4. **Run** the command block, show the JSON, then offer a PDF per **PDF reporting (optional)**.

```bash
# Directory auto-detection
npx ts-node --require tsconfig-paths/register --project tsconfig.scripts.json "${CLAUDE_SKILL_DIR}/../../../scripts/parse-cassandra.ts" --dir <directory> --region <region> [--pitr] | tee /tmp/keyspaces-calc.json

# Individual files
npx ts-node --require tsconfig-paths/register --project tsconfig.scripts.json "${CLAUDE_SKILL_DIR}/../../../scripts/parse-cassandra.ts" --region <region> --tablestats <path> [--status <path>] [--info <path>] [--rowsize <path>] [--schema <path>] [--prepared <path>] [--pitr] | tee /tmp/keyspaces-calc.json

cat /tmp/keyspaces-calc.json
```

**After a successful run**

- **Multi-cluster / re-runs:** Write each estimate to its own JSON path, then one `generate-pdf.ts` with multiple `--input` if they want a single comparison PDF (**PDF reporting (optional)**).
- **Flags:** Multiple `--info` (one per node). `--dir` plus explicit flags: explicit wins.
- **`compatibility` in JSON:** If `schema` or `prepared` was supplied (`--schema` / `--prepared` or detected in `--dir`), output includes `compatibility`: `{ has_issues, summary, details }`, with `summary` / `details` split into `schema` (DDL) and `query_patterns` (prepared statements). If `has_issues`, use Mode 3 display rules alongside pricing.

#### Capturing prepared statements

The **`prepared`** row in the table reads `system.prepared_statements` and emits one JSON object per line (stdout → file). Pass that file as `--prepared`. It affects output in two ways:

1. **Compatibility:** LWT inside `BEGIN UNLOGGED BATCH`, aggregates (`COUNT` / `MIN` / `MAX` / `SUM` / `AVG`), and (with `schema`) calls to user-declared UDFs.
2. **Pricing:** `INSERT … USING TTL` / `UPDATE … USING TTL` marks those tables as fully TTL-driven for write accounting even without `default_time_to_live` in DDL. Tables that already have `default_time_to_live` still follow the existing `rowsize`-based TTL path.

### Display results (Modes 1 & 2)

Show a cluster/inputs summary, a keyspace breakdown (Mode 2), and an on-demand vs provisioned cost table. Include a savings plan row if `savings_plan_available` is true. Recommend the cheaper option. If the user opted into a PDF, say where it was written.

---

### Mode 3 — Compatibility check

Triggered when the user asks whether a Cassandra schema (or workload) is compatible with Amazon Keyspaces, or provides CQL DDL and/or prepared statements and wants to know what will / won't work — **without** asking for a cost estimate. (If they also want pricing, use Mode 2, which includes compatibility automatically when a schema or prepared statements file is present.)

Two input sources, either or both may be supplied:

- **`schema`** input (CQL DDL — see Mode 2 → **Required diagnostic files** for the capture command) — wraps `parse_cassandra_schema_compatibility`. Flags:
  - `CREATE INDEX` (secondary indexes) — per table
  - `CREATE TRIGGER` — per table
  - `CREATE MATERIALIZED VIEW` — attached to the base table
  - `CREATE FUNCTION` — counted globally
  - `CREATE AGGREGATE` — counted globally
- **`prepared`** input (`system.prepared_statements` — see Mode 2 → **Required diagnostic files** for the capture command) — wraps `parse_prepared_statements`. Flags:
  - **LWT inside `BEGIN UNLOGGED BATCH`** — any conditional (`IF NOT EXISTS`, `IF EXISTS`, `IF <col>=…`) inside an unlogged batch.
  - **Aggregate calls** — `COUNT(`, `MIN(`, `MAX(`, `SUM(`, `AVG(` in any `SELECT`.
  - **Per-table `USING TTL`** — informational only (not an issue); reported under `query_patterns.ttl_tables` and used by Mode 2 to set `has_ttl` for pricing.

User-defined function usage is intentionally not detected from prepared statements — `CREATE FUNCTION` in the schema is the source of truth and is already surfaced by the schema check.

Every flagged feature is **not supported** by Amazon Keyspaces — Keyspaces compatibility is binary (a feature is either supported or it is not; do not describe detected features as "supported with restrictions", "supported with caveats", "problematic under certain conditions", or similar).

```bash
# Schema file
npx ts-node --require tsconfig-paths/register --project tsconfig.scripts.json "${CLAUDE_SKILL_DIR}/../../../scripts/check-compatibility.ts" --schema <path> | tee /tmp/keyspaces-compat.json

# Prepared statements file
npx ts-node --require tsconfig-paths/register --project tsconfig.scripts.json "${CLAUDE_SKILL_DIR}/../../../scripts/check-compatibility.ts" --prepared <path> | tee /tmp/keyspaces-compat.json

# Both
npx ts-node --require tsconfig-paths/register --project tsconfig.scripts.json "${CLAUDE_SKILL_DIR}/../../../scripts/check-compatibility.ts" --schema <schema-path> --prepared <prepared-path> | tee /tmp/keyspaces-compat.json

# Schema piped on stdin (when the user pastes CQL inline; valid only without --prepared)
echo "<cql-content>" | npx ts-node --require tsconfig-paths/register --project tsconfig.scripts.json "${CLAUDE_SKILL_DIR}/../../../scripts/check-compatibility.ts" | tee /tmp/keyspaces-compat.json

cat /tmp/keyspaces-compat.json
```

Output shape:

```json
{
  "source": "compatibility-check",
  "input": { "schema": "<path or null>", "prepared": "<path or null>" },
  "has_issues": true | false,
  "summary": {
    "total_issues": N,
    "schema": { "total_issues": N, "keyspaces_affected": N, "tables_affected": N, "functions": N, "aggregates": N } | null,
    "query_patterns": { "lwt_in_unlogged_batch": N, "aggregations": N, "ttl_tables": N } | null
  },
  "details": {
    "schema": { "functions": N, "aggregates": N, "keyspaces": { "<ks>": { "<table>": { "indexes": [...], "triggers": [...], "materializedViews": [...] } } } } | null,
    "query_patterns": {
      "lwt_in_unlogged_batch": [ { "prepared_id": "...", "query_string": "..." } ],
      "aggregations":          [ { "prepared_id": "...", "function": "COUNT", "query_string": "..." } ],
      "ttl_tables": { "<ks>.<table>": { "uses_ttl": true, "ttl_values": [3600, 86400] } }
    } | null
  }
}
```

#### Display results (Mode 3)

- If `has_issues` is false, state clearly that the schema/workload is compatible with Amazon Keyspaces.
- Otherwise, present a per-keyspace/per-table breakdown of unsupported features (from `details.schema`) and a per-query breakdown of unsupported patterns (from `details.query_patterns`). Show the offending `query_string` (truncated if long) for each prepared-statement finding.
- State compatibility in binary terms only — every detected feature is **not supported**. Do not add conditional language ("supported with restrictions", "works if cardinality is high", "may cause hot partitions", etc.); those qualifiers do not apply here.
- `query_patterns.ttl_tables` is informational, not an issue. Report it as "tables using `USING TTL`: …" so the user can verify the TTL signal Mode 2 is using for pricing.
- Offer migration guidance for each category. Keep the guidance to *what to do instead*, not to *why the feature is limited*:
  - **Secondary indexes (`CREATE INDEX`)**: not supported. Create a separate denormalized table keyed by the column you wanted to query on and write to both tables from the application.
  - **Triggers (`CREATE TRIGGER`)**: not supported. Replicate the logic in the application layer or in a stream consumer (CDC).
  - **Materialized views (`CREATE MATERIALIZED VIEW`)**: not supported. Maintain a second table in the application (dual-write) keyed for the alternate access pattern.
  - **User-defined functions / aggregates (`CREATE FUNCTION`, `CREATE AGGREGATE`)**: not supported. Move the computation client-side or into an ETL / stream-processing step.
  - **LWT in unlogged batch**: not supported. Issue the LWT as a single-statement conditional outside any batch, or use a logged batch (note: LWTs in logged batches are permitted in Cassandra but have their own semantics — the user should pick whichever fits the app).
  - **Aggregate calls (`COUNT`/`MIN`/`MAX`/`SUM`/`AVG`)**: not supported. Compute the aggregate client-side from a paginated `SELECT`, or maintain pre-aggregated counter/summary tables.

---

### Mode 4 — SQL to Keyspaces model translation

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
npx ts-node --require tsconfig-paths/register --project tsconfig.scripts.json "${CLAUDE_SKILL_DIR}/../../../scripts/calculate.ts" <region> <reads_per_sec> <writes_per_sec> <avg_row_size_bytes> <storage_gb> 0 false | tee /tmp/keyspaces-sql-optionA.json
# Repeat for B → /tmp/keyspaces-sql-optionB.json, C → /tmp/keyspaces-sql-optionC.json
```

Extract `provisioned.total`, `on_demand.total`, `provisioned_savings_plan.total` from each JSON.

After displaying the comparison, ask the user whether they would like a PDF report. If yes, follow "PDF reporting (optional)" below — Mode 4 is a natural multi-input case, so pass all three JSON files to a single `generate-pdf.ts` invocation to produce one consolidated comparison PDF.

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

---

### PDF reporting (optional)

PDF generation is **never automatic**. After running any cost-bearing mode (1, 2, or 4) and displaying the JSON results, ask the user whether they would like a PDF report. Skip PDF generation for Mode 3 (compatibility check only) — there is no pricing data to report; if a PDF is desired alongside compatibility findings, run Mode 2 instead, which auto-includes the compatibility section.

`generate-pdf.ts` accepts a single estimate (via stdin or `--input`) or multiple estimates (via repeated `--input` flags) and writes one PDF.

```bash
# Single estimate (from stdin — backwards-compatible with the old pipeline)
cat /tmp/keyspaces-calc.json | npx ts-node --require tsconfig-paths/register --project tsconfig.scripts.json "${CLAUDE_SKILL_DIR}/../../../scripts/generate-pdf.ts"

# Single estimate (from file)
npx ts-node --require tsconfig-paths/register --project tsconfig.scripts.json "${CLAUDE_SKILL_DIR}/../../../scripts/generate-pdf.ts" --input /tmp/keyspaces-calc.json

# Multiple estimates → one consolidated comparison PDF
npx ts-node --require tsconfig-paths/register --project tsconfig.scripts.json "${CLAUDE_SKILL_DIR}/../../../scripts/generate-pdf.ts" \
  --input /tmp/keyspaces-sql-optionA.json --label "Option A — Denorm" \
  --input /tmp/keyspaces-sql-optionB.json --label "Option B — Normalized" \
  --input /tmp/keyspaces-sql-optionC.json --label "Option C — Reverse Index" \
  --output /tmp/keyspaces-comparison.pdf
```

Flags:

- `--input <path>` — path to a `calculate.ts` / `parse-cassandra.ts` JSON file. Repeatable.
- `--label <name>` — display label for the most recent `--input` (used in the comparison summary table and per-estimate section headers). Optional; defaults to `Estimate 1`, `Estimate 2`, etc.
- `--output <path>` — output PDF path. Defaults to `./keyspaces-pricing-estimate.pdf`.

Behavior:

- **1 input** (stdin or single `--input`): renders the existing single-estimate report.
- **2+ inputs**: renders a consolidated comparison report — title page with a side-by-side comparison summary table (storage, reads/s, writes/s, on-demand/mo, OD+SP/mo, provisioned/mo, prov+SP/mo) followed by a per-estimate section with full input/pricing tables and any compatibility findings.

Use a single multi-input invocation whenever the user has more than one estimate to report — Mode 4 (always 3 strategies), repeated Mode 1 sensitivity runs, or several Mode 2 clusters being compared. Avoid generating a separate PDF per estimate.
