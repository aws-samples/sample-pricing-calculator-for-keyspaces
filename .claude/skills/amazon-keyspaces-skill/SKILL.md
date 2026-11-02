---
name: amazon-keyspaces-skill
description: Skill file for Amazon Keyspaces. Calculate the estimated monthly cost for Amazon Keyspaces (for Apache Cassandra). Use when the user asks for Keyspaces pricing, cost estimates, or wants to evaluate on-demand vs provisioned capacity. Also use when the user provides Cassandra diagnostic files (nodetool info, status, tablestats, row size sampler, schema, prepared statements). Also use when the user provides a SQL data model and wants to translate it to Keyspaces and compare pricing across 3 NoSQL modeling strategies. Also use when the user asks whether a Cassandra schema is compatible with Amazon Keyspaces or wants to check for unsupported features (secondary indexes, triggers, materialized views, user-defined functions, aggregates, lightweight transactions in unlogged batches, aggregate calls in queries). Also use when the user wants an AWS CloudFormation template for Amazon Keyspaces keyspaces and tables from a Cassandra schema or a proposed data model.
argument-hint: [region] [reads/s] [writes/s] [row-size-bytes] [storage-gb] [ttl/s] [pitr-legacy] [--dc-count N] [--reads-override dc-1=...,dc-2=...] [--pitr | --no-pitr]
---

## Amazon Keyspaces Pricing Calculator

### Scripts first; rules for reference

**Every number comes from the TypeScript scripts.** Use a script whenever one covers the task. Don't calculate costs, units, storage, sums, ratios or conversions yourself, even simple ones. If you're about to do arithmetic, find the script or spec field that does it.

| Task | Script |
|---|---|
| Cost from round numbers | `calculate.ts` |
| Cost from Cassandra diagnostics | `parse-cassandra.ts` |
| Cost from another database, monitoring data or a SQL-model option | `estimate-workload.ts` (workload spec; row sizes from `columns`, storage from `rows`) |
| Compatibility | `check-compatibility.ts` |
| CloudFormation | `generate-cfn.ts` |
| PDF | `generate-pdf.ts` |
| Showing the math for an estimate | any estimate script with `--explain` |

Your part is the **judgment**: which mode applies, what to collect, the data model (tables, keys, columns, row counts, read and write patterns), Regions, and presenting results.

**`keyspaces-pricing-rules.md` is the reference, not the calculator.** Read it to:
- **Answer questions about pricing:** "how is X billed?", "why is this number what it is?", free tier, data transfer, what isn't modeled.
- **Explain an estimate:** pair it with the `--explain` lines, which are the numbers.
- **Develop new features:** the rules say what the code does and where. When adding or changing a rule, change the TypeScript first, add an `--explain` line and a test, then update the rules file. The code is the source of truth; if the file and the code disagree, fix the file.
- **Handle a gap:** if no script covers something (e.g. CDC stream costs, data transfer out), say it isn't modeled; don't estimate it by hand.

Four modes:

1. **Manual inputs** — call `calculate.ts` and emit pricing JSON.
2. **Cassandra diagnostic files** — call `parse-cassandra.ts` and emit pricing JSON (auto-includes a `compatibility` block when a schema is present).
3. **Compatibility check** — call `check-compatibility.ts` and emit a JSON compatibility report.
4. **SQL → Keyspaces translation** — model three NoSQL strategies, write each as a workload spec, price each with `estimate-workload.ts`, and compare.

PDF generation is **optional** for any cost-bearing mode (1, 2, 4) — see "PDF reporting (optional)" below. By default, modes emit JSON only; the agent should ask the user whether a PDF is wanted after the JSON has been displayed. Multiple estimates can be consolidated into a single PDF in one `generate-pdf.ts` invocation.

A **CloudFormation template of the target Keyspaces data model** is the other standard output: offer it after Modes 2, 3, and 4 (Mode 1 has no schema). See "CloudFormation template (target data model)" below. Pass the estimate so the template matches the priced model.

### Mode 1 — Manual inputs

1. Ask the user if they are currently running Cassandra and can collect any of the inputs in the **Required diagnostic files** table under Mode 2 (`tablestats`, `info`, `status`, `schema`, `rowsize`, `prepared`, `tco`). Explain that diagnostic output improves estimate accuracy.
2. If the user answers **yes**, switch to Mode 2 and use the diagnostic data.
3. Otherwise, ask for any missing values:
   - region (default `us-east-1`)
   - reads/s
   - writes/s
   - row size bytes (default `1024`)
   - rows per read (default `1`). Ask when queries read ranges or whole partitions. A read is billed on the bytes of all rows it returns (per 1 MB page, rounded up to 4 KB); pass it as `--rows-per-read N`.
   - storage GB
   - TTL deletes/s (default `0`)
   - **Symmetric multi-datacenter / multi-region** (optional): `--dc-count N` — same workload in each logical DC; datacenter IDs in the JSON become `dc-1` … `dc-N` (for `N=1` the ID stays the region short name for backward compatibility). **Read rates** are the **same in each DC** unless `--reads-override 'dc-1=100,dc-2=50'` supplies **reads/s per DC** (comma-separated `name=value`; names must match those IDs).
   - **Backups / PITR:** PITR can differ per Region: `--pitr-override 'dc-1=on,dc-2=off'`. With `--dc-count` **> 1**, PITR is **on for every DC by default**. Use `--no-pitr` to disable. With a **single** DC, PITR defaults **off** unless the user passes `--pitr` or the legacy 7th positional `true` / `false` (`<ttl/s> <pitr>`).
4. Run the command block below to produce the JSON. After displaying the results, ask the user whether they would like a PDF report. If yes, follow "PDF reporting (optional)" below.

```bash
# Single DC (legacy 7th arg `true`/`false` for PITR still supported)
npx ts-node --require tsconfig-paths/register --project tsconfig.scripts.json "${CLAUDE_SKILL_DIR}/../../../scripts/calculate.ts" <region> <reads/s> <writes/s> <row-size-bytes> <storage-gb> [ttl/s] [pitr true|false] [--rows-per-read N] | tee /tmp/keyspaces-calc.json

# Same workload in 3 DCs, PITR on each by default; optional per-DC reads
npx ts-node --require tsconfig-paths/register --project tsconfig.scripts.json "${CLAUDE_SKILL_DIR}/../../../scripts/calculate.ts" <region> <reads/s> <writes/s> <row-size-bytes> <storage-gb> [ttl/s] --dc-count 3 [--reads-override 'dc-1=500,dc-2=500,dc-3=100'] [--no-pitr] | tee /tmp/keyspaces-calc.json

cat /tmp/keyspaces-calc.json
```

### Mode 2 — Cassandra diagnostic files

**Pipeline (always in this order):** (1) **Gather** — Step 0 + **Required diagnostic files** (below) → (2) **Parse** — Step 1+ through the `parse-cassandra.ts` command → (3) **Present JSON** → (4) **Optional PDF** — **PDF reporting (optional)**.

#### Step 0 — Intake and capture

Collect from **one node per datacenter**, not every node. `parse-cassandra.ts` scales each datacenter's sample by that datacenter's node count from `nodetool status`. Keep each cluster in one directory:

```
<cluster>/
├── status.txt                  nodetool status        (collected first)
├── schema.cql                  DESCRIBE SCHEMA        (once per cluster)
└── <datacenter name>/          one folder per DC, captured on ONE node in that DC
    ├── tablestats.txt          nodetool tablestats    (required)
    ├── info.txt                nodetool info          (required)
    ├── prepared_statements.ndjson   tools/prepared-statements-sampler.sh (recommended)
    └── rowsize.txt             tools/row-size-sampler.sh (optional, once per cluster is enough)
```

Work through these in order:

1. **Directory already provided?** If the user supplies a folder in this layout (or a flat folder with one `tablestats` and one `info`), check it with the detectors from Step 1+. If `status.txt` and every datacenter's `tablestats` + `info` are present, go to **Step 1+** with `--dir`. Otherwise continue below for the missing pieces.
2. **`nodetool status` first.** Ask for `nodetool status > status.txt` from any node before anything else. Read it to learn the datacenters and their node counts, and tell the user what you found.
3. **Region per datacenter.** A datacenter named after an AWS Region code (e.g. `us-east-1`) is priced in that Region. For every other datacenter, ask which AWS Region it should be priced in. Don't guess. Pass the answers as `--dc-region 'dc1=us-east-1,dc2=eu-west-1'`.
4. **Connection details (once, before any remote capture):** host, port, optional `-u` / `-p`, optional `--ssl`. Reuse them for every `cqlsh` and `./tools/...` command. If the workload is already on Amazon Keyspaces, use `cassandra.<region>.amazonaws.com 9142 --ssl`.
5. **Schema once:** `cqlsh <host> <port> [<auth>] -e 'DESCRIBE SCHEMA' > schema.cql`, saved at the cluster root.
6. **One datacenter at a time.** Name the datacenter to collect next. On one node in that datacenter, run `nodetool tablestats` and `nodetool info`, plus `./tools/prepared-statements-sampler.sh` pointed at that node (`system.prepared_statements` is local to each node). Once per cluster, run `./tools/row-size-sampler.sh`. Save the files into `<cluster>/<datacenter name>/`. The folder name must match the datacenter name in `nodetool status`. Files can arrive one by one or as a zip with one folder per datacenter.
7. **Track progress:** after each datacenter, say which datacenters are done and which are still pending.
8. **Only one datacenter sampled?** That's enough to estimate. A single `tablestats` + `info` sample, in one datacenter folder or flat in the cluster directory, applies to **every datacenter its keyspaces replicate to**, based on the `schema.cql` replication. Reads, writes, storage and TTL are all applied, and each datacenter is priced in its own Region. Tell the user, and offer to collect the other datacenters if their traffic differs. If **several** datacenters are sampled, each one uses its own sample. Any datacenter without a sample falls back, per keyspace, to the first sampled datacenter in `nodetool status` order. The JSON lists these in `fallback_datacenters`; tell the user which datacenters use another datacenter's sample. **`tablestats` and `info` are mandatory** for at least one datacenter; without them, use **Mode 1**. **`tco`** is optional (TCO comparison only).

#### Required diagnostic files

Each **ID** matches a `parse-cassandra.ts` flag (`--<id>`) when you pass paths explicitly. **If missing — ask** is the prompt when the capture cannot be produced; **Default / escalation** is what to do next (see Step 1+).

| ID | Captures | Run | Output | Command | If missing — ask | Default / escalation |
|---|---|---|---|---|---|---|
| `tablestats` | Live space + column-family details | one node per DC | `tablestats.txt` | `nodetool tablestats > tablestats.txt` | — | **Mandatory.** Recapture from the cluster. **When absent:** use **Mode 1** if it still cannot be obtained — `parse-cassandra.ts` exits without `--tablestats`. |
| `info` | DC, host id, uptime → derives reads/writes per second from cumulative counters | one node per DC (same node as `tablestats`) | `info.txt` | `nodetool info > info.txt` | — | **Mandatory.** **When absent:** use **Mode 1** — diagnostic pricing requires `nodetool info` alongside `tablestats` (RPS from cumulative counters). |
| `status` | DC list and node count per DC (`parseNodetoolStatus` in `ParsingHelpers.ts`) | **first**, once on any node | `status.txt` | `nodetool status > status.txt` | How many **datacenters** (or target regions) are in the cluster? How many **Cassandra nodes** in each? | **`nodetool status` is authoritative** for how many datacenters are priced. Prefer capturing it. **`--dir` classification** also picks the **first file whose contents look like** `nodetool status` (e.g. **`nodetool.txt`**) when no `status.txt`-style name matched. If the file is missing, use answered topology **or** infer from **`info`** files grouped by DC (see `parse-cassandra.ts`). **Multi-DC:** Collect **one folder per datacenter** (Step 0). Each datacenter is then priced from its own sample in its own Region (`bundle_mode: "perDc"`). If **only one** logical bundle exists but **status shows multiple DCs**, `parse-cassandra.ts` uses **`bundle_mode: "shared"`**: aggregate once from the measured DC, **clone** that result per status DC, then use **`DESCRIBE SCHEMA`** (NetworkTopologyStrategy) to **drop keyspaces with no replica** on each DC and to **map** status DC names (e.g. `*-seeds`) to schema DC keys. **Reads** are applied too: with one sample, `--infer-reads` defaults to **`same`**. Use `zero` or `measured` to keep reads only on the sampled DC. **Writes, storage, and TTL** mirror the sampled aggregate where the keyspace replicates. **`--reads-override 'dc-a=100,dc-b=50'`** still adjusts **reads/s** per status DC name (`name=value`, comma-separated). If topology still cannot be established, use **Mode 1**. |
| `schema` | DDL — source of truth for compatibility (Mode 3 and the auto-compat block in Mode 2) | once | `schema.cql` | `cqlsh <host> <port> [<auth>] -e 'DESCRIBE SCHEMA' > schema.cql` | What **replication factor** do you use for application keyspaces (per DC)? | **When absent:** parser uses replication factor **3** internally (`REPLICATION_FACTOR`); ask the question so the customer’s intent matches the estimate. |
| `rowsize` | Average row size per table and per materialized view (low-rate live sample) | once | `rowsize.txt` | `./tools/row-size-sampler.sh <host> <port> [<auth>] > rowsize.txt` | — | **1024** bytes (1 KB). No further questions. |
| `prepared` | Prepared statements — drives compatibility (LWT-in-batch, aggregations) and the `USING TTL` pricing signal (recommended) | one node per DC (point the sampler at that node) | `prepared_statements.ndjson` | `./tools/prepared-statements-sampler.sh <host> <port> [<auth>] > prepared_statements.ndjson` | — | Omit `--prepared`. No further questions. |
| `tco` | EC2 instance + storage costs for self-managed Cassandra (optional — TCO comparison in JSON/PDF) | one **cluster-wide** sample is typical | `tco.json` | `python tools/cassandra_tco_helper.py <instance-id> --region <region> > tco.json` | — | Omit if not needed. **`parse-cassandra.ts --tco <path>`** attaches that JSON as **`report_data.tco._cluster`**: per-node fields are multiplied by **total node count from status**, **`operations`** once. For UI uploads, per-DC maps are still supported. |

`<auth>` shorthand = `[-u user] [-p pass] [--ssl]`. Omit when the cluster has no authentication or TLS.

**Mode 2 vs Mode 1:** Diagnostic pricing requires **`tablestats` and `info`** (`nodetool tablestats` / `nodetool info`). Without either capture, use **Mode 1**. Without **`nodetool status`**, ask the topology questions in the **`status`** row; if the customer cannot answer and you cannot infer DC/node counts from **`info`**, use **Mode 1**.

#### Step 1+ — Run the parser, fill gaps, emit JSON

1. **Prepare paths:** Prefer `--dir <path>` so every file in the folder is auto-classified — one detector per **ID** (`isTablestatsFile`, `isStatusFile`, `isInfoFile`, `isRowSizeFile`, `isSchemaFile`, `isTcoFile`, `isPreparedStatementsFile` in `ParsingHelpers.ts`). Otherwise pass explicit `--tablestats`, `--status`, `--info`, `--rowsize`, `--schema`, `--prepared` (repeat `--info` per node if needed).
2. **Mode 1 gate and gaps:** Before running the parser, apply the **Required diagnostic files** table. **Without `tablestats` or without any `info` file, stop — use Mode 1** (do not invoke `parse-cassandra.ts`). For every other **ID**, use **If missing — ask** and **Default / escalation** (topology/`status`, **`schema`** RF / internal default 3, **`rowsize`** 1024 bytes, omit **`prepared`** / **`tco`** with no extra questions).
3. **Region:** A Region is chosen per datacenter, in this order:
   - `--dc-region <dc>=<region>` (repeatable or CSV)
   - the datacenter name itself, when it's a Region code
   - `--region` (default **`us-east-1`**)

   The JSON reports the result in `regions_by_datacenter`.
4. **Run** the command block, show the JSON, then offer a PDF per **PDF reporting (optional)**.

```bash
# Directory auto-detection
npx ts-node --require tsconfig-paths/register --project tsconfig.scripts.json "${CLAUDE_SKILL_DIR}/../../../scripts/parse-cassandra.ts" --dir <directory> --region <region> [--tco <tco.json>] [--infer-reads same|zero|measured] [--pitr | --no-pitr] [--reads-override 'dc1=100,dc2=50'] | tee /tmp/keyspaces-calc.json

# Individual files
npx ts-node --require tsconfig-paths/register --project tsconfig.scripts.json "${CLAUDE_SKILL_DIR}/../../../scripts/parse-cassandra.ts" --region <region> --tablestats <path> [--status <path>] [--info <path>] [--rowsize <path>] [--schema <path>] [--prepared <path>] [--tco <tco.json>] [--infer-reads same|zero|measured] [--pitr | --no-pitr] [--reads-override 'dc1=100,dc2=50'] | tee /tmp/keyspaces-calc.json

# Per-datacenter folders (status.txt + schema.cql at the root, one sub-folder per DC)
npx ts-node --require tsconfig-paths/register --project tsconfig.scripts.json "${CLAUDE_SKILL_DIR}/../../../scripts/parse-cassandra.ts" --dir <cluster> --region us-east-1 --dc-region 'dc-eu=eu-west-1' | tee /tmp/keyspaces-calc.json

# Example — multi-DC folder with status in nodetool.txt, one bundle, optional TCO sample
npx ts-node --require tsconfig-paths/register --project tsconfig.scripts.json "${CLAUDE_SKILL_DIR}/../../../scripts/parse-cassandra.ts" --dir ~/Downloads/NV --region us-east-1 --tco ~/Downloads/NV/output.json | tee /tmp/keyspaces-nv.json

cat /tmp/keyspaces-calc.json
```

- **`bundle_mode`:** reported in the JSON.
  - `perDc` with per-DC folders: each datacenter uses its own `tablestats` / `info` / `rowsize`, scaled by its node count in status. Prepared statements from every folder are merged.
  - `shared`: one `tablestats` bundle plus **multi-DC status**. Aggregates are **cloned** per DC and **schema-filtered** by replication (not a global **×N** scale on raw tablestats).
- **PITR:** defaults **on** for every DC whenever more than one datacenter is priced (shared sample or per-DC folders), unless **`--no-pitr`**. For a single DC, it stays **off** unless **`--pitr`**.

**After a successful run**

- **Multi-cluster / re-runs:** Write each estimate to its own JSON path, then one `generate-pdf.ts` with multiple `--input` if they want a single comparison PDF (**PDF reporting (optional)**).
- **Flags:** **`--views ks.view1,ks.view2` | `all`** (materialized views carried over as tables; default `all`; unlisted views aren't priced). **`--view-writes own | unlogged-batch`** (view writes from the view's own tablestats counters, the default, or from the base table's writes when the view is written in an `UNLOGGED BATCH` with its base row). **`--pitr-override 'dc1=on,dc2=off'`** (PITR per datacenter; reads already differ per datacenter through each sample and `--reads-override`). **`--rows-per-read N`** (rows returned per read for every keyspace; default 1, since tablestats counts requests, not rows). Multiple `--info` (one per node). **`--tco`** (cluster sample → `report_data.tco._cluster`). **`--infer-reads`** sets reads for DCs without their own `info`. It defaults to **`same`** with one sample (the sample applies everywhere) and to **`zero`** otherwise. **`--reads-override`** and **`--no-pitr`** / **`--pitr`** as above. `--dir` plus explicit flags: explicit wins.
- **`compatibility` in JSON:** If `schema` or `prepared` was supplied (`--schema` / `--prepared` or detected in `--dir`), output includes `compatibility`: `{ has_issues, summary, details }`, with `summary` / `details` split into `schema` (DDL) and `query_patterns` (prepared statements). If `has_issues`, use Mode 3 display rules alongside pricing.

#### Capturing prepared statements

The **`prepared`** row in the table reads `system.prepared_statements` and emits one JSON object per line (stdout → file). Pass that file as `--prepared`. It affects output in two ways:

1. **Compatibility:** LWT inside `BEGIN UNLOGGED BATCH`, aggregates (`COUNT` / `MIN` / `MAX` / `SUM` / `AVG`), and (with `schema`) calls to user-declared UDFs.
2. **Pricing:** `INSERT … USING TTL` / `UPDATE … USING TTL` marks those tables as fully TTL-driven for write accounting even without `default_time_to_live` in DDL. Tables that already have `default_time_to_live` still follow the existing `rowsize`-based TTL path.

### Display results (Modes 1 & 2)

**Showing the formulas.** When the user asks how a number was calculated, or wants the math written out, re-run the same command with `--explain`. Show the `formulas` lines verbatim, one block per datacenter and keyspace. The lines cover:
- **Mode 2 only:** the conversion (sampled rate × N ÷ RF)
- units per operation
- each cost dimension with its price
- the totals and savings plans

The scripts generate every number from the same functions that priced the estimate. Don't recompute or round them yourself. For the rule behind a line, cite `keyspaces-pricing-rules.md`.

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
  - **Materialized views (`CREATE MATERIALIZED VIEW`)**: not supported. Carry each view over as a regular table that the application writes. The recommended pattern writes the view row together with the base row in an `UNLOGGED BATCH`, which costs the same as the individual writes. It's more resilient than Cassandra, because the view table can have PITR (Cassandra backups don't cover views). Keeping the view in sync with a CDC stream is an alternative, but its cost isn't modeled. Mode 2 prices every view by default, from its own tablestats counters. Use `--view-writes unlogged-batch` to price view writes as the base table's writes instead, and `--views ks.v1,ks.v2` to keep only some views. Rules: section 4.6 of `keyspaces-pricing-rules.md`.
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

#### Step 2 — Column types

List each table's column types exactly as declared (e.g. `UUID`, `VARCHAR(100)`, `TEXT`, `DECIMAL(10,2)`). Don't add up bytes yourself. Put the types in the spec's `columns` and `estimate-workload.ts` sums them using the type table in section 3.2 of `keyspaces-pricing-rules.md` (for example, `VARCHAR(n)` is n bytes, `TEXT` is 64, `UUID` is 16, `BLOB` is 512).

#### Step 3 — Gather workload inputs

If not provided, ask for:
- **Rows per table**
- **Reads/s** and **writes/s** per table or combined
- **Rows per read** for queries that return several rows
- **AWS Region(s)** (default: `us-east-1`)

#### Step 4 — Model the 3 strategies as spec tables

Each option is a list of Keyspaces tables. For each table, decide its columns, `rows`, `reads_per_second` and `writes_per_second`. These are modeling decisions. The script computes storage, row size, keyspace sums and cost.

**Option A — Full Denormalization.** Merge all FK-related tables into one table.
- **Columns:** every unique column across the tables (FK columns once).
- **`rows`:** the merged row count. For a one-to-many chain this is the child-side row count; state the number you use and why.
- **Reads / writes:** the sum of the original tables' reads and writes.
- **CQL:** single merged table; partition key = the FK column matching the access query; clustering key = the child table's PK.

**Option B — Normalized with Lookup Tables.** Keep the original tables and add a lookup table per FK for application-side joins.
- **Original tables:** one spec table each, with their own columns, rows, reads and writes.
- **Lookup table per FK** `(source.col → target.pk)`, named `target_by_source`:
  - **Columns:** the FK column + the target PK column only.
  - **`rows`:** the target row count.
  - **Writes:** the source table's insert rate (1 lookup write per insert).
  - **Reads:** the FK lookups the queries perform.
- **CQL:** the original tables unchanged, plus one lookup table per FK.

**Option C — Denormalized with Reverse Index.** The merged table from Option A, plus a reverse index table for each non-PK FK column.
- **Merged table:** the same spec table as Option A.
- **Reverse index per non-PK FK column:** the same columns, rows and writes as the merged table (a full copy); reads = the queries that use that column. Partition key = the FK column; clustering key = the merged PK.
- **CQL:** the merged table plus one reverse index table per non-PK FK column.

#### Step 5 — Price each option

Write each option's Step 4 tables as a **workload spec**: `columns`, `rows`, `reads_per_second`, `writes_per_second` and, where queries read several rows, `rows_per_read`. Merged, lookup and reverse-index tables are separate spec tables. The script does all the arithmetic (row bytes from `columns`, storage, keyspace sums, cost).

```bash
cat > /tmp/keyspaces-sql-optionA-spec.json <<'JSON'
{"source": "sql-model option A", "regions": ["<region>"], "pitr": false,
 "keyspaces": [{"name": "<keyspace>", "tables": [
   {"name": "<table>", "columns": ["UUID", "VARCHAR(100)", "..."], "rows": <rows>, "reads_per_second": <r>, "writes_per_second": <w>}]}]}
JSON
npx ts-node --require tsconfig-paths/register --project tsconfig.scripts.json "${CLAUDE_SKILL_DIR}/../../../scripts/estimate-workload.ts" --spec /tmp/keyspaces-sql-optionA-spec.json [--explain] | tee /tmp/keyspaces-sql-optionA.json
# Repeat for B → /tmp/keyspaces-sql-optionB.json, C → /tmp/keyspaces-sql-optionC.json
```

Extract `provisioned.total`, `on_demand.total`, `provisioned_savings_plan.total` from each JSON. With `--explain`, `formulas` shows each table's storage (rows × row bytes ÷ 1,073,741,824), the keyspace sums and the pricing lines.

Write each option's CQL (`CREATE KEYSPACE` plus every `CREATE TABLE` from Step 4) to `/tmp/keyspaces-sql-optionA.cql`, `…B.cql`, `…C.cql`, using the same keyspace and table names as the spec. Then offer a CloudFormation template per option, built from that option's estimate (see "CloudFormation template (target data model)"). Give the recommended option's template at minimum.

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

### CloudFormation template (target data model)

`generate-cfn.ts` turns CQL into a CloudFormation template (`AWS::Cassandra::Keyspace`, `AWS::Cassandra::Type`, `AWS::Cassandra::Table`). The CQL is the customer's `DESCRIBE SCHEMA` (Modes 2 and 3) or the data model you produced (Mode 4). Pass the estimate for that model with `--estimate` so the template matches what was priced:

- **Capacity mode:** **on-demand** by default. Pass `--billing-mode provisioned` only when the user asks for provisioned capacity. The estimate's cheaper mode is a recommendation to report, not a template setting.
- **Provisioned capacity** (with `--billing-mode provisioned`): RCU/WCU per keyspace, using the same unit rounding and default target utilization (`DEFAULT_TARGET_UTILIZATION`, 70%) as the pricing formulas. The largest datacenter is used, the capacity is split evenly across the keyspace's tables, and auto scaling is added (target = the default target utilization, 70%; max 2× the starting capacity).
- **PITR:** on wherever the estimate priced backups.
- **Regions:** datacenter → AWS Region comes from the estimate, so `MULTI_REGION` keyspaces get real Region codes. A datacenter without a Region is reported, and its keyspace is generated as single-Region.
- **Unsupported features:** left out, and reported on stderr as `WARNING:` lines.

```bash
# Mode 2 — cluster schema + cluster estimate
npx ts-node --require tsconfig-paths/register --project tsconfig.scripts.json "${CLAUDE_SKILL_DIR}/../../../scripts/generate-cfn.ts" --schema <cluster>/schema.cql --estimate /tmp/keyspaces-calc.json --output /tmp/keyspaces-cfn.yaml

# Mode 4 — one template per option
npx ts-node --require tsconfig-paths/register --project tsconfig.scripts.json "${CLAUDE_SKILL_DIR}/../../../scripts/generate-cfn.ts" --schema /tmp/keyspaces-sql-optionB.cql --estimate /tmp/keyspaces-sql-optionB.json --output /tmp/keyspaces-sql-optionB-cfn.yaml

# Mode 3 — schema only (on-demand, no sizing)
npx ts-node --require tsconfig-paths/register --project tsconfig.scripts.json "${CLAUDE_SKILL_DIR}/../../../scripts/generate-cfn.ts" --schema schema.cql --output /tmp/keyspaces-cfn.yaml
```

Flags:
- `--estimate <json>`: shape the template from an estimate.
- `--dc-region dc=region`: Region mapping when there's no estimate, or to override it.
- `--billing-mode on_demand|provisioned`: capacity mode (default `on_demand`).
- `--pitr`: PITR on every table.
- `--no-auto-scaling`
- `--format json|yaml`
- `--single-region`
- `--include-system`

Show the user every `WARNING:` line. These cover features left out of the template, datacenters without a Region, which Region the stack must be deployed in, and capacity split evenly across tables. Provisioned capacity comes from sampled averages, so tell the user that auto scaling adjusts it after deployment.

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
- `--output <path>` — output PDF path. Defaults to `./reports/keyspaces-pricing-estimate.pdf` (`reports/` is created automatically if it does not exist).

Behavior:

- **1 input** (stdin or single `--input`): renders the single-estimate report — **multi-DC** runs add a **keyspace × datacenter** cost matrix (on-demand / provisioned per DC plus row totals). The executive summary includes **total Cassandra nodes (all DCs)** and **TCO** rolled up correctly: with **`_cluster`** TCO JSON, per-node costs × **Σ nodes from status**, **operations** once; a short note is shown when a single cluster sample is used.
- **2+ inputs**: renders a consolidated comparison report — title page with a side-by-side comparison summary table (storage, reads/s, writes/s, on-demand/mo, OD+SP/mo, provisioned/mo, prov+SP/mo) followed by a per-estimate section with full input/pricing tables, per-estimate pivot/matrix when applicable, and any compatibility findings.

Use a single multi-input invocation whenever the user has more than one estimate to report — Mode 4 (always 3 strategies), repeated Mode 1 sensitivity runs, or several Mode 2 clusters being compared. Avoid generating a separate PDF per estimate.

---

### Price data (`mcs.json`, `savings-plans.json`, `regions.json`)

When the user asks about the price data, where `savings-plans.json` (or `mcs.json` / `regions.json`) comes from, what a field means, or wants to refresh prices, read section 2.7 of `keyspaces-pricing-rules.md`. It gives each file's source, the exact command that fetches it and the meaning of every field. To refresh:
1. Run `./tools/get-pricing.sh` from the repo root. `mcs.json` is public; the savings-plan and Region calls need AWS credentials and are read-only.
2. Re-run the estimate.
3. Check `manifest.hawkFilePublicationDate` in `mcs.json` and say which price date an estimate used.

### Negative knowledge

Never state these, and correct them when a source or customer assumes them (full list: section 1.4 of `keyspaces-pricing-rules.md`):
- **No Region twice:** a keyspace or table can't be replicated to the same Region twice. Cassandra datacenters that map to one Region are priced as one Region (`merged_datacenters`).
- **No replication factor in pricing:** Keyspaces stores three copies and bills one.
- **Batches don't save writes:** `UNLOGGED BATCH` costs the same as its individual writes; `LOGGED BATCH` costs **2×** per row (prepare + commit).
- **Reads aren't per row:** multi-row reads add up bytes per 1 MB page.
- **No transfer charge** for replication, same-Region traffic or inbound traffic.
- **Free tier not in estimates.**
- **Compatibility is binary.**

### AWS Free Tier

Know it, but never apply it to an estimate. For the **first three months**, starting in the month the account creates its first Amazon Keyspaces resource, each month includes **30 million on-demand write request units, 30 million on-demand read request units and 1 GB of storage** free, with one free tier per payer account. The AWS Free Tier also includes 1 GB of data transfer out per month across AWS services. Estimates are steady-state monthly costs: show the regular totals, and mention the free tier in words when the customer asks about trial or first-month costs. Details: section 2.8 of `keyspaces-pricing-rules.md`.

### Workloads from other databases

When the source is another database (DynamoDB, MongoDB, PostgreSQL, MySQL, …) or monitoring data rather than Cassandra diagnostics, collect per-table metrics and write them as a workload spec. The spec format and conversion formulas are in Part 3 of `keyspaces-pricing-rules.md`:
- **Requests:** logical requests per second. Set `request_copies` when the metrics count every replica.
- **Rows per read:** for queries that return several rows.
- **Storage:** `rows` with `avg_row_bytes`, or `stored_bytes` with `compression_ratio` and `storage_copies`.
- **Regions:** every Region the keyspaces replicate to.

Then run `estimate-workload.ts --spec <file> [--explain]`. Its output works with `generate-pdf.ts` and `generate-cfn.ts --estimate` like any other estimate.

### Pricing and conversion rules

[`keyspaces-pricing-rules.md`](keyspaces-pricing-rules.md) (next to this file) describes what the code implements, in five parts:

| Part | Covers | Use for |
|---|---|---|
| 1. Shared: the Keyspaces workload | The workload fields pricing needs (per keyspace per Region), shared constants, rules that apply to every source | Always |
| 2. Keyspaces pricing | Workload → cost: unit rounding (multi-row reads per 1 MB page), cost formulas, savings plans, CloudFormation sizing, what isn't modeled | Every estimate |
| 3. Converting other sources | Workload spec → workload (`estimate-workload.ts`) and round numbers (`calculate.ts`) | Modes 1 and 4, any non-Cassandra source |
| 4. Apache Cassandra conversion | Diagnostics → workload: per-node rates, N ÷ RF scaling, row size and TTL, datacenters → Regions, self-managed TCO | Mode 2 |
| 5. End to end | The same workload from Cassandra and from PostgreSQL tables, priced once | Explaining an estimate |

Use the file to explain an estimate or answer "how is X calculated?". To show the math for a specific estimate, re-run with `--explain` (see "Showing the formulas"). Never recompute prices by hand. If the file and the code disagree, the code is correct.
