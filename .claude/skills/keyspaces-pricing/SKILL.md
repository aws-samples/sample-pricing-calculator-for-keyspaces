---
name: keyspaces-pricing
description: Calculate the estimated monthly cost for Amazon Keyspaces (for Apache Cassandra). Use when the user asks for Keyspaces pricing, cost estimates, or wants to evaluate on-demand vs provisioned capacity. Also use when the user provides Cassandra diagnostic files (nodetool info, status, tablestats, row size sampler, schema).
argument-hint: [region] [reads/s] [writes/s] [row-size-bytes] [storage-gb] [ttl/s] [pitr]
---

## Amazon Keyspaces Pricing Calculator

Two modes. In both cases, pipe output to `generate-pdf.ts` and then `cat` the JSON.

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

### Display results

Show a cluster/inputs summary, a keyspace breakdown (Mode 2), and an on-demand vs provisioned cost table. Include a savings plan row if `savings_plan_available` is true. Recommend the cheaper option. Note the PDF was saved.
