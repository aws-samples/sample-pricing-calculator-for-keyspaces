# Amazon Keyspaces Pricing Calculator

Invoke the `amazon-keyspaces-skill` skill (`.claude/skills/amazon-keyspaces-skill/SKILL.md`) and follow its instructions.

The skill supports four modes:

1. **Manual inputs** — user provides region, reads/s, writes/s, row size, storage, TTL, and PITR.
2. **Cassandra diagnostic files** — user provides `nodetool` output, schema, or a directory to scan. When a schema is present, a compatibility report is included alongside the pricing.
3. **Compatibility check** — user provides a Cassandra CQL schema (file or pasted) and wants to know what features won't work on Amazon Keyspaces.
4. **SQL → Keyspaces translation** — user provides SQL DDL; the skill prices 3 data-model strategies.

Arguments (optional, passed through to Mode 1): `[region] [reads/s] [writes/s] [row-size-bytes] [storage-gb] [ttl/s] [pitr]`

Example: `/amazon-keyspaces-skill us-east-1 1000 500 4096 100 0 true`

If the user provides arguments, use Mode 1 directly. Otherwise, let the skill's logic decide the correct mode based on the user's inputs.
