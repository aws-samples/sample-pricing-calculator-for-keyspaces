#!/usr/bin/env bash
# package-skill.sh
#
# Build a standalone distribution zip of the amazon-keyspaces-skill for both
# Claude Code (.claude/skills/) and Kiro (.kiro/skills/), bundled with the
# TypeScript CLI scripts, the src/calculator/ core module, the pricing data
# files, and a minimal package.json so it can run on any machine with Node.js.
#
# Usage:
#   ./package-skill.sh [version]
#
# If version is omitted, today's date (YYYYMMDD) is used. Output lands in
# dist/amazon-keyspaces-skill-<version>.zip.

set -euo pipefail

VERSION="${1:-$(date +%Y%m%d)}"
DIST_NAME="amazon-keyspaces-skill-${VERSION}"
REPO_ROOT="$(cd "$(dirname "$0")" && pwd)"
DIST_ROOT="${REPO_ROOT}/dist"
STAGING="${DIST_ROOT}/${DIST_NAME}"
ZIP_FILE="${DIST_ROOT}/${DIST_NAME}.zip"

cd "${REPO_ROOT}"

echo "Packaging ${DIST_NAME}..."

rm -rf "${STAGING}" "${ZIP_FILE}"
mkdir -p "${STAGING}"

# --- 1. Claude skill ------------------------------------------------------
echo "  [1/7] Claude skill"
mkdir -p "${STAGING}/.claude/skills"
cp -R .claude/skills/amazon-keyspaces-skill "${STAGING}/.claude/skills/"

# --- 2. Kiro skill --------------------------------------------------------
echo "  [2/7] Kiro skill"
mkdir -p "${STAGING}/.kiro/skills"
cp -R .kiro/skills/amazon-keyspaces-skill "${STAGING}/.kiro/skills/"

# --- 3. CLI scripts referenced by both SKILL.md files ---------------------
echo "  [3/7] CLI scripts"
mkdir -p "${STAGING}/scripts"
cp scripts/calculate.ts \
   scripts/generate-pdf.ts \
   scripts/parse-cassandra.ts \
   scripts/check-compatibility.ts \
   "${STAGING}/scripts/"

# tools/ — sampler shell scripts and helpers referenced by SKILL.md
mkdir -p "${STAGING}/tools"
cp tools/* "${STAGING}/tools/"
chmod +x "${STAGING}/tools/"*.sh

# --- 4. Calculator core module + pricing data -----------------------------
echo "  [4/7] src/calculator/ core"
mkdir -p "${STAGING}/src/calculator/data"
cp src/calculator/*.ts src/calculator/*.js "${STAGING}/src/calculator/"
cp src/calculator/data/* "${STAGING}/src/calculator/data/"

# --- 5. TypeScript config for the scripts --------------------------------
echo "  [5/7] tsconfig.scripts.json"
cp tsconfig.scripts.json "${STAGING}/"

# --- 6. Minimal package.json (runtime deps only; no React/UI) -------------
echo "  [6/7] package.json + README.md"
cat > "${STAGING}/package.json" <<'EOF'
{
  "name": "amazon-keyspaces-skill",
  "version": "1.0.0",
  "description": "Standalone distribution of the Amazon Keyspaces skill for Claude Code and Kiro.",
  "private": true,
  "scripts": {
    "calculate": "ts-node --require tsconfig-paths/register --project tsconfig.scripts.json scripts/calculate.ts",
    "parse-cassandra": "ts-node --require tsconfig-paths/register --project tsconfig.scripts.json scripts/parse-cassandra.ts",
    "generate-pdf": "ts-node --require tsconfig-paths/register --project tsconfig.scripts.json scripts/generate-pdf.ts",
    "check-compatibility": "ts-node --require tsconfig-paths/register --project tsconfig.scripts.json scripts/check-compatibility.ts"
  },
  "dependencies": {
    "jspdf": "^2.5.2",
    "jspdf-autotable": "^3.8.4",
    "ts-node": "^10.9.2",
    "tsconfig-paths": "^3.15.0",
    "typescript": "^5.0.0"
  },
  "devDependencies": {
    "@types/node": "^20.0.0"
  }
}
EOF

cat > "${STAGING}/README.md" <<'EOF'
# Amazon Keyspaces Skill — Distribution

Standalone bundle of the Amazon Keyspaces skill (pricing calculator plus
schema compatibility check) for **Claude Code** and **Kiro**, plus the
TypeScript CLI scripts they call.

## Install

1. Unzip this archive into your project root (or any working directory).
2. Install the runtime dependencies:

   ```bash
   npm install
   ```

## Layout

```
.claude/skills/amazon-keyspaces-skill/SKILL.md   # Claude Code skill
.kiro/skills/amazon-keyspaces-skill/SKILL.md     # Kiro skill
scripts/
  calculate.ts             # Mode 1: manual inputs
  parse-cassandra.ts       # Mode 2: Cassandra diagnostic files
  check-compatibility.ts   # Mode 3: schema + prepared-statement compatibility
  generate-pdf.ts          # PDF report generator
tools/
  get-pricing.sh                  # Refresh src/calculator/data/* from AWS APIs
  row-size-sampler.sh             # Sampler: per-table average row size
  prepared-statements-sampler.sh  # Sampler: system.prepared_statements export
  cassandra_tco_helper.py         # TCO comparison helper (per EC2 host)
src/calculator/         # Framework-agnostic pricing core
  PricingFormulas.ts
  PricingData.js
  ParsingHelpers.ts
  CreatePDFReport.ts
  Constants.js
  index.ts
  data/                 # mcs.json, regions.json, savings-plans.json
tsconfig.scripts.json
package.json
```

## Use with an agent

Once unzipped into your project and `npm install` is run, Claude Code and Kiro
will auto-discover the skill via `.claude/skills/` and `.kiro/skills/`.
The skills reference the scripts via relative paths so everything just works.

## Use from the command line

```bash
# Mode 1 — manual inputs
npm run calculate -- us-east-1 1000 500 1024 100 0 false \
  | tee /tmp/keyspaces-calc.json \
  | npm run generate-pdf

# Mode 2 — Cassandra diagnostic files (directory auto-detection)
npm run parse-cassandra -- --dir /path/to/diagnostics --region us-east-1 \
  | tee /tmp/keyspaces-calc.json \
  | npm run generate-pdf
```

## Refresh pricing data

The files under `src/calculator/data/` are snapshotted at build time. To
refresh them, clone the upstream repo and run `./tools/get-pricing.sh` there, then
copy the updated JSON files back into this distribution.
EOF

# --- 7. Zip up ------------------------------------------------------------
echo "  [7/7] Creating zip"
( cd "${DIST_ROOT}" && zip -qr "${DIST_NAME}.zip" "${DIST_NAME}" )

echo ""
echo "Wrote ${ZIP_FILE}"
echo ""
echo "Contents:"
unzip -l "${ZIP_FILE}"
