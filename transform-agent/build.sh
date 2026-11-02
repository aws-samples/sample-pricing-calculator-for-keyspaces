#!/usr/bin/env bash
# build.sh
#
# Stage everything the AWS Transform agent container needs from the parent repo:
#   vendor/calculator/*.js  - scripts/*.ts bundled with esbuild (pricing data inlined,
#                             no node_modules needed at runtime)
#   vendor/calculator/cluster-bundle.js - transform-agent/calculator/cluster-bundle.ts (topology +
#                             per-datacenter collection progress)
#   vendor/samplers/        - tools/row-size-sampler.sh + tools/prepared-statements-sampler.sh,
#                             handed to customers as a downloadable artifact
#   skills/amazon-keyspaces-skill/SKILL.md - the Claude Code skill, loaded via Strands AgentSkills
#
# Run before `finch build` / `docker build` / deploy_agent_full_pipeline.
#
# Usage:
#   ./build.sh

set -euo pipefail

AGENT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${AGENT_DIR}/.." && pwd)"
VENDOR="${AGENT_DIR}/vendor/calculator"
SAMPLERS="${AGENT_DIR}/vendor/samplers"
SKILLS="${AGENT_DIR}/skills"
ESBUILD_VERSION="0.25.0"
# Conservative target: python:3.11-slim ships Node.js 18+ (currently 20) via apt.
NODE_TARGET="node18"

rm -rf "${AGENT_DIR}/vendor" "${SKILLS}"
mkdir -p "${VENDOR}" "${SAMPLERS}" "${SKILLS}"

echo "Bundling calculator CLIs -> ${VENDOR}"
cd "${REPO_ROOT}"
for script in calculate parse-cassandra estimate-workload check-compatibility generate-pdf generate-cfn; do
  npx -y "esbuild@${ESBUILD_VERSION}" "scripts/${script}.ts" \
    --bundle --platform=node --target="${NODE_TARGET}" \
    --tsconfig=tsconfig.scripts.json --log-level=warning \
    --outfile="${VENDOR}/${script}.js"
done
npx -y "esbuild@${ESBUILD_VERSION}" "${AGENT_DIR}/calculator/cluster-bundle.ts" \
  --bundle --platform=node --target="${NODE_TARGET}" \
  --tsconfig=tsconfig.scripts.json --log-level=warning \
  --outfile="${VENDOR}/cluster-bundle.js"

echo "Staging customer-side samplers -> ${SAMPLERS}"
cp tools/row-size-sampler.sh tools/prepared-statements-sampler.sh "${SAMPLERS}/"
chmod +x "${SAMPLERS}/"*.sh

echo "Copying amazon-keyspaces-skill -> ${SKILLS}"
cp -R "${REPO_ROOT}/.claude/skills/amazon-keyspaces-skill" "${SKILLS}/"
# Strands parses frontmatter as strict YAML: quote the bracketed argument-hint
# (valid for Claude Code, invalid YAML) or the skill is silently skipped.
sed -i.bak -E 's/^argument-hint: (\[.*)$/argument-hint: "\1"/' "${SKILLS}/amazon-keyspaces-skill/SKILL.md"
rm -f "${SKILLS}/amazon-keyspaces-skill/SKILL.md.bak"

echo "Done."
