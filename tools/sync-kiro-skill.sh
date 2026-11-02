#!/usr/bin/env bash
# sync-kiro-skill.sh
#
# Regenerate .kiro/skills/amazon-keyspaces-skill/ (SKILL.md + supporting files) from the Claude Code
# skill, which is the source of truth. The only difference is how scripts are referenced: Claude resolves them from the
# skill directory ("${CLAUDE_SKILL_DIR}/../../../scripts/x.ts"), Kiro from the project root (scripts/x.ts).
#
# Usage:
#   ./tools/sync-kiro-skill.sh

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SRC="${REPO_ROOT}/.claude/skills/amazon-keyspaces-skill/SKILL.md"
DEST="${REPO_ROOT}/.kiro/skills/amazon-keyspaces-skill/SKILL.md"

mkdir -p "$(dirname "${DEST}")"
sed -E 's#"\$\{CLAUDE_SKILL_DIR\}/\.\./\.\./\.\./(scripts/[A-Za-z0-9_-]+\.ts)"#\1#g' "${SRC}" > "${DEST}"
echo "Wrote ${DEST}"

# Supporting files (rules references, ...) are identical for both agents. Remove Kiro-only leftovers
# so renamed or deleted files don't linger.
for f in "$(dirname "${DEST}")"/*; do
  name="$(basename "$f")"
  [ "${name}" = "SKILL.md" ] && continue
  [ -e "$(dirname "${SRC}")/${name}" ] || { rm -f "$f"; echo "Removed stale ${name}"; }
done

for f in "$(dirname "${SRC}")"/*; do
  [ "$(basename "$f")" = "SKILL.md" ] && continue
  cp "$f" "$(dirname "${DEST}")/"
  echo "Copied $(basename "$f")"
done
