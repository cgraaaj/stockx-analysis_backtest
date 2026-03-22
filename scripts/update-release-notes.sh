#!/usr/bin/env bash
# Append a new release section to RELEASE_NOTES.md with the current date
# and commits since the last release. Used by CI on push to master; can
# also be run locally before or after a push.
#
# Usage: ./scripts/update-release-notes.sh [--dry-run]

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RELEASE_NOTES="${PROJECT_ROOT}/RELEASE_NOTES.md"

cd "$PROJECT_ROOT"

# Find the most recent release date from RELEASE_NOTES.md (last ## YYYY-MM-DD)
LAST_RELEASE=""
if [[ -f "$RELEASE_NOTES" ]]; then
  LAST_RELEASE=$(grep -E '^## [0-9]{4}-[0-9]{2}-[0-9]{2}' "$RELEASE_NOTES" | tail -1 | sed 's/^## //' | awk '{print $1}')
fi

TODAY=$(date +%Y-%m-%d)

# Get commits since last release
if [[ -n "$LAST_RELEASE" ]]; then
  SINCE=$(git rev-list -n1 --before="${LAST_RELEASE} 23:59" HEAD 2>/dev/null || true)
  if [[ -n "$SINCE" ]]; then
    COMMITS=$(git log --oneline --no-merges "${SINCE}..HEAD" 2>/dev/null || true)
  else
    COMMITS=$(git log --oneline -20)
  fi
else
  COMMITS=$(git log --oneline -20)
fi

# Build new section
NEW_SECTION="## ${TODAY}
"
if [[ -n "$COMMITS" ]]; then
  NEW_SECTION="${NEW_SECTION}
$(echo "$COMMITS" | sed 's/^/- /')
"
fi

if [[ "${1:-}" == "--dry-run" ]]; then
  echo "$NEW_SECTION"
  exit 0
fi

# Skip if entry for today already exists
if [[ -f "$RELEASE_NOTES" ]] && grep -q "^## ${TODAY}" "$RELEASE_NOTES"; then
  echo "Release entry for ${TODAY} already exists; skipping."
  exit 0
fi

# Append new section
if [[ ! -f "$RELEASE_NOTES" ]]; then
  echo -e "# Release Notes\n\n---\n\n${NEW_SECTION}" > "$RELEASE_NOTES"
else
  echo "$NEW_SECTION" >> "$RELEASE_NOTES"
fi

echo "Updated $RELEASE_NOTES with release ${TODAY}"
