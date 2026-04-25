#!/usr/bin/env bash
# Pulls the latest A2A Agent Card schema from upstream and diffs against
# our vendored copy. Human-review-only — does NOT auto-merge. See ADR-0003.
set -euo pipefail

UPSTREAM="https://raw.githubusercontent.com/a2aproject/A2A/main/specification/json/a2a.json"
VENDORED="$(cd "$(dirname "$0")/.." && pwd)/schemas/agent-card.v1.json"
TMP="$(mktemp)"
trap 'rm -f "$TMP"' EXIT

echo "Fetching A2A schema from $UPSTREAM ..."
curl -fsSL "$UPSTREAM" -o "$TMP"

echo "Diff (upstream → vendored):"
diff -u "$TMP" "$VENDORED" || true
echo
echo "Review the diff and update schemas/agent-card.v1.json by hand if needed."
echo "Do NOT run sed/jq replacement unguarded — our metadata vocabulary is additive."
