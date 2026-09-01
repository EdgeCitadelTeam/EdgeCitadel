#!/bin/sh
set -eu

# Compatibility wrapper. The unified CLI owns enrollment now.
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
    echo "Usage: ./add-agent.sh <node-id> [reachable-core-host]" >&2
    echo "Preferred: ./scripts/edgecitadel invite --node-id <node-id> --host <reachable-core-host>" >&2
    exit 2
fi

HOST=${2:-$(hostname)}
echo "Deprecated wrapper: use ./scripts/edgecitadel invite directly." >&2
exec "$ROOT/scripts/edgecitadel" invite --node-id "$1" --host "$HOST"
