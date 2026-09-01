#!/bin/sh
set -eu

# Compatibility wrapper. Raw broker secrets and the former MQTT setup path are
# intentionally unsupported; use a short-lived ecjoin:// invitation.
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
if [ "$#" -ne 1 ]; then
    echo "Usage: ./join.sh 'ecjoin://...'" >&2
    echo "Preferred: ./scripts/edgecitadel join 'ecjoin://...'" >&2
    exit 2
fi

echo "Deprecated wrapper: use ./scripts/edgecitadel join directly." >&2
exec "$ROOT/scripts/edgecitadel" join "$1"
