#!/usr/bin/env bash
set -euo pipefail
HERE="$(cd "$(dirname "$0")/.." && pwd)"
SRC="$HERE/nats/nats.conf.tpl"
DST="$HERE/nats/nats.conf"

if [[ "${EC_ENABLE_MQTT:-0}" == "1" ]]; then
  # uncomment the block between MQTT_BEGIN and MQTT_END
  awk '/# MQTT_BEGIN/{flag=1;next} /# MQTT_END/{flag=0;next} flag{sub(/^# /,"")} {print}' \
    "$SRC" > "$DST"
  echo "Rendered $DST with MQTT ingress ENABLED (port 1883 exposed)."
else
  cp "$SRC" "$DST"
  echo "Rendered $DST with MQTT ingress DISABLED (default)."
fi
