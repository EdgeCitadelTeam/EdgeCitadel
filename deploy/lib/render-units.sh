#!/usr/bin/env bash
# deploy/lib/render-units.sh — install systemd unit files from templates.
#
# Usage: ./render-units.sh [--dry-run]
#
# For each .service.in in deploy/systemd/, renders to /etc/systemd/system/
# (envsubst — currently no $VARS but reserved for forward-compat).
# Then `systemctl daemon-reload`.
# Enables units listed in manifest [plugins].enabled (PLUS ollama).
# Leaves units in [plugins].optional_disabled installed-but-disabled.
#
# Does NOT start any units — that's a separate phase of deploy-host.sh.

set -euo pipefail

LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${LIB_DIR}/platform.sh"
PARSE_MANIFEST="${LIB_DIR}/parse-manifest.py"

[[ "${1:-}" == "--dry-run" ]] && export DRY_RUN=1

# Manifest read with explicit exit-code check (per pattern).
_read_manifest() {
  local key="$1"
  local out
  if ! out=$(python3 "$PARSE_MANIFEST" get "$key" --format lines); then
    log_err "manifest read failed for $key"
    exit 1
  fi
  _OUT=()
  while IFS= read -r line; do
    [[ -n "$line" ]] && _OUT+=("$line")
  done <<<"$out"
}

REPO_ROOT="$(cd "${LIB_DIR}/../.." && pwd)"
TEMPLATES_DIR="${REPO_ROOT}/deploy/systemd"
TARGET_DIR="/etc/systemd/system"

if ! is_linux; then
  log_warn "render-units.sh: not linux (this host is $(detect_platform)); use deploy/launchd/ for macos"
  exit 0
fi

# Real install requires root (skipped in dry-run).
if [[ "${DRY_RUN:-0}" != "1" ]]; then
  require_root "$@"
fi

_read_manifest plugins.enabled
enabled=("${_OUT[@]:-}")
_read_manifest plugins.optional_disabled
optional=("${_OUT[@]:-}")
# Always render Ollama too (not in [plugins] because it's not a Python Plugin).
ALL_UNITS=("ollama" "${enabled[@]}" "${optional[@]}")

for plugin in "${ALL_UNITS[@]}"; do
  SRC="${TEMPLATES_DIR}/edgecitadel-${plugin}.service.in"
  DST="${TARGET_DIR}/edgecitadel-${plugin}.service"
  if [[ ! -f "$SRC" ]]; then
    log_warn "no template at $SRC — skipping"
    continue
  fi
  log_info "rendering ${SRC} → ${DST}"
  if [[ "${DRY_RUN:-0}" == "1" ]]; then
    printf "DRY-RUN: envsubst < %s > %s\n" "$SRC" "$DST" >&2
  else
    envsubst < "$SRC" > "$DST"
    chmod 0644 "$DST"
  fi
done

run systemctl daemon-reload

# Enable enabled plugins (we don't START — that's a separate phase).
# Always enable Ollama (it's the dependency of gemma).
for plugin in "ollama" "${enabled[@]}"; do
  log_info "systemctl enable edgecitadel-${plugin}.service (not starting yet)"
  run systemctl enable "edgecitadel-${plugin}.service"
done

# Install but do NOT enable optional_disabled units.
for plugin in "${optional[@]}"; do
  log_info "edgecitadel-${plugin}.service installed but NOT enabled (opt-in by operator)"
done

log_info "render-units.sh: complete (${#ALL_UNITS[@]} units installed)"
