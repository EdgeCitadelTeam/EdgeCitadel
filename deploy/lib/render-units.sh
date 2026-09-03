#!/usr/bin/env bash
# deploy/lib/render-units.sh — install systemd unit files from templates.
#
# Usage: ./render-units.sh [--dry-run]
#
# For each .service.in in deploy/systemd/, renders to /etc/systemd/system/
# (envsubst — currently no $VARS but reserved for forward-compat).
# Then `systemctl daemon-reload`.
# The host installer owns only the Ollama dependency unit. Agent packages are
# installed and supervised through `edgecitadel agent` and agentd.
#
# Does NOT start any units — that's a separate phase of deploy-host.sh.

set -euo pipefail

LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${LIB_DIR}/platform.sh"
[[ "${1:-}" == "--dry-run" ]] && export DRY_RUN=1

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

ALL_UNITS=("ollama")

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

for plugin in "${ALL_UNITS[@]}"; do
  log_info "systemctl enable edgecitadel-${plugin}.service (not starting yet)"
  run systemctl enable "edgecitadel-${plugin}.service"
done

log_info "render-units.sh: complete (${#ALL_UNITS[@]} units installed)"
