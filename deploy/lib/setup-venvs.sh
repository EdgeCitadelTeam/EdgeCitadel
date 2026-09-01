#!/usr/bin/env bash
# deploy/lib/setup-venvs.sh — create per-Plugin Python venvs.
#
# Usage: ./setup-venvs.sh [--dry-run] [--source-dir PATH]
#
# Creates /var/lib/edgecitadel/venvs/<name>/ for each Plugin in
# manifest.toml's [plugins].enabled and .optional_disabled.
# Records sha256 of the Plugin requirements and lockfile at
# <venv>/.requirements.sha so subsequent runs skip pip when unchanged.

set -euo pipefail

LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${LIB_DIR}/platform.sh"

PARSE_MANIFEST="${LIB_DIR}/parse-manifest.py"

DRY_RUN="${DRY_RUN:-0}"
SOURCE_DIR="/opt/edgecitadel"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=1; export DRY_RUN; shift ;;
    --source-dir) SOURCE_DIR="$2"; shift 2 ;;
    *) log_err "unknown arg: $1"; exit 1 ;;
  esac
done

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

VENV_ROOT="/var/lib/edgecitadel/venvs"
PY_VERSION="$(python3 "$PARSE_MANIFEST" get python.version)"
PYTHON_BIN="python${PY_VERSION}"

require_command "$PYTHON_BIN"

_read_manifest plugins.enabled
enabled=("${_OUT[@]:-}")
_read_manifest plugins.optional_disabled
optional=("${_OUT[@]:-}")
ALL_PLUGINS=("${enabled[@]}" "${optional[@]}")

# Real install requires root (skipped in dry-run).
if [[ "${DRY_RUN:-0}" != "1" ]]; then
  require_root "$@"
fi

run mkdir -p "$VENV_ROOT"
run chown edgecitadel:edgecitadel "$VENV_ROOT" 2>/dev/null || true

for plugin in "${ALL_PLUGINS[@]}"; do
  VENV="${VENV_ROOT}/${plugin}"
  PLUGIN_ROOT="${SOURCE_DIR}/plugins/${plugin}"
  REQ="${PLUGIN_ROOT}/edgecitadel_${plugin}_plugin/requirements.txt"

  if [[ ! -f "$REQ" && "$DRY_RUN" != "1" ]]; then
    log_warn "$plugin: requirements.txt not found at $REQ — skipping"
    continue
  fi

  CURRENT_SHA=""
  if [[ -f "$REQ" ]]; then
    CURRENT_SHA="$(sha256sum "$REQ" "${PLUGIN_ROOT}/plugin.lock.json" | sha256sum | cut -d' ' -f1)"
  fi
  RECORDED_SHA=""
  [[ -f "${VENV}/.requirements.sha" ]] && RECORDED_SHA="$(cat "${VENV}/.requirements.sha")"

  if [[ -d "$VENV" && "$CURRENT_SHA" == "$RECORDED_SHA" && -n "$CURRENT_SHA" ]]; then
    log_info "$plugin: venv up-to-date (sha matches)"
    continue
  fi

  if [[ ! -d "$VENV" ]]; then
    log_info "$plugin: creating venv at $VENV"
    run sudo -u edgecitadel "$PYTHON_BIN" -m venv "$VENV"
  fi

  log_info "$plugin: installing shared runtime and declared dependencies"
  run sudo -u edgecitadel "${VENV}/bin/pip" install --upgrade --quiet pip
  run sudo -u edgecitadel "${VENV}/bin/pip" install --quiet -e "${SOURCE_DIR}/plugin-toolkit" -r "$REQ"

  if [[ "$DRY_RUN" != "1" ]]; then
    echo -n "$CURRENT_SHA" | sudo -u edgecitadel tee "${VENV}/.requirements.sha" >/dev/null
  fi
  log_info "$plugin: venv ready"
done

log_info "setup-venvs.sh: complete"
