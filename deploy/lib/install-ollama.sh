#!/usr/bin/env bash
# deploy/lib/install-ollama.sh — install Ollama at the version pinned in manifest.toml
#
# Usage: ./install-ollama.sh [--dry-run]
#
# On linux: curl-pipes the official installer with OLLAMA_VERSION env override.
# On macos: documents manual steps (real automation deferred until Mac Mini hardware).
#
# Idempotent: skips install if `ollama --version` already matches the pinned version.

set -euo pipefail

LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${LIB_DIR}/platform.sh"

PARSE_MANIFEST="${LIB_DIR}/parse-manifest.py"

[[ "${1:-}" == "--dry-run" ]] && export DRY_RUN=1

PLATFORM="$(detect_platform)"

# Manifest read with explicit exit-code check (per pattern from install-deps.sh).
_read_manifest_value() {
  local key="$1"
  local out
  if ! out=$(python3 "$PARSE_MANIFEST" get "$key"); then
    log_err "manifest read failed for $key"
    exit 1
  fi
  echo "$out"
}

WANT_VERSION="$(_read_manifest_value ollama.version)"

current_version() {
  if command -v ollama >/dev/null 2>&1; then
    ollama --version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1 || true
  fi
}

CUR="$(current_version)"

if [[ "$CUR" == "$WANT_VERSION" ]]; then
  log_info "ollama: already at pinned version $WANT_VERSION"
  exit 0
fi

if [[ -n "$CUR" ]]; then
  log_info "ollama: have $CUR, want $WANT_VERSION — upgrading"
else
  log_info "ollama: not installed; installing $WANT_VERSION"
fi

# Real install requires root (skipped in dry-run for testability).
if [[ "${DRY_RUN:-0}" != "1" ]]; then
  require_root "$@"
fi

if [[ "$PLATFORM" == "linux" ]]; then
  INSTALL_URL="$(_read_manifest_value ollama.install_url_linux)"
  log_info "ollama: curl $INSTALL_URL | OLLAMA_VERSION=$WANT_VERSION sh"
  if [[ "${DRY_RUN:-0}" == "1" ]]; then
    printf "DRY-RUN: curl -fsSL %s | OLLAMA_VERSION=%s sh\n" "$INSTALL_URL" "$WANT_VERSION" >&2
  else
    curl -fsSL "$INSTALL_URL" | OLLAMA_VERSION="$WANT_VERSION" sh
  fi
elif [[ "$PLATFORM" == "macos" ]]; then
  INSTALL_URL_MACOS="$(_read_manifest_value ollama.install_url_macos)"
  log_warn "ollama: macOS install path not yet executed against real hardware."
  log_warn "Manual step: download from $INSTALL_URL_MACOS"
  log_warn "and install Ollama-darwin.zip; then re-run deploy-host.sh."
  exit 1
fi

# Verify the install landed at the right version (skip in dry-run).
if [[ "${DRY_RUN:-0}" != "1" ]]; then
  NEW="$(current_version)"
  if [[ "$NEW" != "$WANT_VERSION" ]]; then
    log_err "ollama: post-install version mismatch (have $NEW, want $WANT_VERSION)"
    exit 1
  fi
  log_info "ollama: installed $WANT_VERSION"
fi
