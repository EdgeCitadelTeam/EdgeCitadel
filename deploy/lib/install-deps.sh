#!/usr/bin/env bash
# deploy/lib/install-deps.sh — install apt/brew packages per manifest.
#
# Usage: ./install-deps.sh [--dry-run]
# Reads:
#   manifest[apt_packages][common]   on linux
#   manifest[brew_packages][common]  on macos
#   manifest[python][packages_ubuntu] on linux
#   manifest[python][packages_macos]  on macos

set -euo pipefail

LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${LIB_DIR}/platform.sh"

PARSE_MANIFEST="${LIB_DIR}/parse-manifest.py"

# Optional flag
[[ "${1:-}" == "--dry-run" ]] && export DRY_RUN=1

# Apt/brew install requires root (skip in dry-run for testability).
# Call here at script-level so require_root sees the script's own args
# and emits an accurate "sudo $0 $*" suggestion.
if [[ "${DRY_RUN:-0}" != "1" ]]; then
  require_root "$@"
fi

PLATFORM="$(detect_platform)"

_read_manifest() {
  # Usage: _read_manifest <dotted.key> -> populates _OUT array
  # Exits non-zero on parse-manifest failure (e.g. missing key).
  # Filters empty lines so an empty manifest list yields an empty array.
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

_apt_install() {
  local pkgs=("$@")
  local missing=()
  for pkg in "${pkgs[@]}"; do
    if ! dpkg -s "$pkg" >/dev/null 2>&1; then
      missing+=("$pkg")
    fi
  done
  if [[ ${#missing[@]} -eq 0 ]]; then
    log_info "apt: all ${#pkgs[@]} packages already installed"
    return 0
  fi
  log_info "apt: installing ${#missing[@]} missing packages: ${missing[*]}"
  run apt-get update -qq
  run apt-get install -y "${missing[@]}"
}

_brew_install() {
  local pkgs=("$@")
  local missing=()
  for pkg in "${pkgs[@]}"; do
    if ! brew list --formula "$pkg" >/dev/null 2>&1; then
      missing+=("$pkg")
    fi
  done
  if [[ ${#missing[@]} -eq 0 ]]; then
    log_info "brew: all ${#pkgs[@]} packages already installed"
    return 0
  fi
  log_info "brew: installing ${#missing[@]} missing packages: ${missing[*]}"
  run brew install "${missing[@]}"
}

if [[ "$PLATFORM" == "linux" ]]; then
  _read_manifest apt_packages.common;     apt_common=("${_OUT[@]:-}")
  _read_manifest python.packages_ubuntu;  apt_python=("${_OUT[@]:-}")
  _apt_install "${apt_common[@]:-}" "${apt_python[@]:-}"
elif [[ "$PLATFORM" == "macos" ]]; then
  _read_manifest brew_packages.common;    brew_common=("${_OUT[@]:-}")
  _read_manifest python.packages_macos;   brew_python=("${_OUT[@]:-}")
  _brew_install "${brew_common[@]:-}" "${brew_python[@]:-}"
fi

log_info "install-deps.sh: complete"
