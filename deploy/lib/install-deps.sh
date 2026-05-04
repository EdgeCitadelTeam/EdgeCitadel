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
  mapfile -t apt_common < <(python3 "$PARSE_MANIFEST" get apt_packages.common --format lines)
  mapfile -t apt_python < <(python3 "$PARSE_MANIFEST" get python.packages_ubuntu --format lines)
  _apt_install "${apt_common[@]}" "${apt_python[@]}"
elif [[ "$PLATFORM" == "macos" ]]; then
  mapfile -t brew_common < <(python3 "$PARSE_MANIFEST" get brew_packages.common --format lines)
  mapfile -t brew_python < <(python3 "$PARSE_MANIFEST" get python.packages_macos --format lines)
  _brew_install "${brew_common[@]}" "${brew_python[@]}"
fi

log_info "install-deps.sh: complete"
