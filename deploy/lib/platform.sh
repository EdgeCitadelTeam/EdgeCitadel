#!/usr/bin/env bash
# deploy/lib/platform.sh — platform detection + small helpers.
# Sourced by other deploy/lib/*.sh scripts.
#
# Provides:
#   detect_platform   → echoes "linux" or "macos"; exits 1 on other OSes
#   require_root      → exits 1 if EUID != 0 (with helpful message)
#   require_command   → exits 1 if a named command is missing
#   log_info / log_warn / log_err — colored prefix output
#   run               → if DRY_RUN=1, print "DRY-RUN: <cmd>"; else exec

set -euo pipefail

readonly _PLATFORM_RED='\033[0;31m'
readonly _PLATFORM_YELLOW='\033[0;33m'
readonly _PLATFORM_GREEN='\033[0;32m'
readonly _PLATFORM_NC='\033[0m'

detect_platform() {
  case "$(uname -s)" in
    Linux)   echo "linux" ;;
    Darwin)  echo "macos" ;;
    *)
      echo "unsupported platform: $(uname -s)" >&2
      exit 1
      ;;
  esac
}

is_linux() { [[ "$(detect_platform)" == "linux" ]]; }
is_macos() { [[ "$(detect_platform)" == "macos" ]]; }

require_root() {
  if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
    log_err "this script must be run as root (try: sudo $0 $*)"
    exit 1
  fi
}

require_command() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    log_err "required command not found: $cmd"
    exit 1
  fi
}

log_info() { printf "${_PLATFORM_GREEN}[INFO]${_PLATFORM_NC} %s\n" "$*" >&2; }
log_warn() { printf "${_PLATFORM_YELLOW}[WARN]${_PLATFORM_NC} %s\n" "$*" >&2; }
log_err()  { printf "${_PLATFORM_RED}[ERR ]${_PLATFORM_NC} %s\n" "$*" >&2; }

run() {
  if [[ "${DRY_RUN:-0}" == "1" ]]; then
    printf "DRY-RUN: %s\n" "$*" >&2
  else
    "$@"
  fi
}
