#!/usr/bin/env bash
# deploy/lib/install-nats-cli.sh — install nats CLI at the pinned version.
#
# Usage: ./install-nats-cli.sh [--dry-run]
#
# Downloads from GitHub releases archive; extracts the `nats` binary
# into /usr/local/bin (linux) or /opt/homebrew/bin (macos).
# Idempotent: skips if `nats --version` already matches.

set -euo pipefail

LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${LIB_DIR}/platform.sh"

PARSE_MANIFEST="${LIB_DIR}/parse-manifest.py"

[[ "${1:-}" == "--dry-run" ]] && export DRY_RUN=1

# Manifest read with explicit exit-code check (per pattern).
_read_manifest_value() {
  local key="$1"
  local out
  if ! out=$(python3 "$PARSE_MANIFEST" get "$key"); then
    log_err "manifest read failed for $key"
    exit 1
  fi
  echo "$out"
}

PLATFORM="$(detect_platform)"
WANT_VERSION="$(_read_manifest_value nats_cli.version)"
URL_TMPL="$(_read_manifest_value nats_cli.install_url_template)"

# Validate version shape (defense against TOML int-vs-string slip)
[[ "$WANT_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || {
  log_err "nats_cli.version must be semver string, got '$WANT_VERSION'"
  exit 1
}

case "$PLATFORM" in
  linux) PLATFORM_TAG="linux-amd64"; INSTALL_DIR="/usr/local/bin" ;;
  macos)
    case "$(uname -m)" in
      arm64)  PLATFORM_TAG="darwin-arm64" ;;
      x86_64) PLATFORM_TAG="darwin-amd64" ;;
      *) log_err "unsupported macos arch: $(uname -m)"; exit 1 ;;
    esac
    INSTALL_DIR="/opt/homebrew/bin"
    ;;
esac

# Substitute template tokens (use // to replace all occurrences;
# {version} appears twice in the template — in the path and the filename).
URL="${URL_TMPL//\{version\}/$WANT_VERSION}"
URL="${URL//\{platform\}/$PLATFORM_TAG}"

current_version() {
  if command -v nats >/dev/null 2>&1; then
    nats --version 2>/dev/null | head -1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' || true
  fi
}

CUR="$(current_version)"
if [[ "$CUR" == "$WANT_VERSION" ]]; then
  log_info "nats-cli: already at pinned version $WANT_VERSION"
  exit 0
fi
if [[ -n "$CUR" ]]; then
  log_info "nats-cli: have $CUR, want $WANT_VERSION — upgrading"
else
  log_info "nats-cli: not installed; installing $WANT_VERSION"
fi

# Real install requires root (skipped in dry-run).
if [[ "${DRY_RUN:-0}" != "1" ]]; then
  require_root "$@"
fi

TMPDIR="$(mktemp -d)"
trap "rm -rf $TMPDIR" EXIT

ARCHIVE="${TMPDIR}/nats.zip"
log_info "nats-cli: downloading $URL"
run curl -fsSL -o "$ARCHIVE" "$URL"
run unzip -q "$ARCHIVE" -d "$TMPDIR/extracted"

# Archive layout: nats-{version}-{platform}/nats
SRC="${TMPDIR}/extracted/nats-${WANT_VERSION}-${PLATFORM_TAG}/nats"
if [[ "${DRY_RUN:-0}" != "1" && ! -f "$SRC" ]]; then
  log_err "nats-cli: archive layout unexpected; binary not at $SRC"
  ls -la "${TMPDIR}/extracted/" >&2
  exit 1
fi

run install -m 0755 "$SRC" "${INSTALL_DIR}/nats"

# Verify (skip in dry-run).
if [[ "${DRY_RUN:-0}" != "1" ]]; then
  NEW="$(current_version)"
  if [[ "$NEW" != "$WANT_VERSION" ]]; then
    log_err "nats-cli: post-install version mismatch (have $NEW, want $WANT_VERSION)"
    exit 1
  fi
  log_info "nats-cli: installed $WANT_VERSION at ${INSTALL_DIR}/nats"
fi
