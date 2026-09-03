#!/usr/bin/env bash
# deploy/deploy-host.sh — Phase 5 host deploy orchestrator.
#
# See docs/onboarding.md and deploy/README.md for usage.
#
# Modes (mutually exclusive):
#   (default)         install everything; full preflight → install → cutover → verify
#   --check           read-only drift detector (runs run-checks.py); exits 1 if drift
#   --dry-run         print every state-modifying command, change nothing
#   --uninstall       reverse: stop units, disable, remove venvs
#   --uninstall --purge   also remove /etc/edgecitadel and backups
#   --update          rsync source from --source-dir to /opt/edgecitadel and refresh venvs
#
# Optional flags:
#   --skip-cutover    install bare-metal services, leave docker stack untouched
#   --skip-backup     no pre-cutover snapshot (DANGEROUS; requires --confirm-skip-backup)
#   --force-cutover   re-run Phase 4 even if marker file exists
#   --source-dir DIR  source repo for --update (default: /root/snap/EdgeCitadel)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LIB_DIR="${SCRIPT_DIR}/lib"
source "${LIB_DIR}/platform.sh"

# ── Argument parsing ─────────────────────────────────────────────────
MODE="install"
PURGE=0
SKIP_CUTOVER=0
SKIP_BACKUP=0
CONFIRM_SKIP_BACKUP=0
FORCE_CUTOVER=0
SOURCE_DIR="/root/snap/EdgeCitadel"
DRY_RUN_FLAG=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --check)               MODE="check"; shift ;;
    --dry-run)             DRY_RUN_FLAG="--dry-run"; export DRY_RUN=1; shift ;;
    --uninstall)           MODE="uninstall"; shift ;;
    --update)              MODE="update"; shift ;;
    --purge)               PURGE=1; shift ;;
    --skip-cutover)        SKIP_CUTOVER=1; shift ;;
    --skip-backup)         SKIP_BACKUP=1; shift ;;
    --confirm-skip-backup) CONFIRM_SKIP_BACKUP=1; shift ;;
    --force-cutover)       FORCE_CUTOVER=1; shift ;;
    --source-dir)          SOURCE_DIR="$2"; shift 2 ;;
    -h|--help)             grep -E "^# " "$0" | head -25; exit 0 ;;
    *)                     log_err "unknown arg: $1"; exit 1 ;;
  esac
done

# ── Mode dispatch ─────────────────────────────────────────────────────
case "$MODE" in
  check)     exec python3 "${LIB_DIR}/run-checks.py" ;;
  uninstall) source "${SCRIPT_DIR}/lib/_uninstall.sh"; do_uninstall; exit 0 ;;
  update)    source "${SCRIPT_DIR}/lib/_update.sh"; do_update; exit 0 ;;
esac

# ── Install mode (the big one) ────────────────────────────────────────
LOG_FILE="/var/log/edgecitadel/deploy-$(date -u +%Y%m%dT%H%M%SZ).log"
require_root
mkdir -p /var/log/edgecitadel
exec > >(tee -a "$LOG_FILE") 2>&1

log_info "EdgeCitadel Phase 5 Host Deploy — starting"
log_info "Mode: install   Source: $SOURCE_DIR   Log: $LOG_FILE"
[[ "${DRY_RUN:-0}" == "1" ]] && log_warn "DRY-RUN mode — no state will change"

# Phase functions retain historical numbering so upgrade logs remain comparable.
source "${LIB_DIR}/_phase_0_preflight.sh"
source "${LIB_DIR}/_phase_1_install_deps.sh"
source "${LIB_DIR}/_phase_3_units.sh"
source "${LIB_DIR}/_phase_4_cutover.sh"
source "${LIB_DIR}/_phase_5_start.sh"
source "${LIB_DIR}/_phase_6_verify.sh"
source "${LIB_DIR}/_phase_7_cron.sh"

phase_0_preflight
phase_1_install_deps
phase_3_units

if [[ "$SKIP_CUTOVER" == "1" ]]; then
  log_warn "--skip-cutover: leaving docker stack untouched (Phase 4 skipped)"
else
  phase_4_cutover
fi

phase_5_start
phase_6_verify
phase_7_cron

log_info "EdgeCitadel Phase 5 Host Deploy — complete"
log_info "Run: deploy-host.sh --check  to verify state any time"
