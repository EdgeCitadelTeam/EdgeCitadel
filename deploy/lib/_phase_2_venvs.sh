#!/usr/bin/env bash
phase_2_venvs() {
  log_info "── Phase 2: Per-adapter venvs ──"
  run "${LIB_DIR}/setup-venvs.sh" $DRY_RUN_FLAG --source-dir /opt/edgecitadel
  log_info "Phase 2: OK"
}
