#!/usr/bin/env bash
phase_3_units() {
  log_info "── Phase 3: Render and install systemd units ──"
  run "${LIB_DIR}/render-units.sh" $DRY_RUN_FLAG
  log_info "Phase 3: OK (units installed; not started yet)"
}
