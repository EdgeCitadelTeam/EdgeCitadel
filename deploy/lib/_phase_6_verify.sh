#!/usr/bin/env bash
phase_6_verify() {
  log_info "── Phase 6: Verification ──"
  run python3 "${LIB_DIR}/run-checks.py"
  log_info "Phase 6: OK (all checks passed)"
}
