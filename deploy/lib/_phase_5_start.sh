#!/usr/bin/env bash
phase_5_start() {
  log_info "── Phase 5: Start host model service ──"

  log_info "starting edgecitadel-ollama (and waiting for HTTP)"
  run systemctl start edgecitadel-ollama

  if [[ "${DRY_RUN:-0}" != "1" ]]; then
    local i=0
    until curl -sf http://localhost:11434/api/version >/dev/null 2>&1; do
      # Use pre-increment + arithmetic-true-only return; ((i++)) returns the
      # OLD value (0 on first iter) which is falsy and set -e aborts the script.
      i=$((i + 1))
      if (( i > 30 )); then
        log_err "ollama HTTP did not come up"; exit 1
      fi
      sleep 1
    done
    log_info "ollama HTTP ready after ${i}s"

    # Pull the model if not already local
    local model
    model=$(python3 "${LIB_DIR}/parse-manifest.py" get ollama.models --format lines | head -1)
    if ! ollama list | grep -q "^${model}"; then
      log_info "pulling ollama model: $model"
      sudo -u edgecitadel ollama pull "$model"
    fi
  fi

  if [[ "${DRY_RUN:-0}" != "1" ]]; then
    for u in edgecitadel-ollama; do
      if ! systemctl is-active --quiet "$u"; then
        log_err "$u failed to start; tail of journal:"
        journalctl -u "$u" --no-pager --lines=20 || true
        exit 1
      fi
      log_info "$u: active"
    done
  fi

  log_info "Managed Agents are installed separately with edgecitadel agent install"

  log_info "Phase 5: OK"
}
