#!/usr/bin/env bash
# Phase 0: preflight checks. Read-only. Exits non-zero on any failure.

phase_0_preflight() {
  log_info "── Phase 0: Preflight ──"
  local fail=0
  is_linux || { log_err "this host is not linux ($(detect_platform))"; fail=1; }
  command -v docker >/dev/null || { log_err "docker not installed"; fail=1; }
  command -v python3.12 >/dev/null || log_warn "python3.12 missing — Phase 1 will install"
  command -v tailscale >/dev/null || log_warn "tailscale missing — host services install but ACL won't be reachable"

  # Disk: at least 10 GB free under /var/lib
  local free_kb=$(df -k /var/lib | awk 'NR==2 {print $4}')
  if (( free_kb < 10485760 )); then
    log_err "/var/lib has < 10 GB free ($free_kb KB)"; fail=1
  fi

  # Memory: at least 4 GB free
  local free_mem_kb=$(awk '/MemAvailable/ {print $2}' /proc/meminfo)
  if (( free_mem_kb < 4194304 )); then
    log_warn "less than 4 GB free memory ($free_mem_kb KB) — Ollama may struggle"
  fi

  # NATS broker reachable
  if ! nc -z -w2 localhost 4222 2>/dev/null; then
    log_warn "NATS broker not reachable on localhost:4222 — Phase 4 will start it"
  fi

  # /etc/edgecitadel/env exists with NATS_TOKEN
  if [[ ! -f /etc/edgecitadel/env ]]; then
    log_err "/etc/edgecitadel/env missing — see setup-linux guide § 2"; fail=1
  elif ! grep -q "^NATS_TOKEN=" /etc/edgecitadel/env; then
    log_err "/etc/edgecitadel/env exists but NATS_TOKEN= not set"; fail=1
  fi

  if (( fail > 0 )); then
    log_err "Preflight failed. Fix the above and re-run."
    exit 1
  fi
  log_info "Phase 0: OK"
}
