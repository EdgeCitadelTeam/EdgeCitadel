#!/usr/bin/env bash
phase_4_cutover() {
  log_info "── Phase 4: Backup-then-cutover ──"

  local marker=/var/lib/edgecitadel/.cutover-completed
  if [[ -f "$marker" && "$FORCE_CUTOVER" != "1" ]]; then
    log_info "cutover already completed (marker: $marker); skip. Use --force-cutover to redo."
    return 0
  fi

  if [[ "$SKIP_BACKUP" == "1" ]]; then
    if [[ "$CONFIRM_SKIP_BACKUP" != "1" ]]; then
      log_err "--skip-backup requires --confirm-skip-backup"
      exit 1
    fi
    log_warn "DANGER: --skip-backup --confirm-skip-backup → no pre-cutover snapshot"
  else
    log_info "snapshotting current state to backups/cutover/"
    run "${SCRIPT_DIR}/backup/edgecitadel-backup.sh" --rotation=cutover
  fi

  log_info "stopping current docker stack"
  # --profile mqtt-ingress is also passed for `down` to clean up any leftover
  # nats-mqtt container from previous attempts (the profile is no longer used
  # in the current docker-compose.yml — folded into `nats` — but keeping the
  # flag here is harmless and helps clean up partial state on re-run).
  ( cd "$SOURCE_DIR" && run docker compose --profile mqtt-ingress down )

  log_info "starting docker stack (rebuild)"
  ( cd "$SOURCE_DIR" && run docker compose up -d --build )

  # Wait for healthchecks
  log_info "waiting for nats:8222/healthz to become healthy (max 60s)"
  if [[ "${DRY_RUN:-0}" != "1" ]]; then
    local i=0
    until curl -sf http://localhost:8222/healthz >/dev/null 2>&1; do
      ((i++)); ((i > 60)) && { log_err "nats healthcheck timed out"; exit 1; }
      sleep 1
    done
    log_info "nats healthy after ${i}s"
  fi

  log_info "waiting for aggregator /api/system/status (max 60s)"
  if [[ "${DRY_RUN:-0}" != "1" ]]; then
    local i=0
    until curl -sf http://localhost/api/system/status >/dev/null 2>&1; do
      ((i++)); ((i > 60)) && { log_err "aggregator healthcheck timed out"; exit 1; }
      sleep 1
    done
    log_info "aggregator healthy after ${i}s"
  fi

  run touch "$marker"
  log_info "Phase 4: OK"
}
