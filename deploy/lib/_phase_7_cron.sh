#!/usr/bin/env bash
phase_7_cron() {
  log_info "── Phase 7: Install backup cron ──"

  cat <<'EOF' | run tee /etc/cron.daily/edgecitadel-backup >/dev/null
#!/bin/sh
exec /opt/edgecitadel/deploy/backup/edgecitadel-backup.sh --rotation=daily
EOF
  cat <<'EOF' | run tee /etc/cron.weekly/edgecitadel-backup-weekly >/dev/null
#!/bin/sh
exec /opt/edgecitadel/deploy/backup/edgecitadel-backup.sh --rotation=weekly
EOF
  run chmod 0755 /etc/cron.daily/edgecitadel-backup /etc/cron.weekly/edgecitadel-backup-weekly

  log_info "cron files installed; next daily run at next 06:25 UTC"
  log_info "Phase 7: OK"
}
