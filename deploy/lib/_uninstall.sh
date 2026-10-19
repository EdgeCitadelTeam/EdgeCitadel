#!/usr/bin/env bash
do_uninstall() {
  require_root
  log_warn "── Uninstall ──"

  # Include retired direct Agent units so uninstall also handles upgrades from
  # an older deployment. Managed Agent data remains outside these units.
  for u in edgecitadel-shell edgecitadel-gemma edgecitadel-homeassistant edgecitadel-watchdog edgecitadel-ollama; do
    if systemctl is-enabled --quiet "$u" 2>/dev/null; then
      log_info "stopping + disabling $u"
      run systemctl stop "$u" 2>/dev/null || true
      run systemctl disable "$u" 2>/dev/null || true
    fi
  done

  log_info "removing systemd unit files"
  run rm -f /etc/systemd/system/edgecitadel-*.service
  run systemctl daemon-reload

  log_info "removing obsolete package runtime environments"
  run rm -rf /var/lib/edgecitadel/venvs

  log_info "removing cron files"
  run rm -f /etc/cron.daily/edgecitadel-backup /etc/cron.weekly/edgecitadel-backup-weekly

  if [[ "$PURGE" == "1" ]]; then
    log_warn "DANGER --purge: removing /etc/edgecitadel, /var/log/edgecitadel, /var/lib/edgecitadel"
    log_warn "This deletes all backups. Type YES to confirm:"
    if [[ "${DRY_RUN:-0}" != "1" ]]; then
      read -r confirm
      [[ "$confirm" == "YES" ]] || { log_info "aborted"; exit 1; }
    fi
    run rm -rf /etc/edgecitadel /var/log/edgecitadel /var/lib/edgecitadel
  fi

  log_info "docker stack left running. To stop: cd $SOURCE_DIR && docker compose down"
  log_info "user 'edgecitadel' left in place. Remove with: sudo userdel edgecitadel"
  log_info "uninstall complete"
}
