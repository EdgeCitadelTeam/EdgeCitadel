#!/usr/bin/env bash
do_update() {
  require_root
  log_info "── Update ──"
  # Reconcile new required credentials before any code or service is changed.
  run python3 "${LIB_DIR}/reconcile-env.py"
  log_info "rsync ${SOURCE_DIR}/ → /opt/edgecitadel/"
  run rsync -a --delete --exclude=node_modules --exclude=.git --exclude=__pycache__ \
      --exclude=data/ --exclude=nats/data/ \
      "${SOURCE_DIR}/" /opt/edgecitadel/
  run chown -R edgecitadel:edgecitadel /opt/edgecitadel

  # Managed Agents are owned by agentd. Retire superseded direct units
  # without deleting their state, logs, or dependency environments.
  for u in edgecitadel-shell edgecitadel-gemma edgecitadel-homeassistant edgecitadel-watchdog; do
    if systemctl is-active --quiet "$u" 2>/dev/null; then
      log_info "stopping superseded direct runtime $u"
      run systemctl stop "$u"
    fi
    if systemctl is-enabled --quiet "$u" 2>/dev/null; then
      run systemctl disable "$u"
    fi
    run rm -f "/etc/systemd/system/${u}.service"
  done
  run systemctl daemon-reload

  log_info "update complete"
}
