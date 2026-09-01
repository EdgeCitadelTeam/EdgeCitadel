#!/usr/bin/env bash
do_update() {
  require_root
  log_info "── Update ──"
  log_info "rsync ${SOURCE_DIR}/ → /opt/edgecitadel/"
  run rsync -a --delete --exclude=node_modules --exclude=.git --exclude=__pycache__ \
      --exclude=data/ --exclude=nats/data/ \
      "${SOURCE_DIR}/" /opt/edgecitadel/
  run chown -R edgecitadel:edgecitadel /opt/edgecitadel

  # Re-run venv setup (will skip unchanged plugins via sha)
  run "${LIB_DIR}/setup-venvs.sh" --source-dir /opt/edgecitadel

  # Restart Plugin services to pick up new code.
  for u in edgecitadel-gemma edgecitadel-watchdog; do
    if systemctl is-active --quiet "$u"; then
      log_info "restarting $u"
      run systemctl restart "$u"
    fi
  done

  log_info "update complete"
}
