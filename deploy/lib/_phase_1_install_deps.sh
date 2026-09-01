#!/usr/bin/env bash
# Phase 1: create user, dirs, install apt/brew/ollama/nats-cli per manifest.

phase_1_install_deps() {
  log_info "── Phase 1: Install dependencies ──"

  # Create dedicated user (idempotent)
  if ! getent passwd edgecitadel >/dev/null; then
    log_info "creating user edgecitadel"
    run useradd --system --no-create-home --shell /usr/sbin/nologin edgecitadel
  else
    log_info "user edgecitadel already exists"
  fi

  # Create dirs
  run mkdir -p /var/lib/edgecitadel/{ollama,venvs,backups/{daily,weekly,cutover}}
  run mkdir -p /var/log/edgecitadel /etc/edgecitadel
  # /home/edgecitadel must exist on disk even though we used --no-create-home
  # above. The ollama daemon (under ProtectHome=true) redirects HOME to
  # /var/lib/edgecitadel via systemd Environment=, but other ad-hoc operator
  # invocations of ollama as edgecitadel (e.g. `sudo -u edgecitadel ollama
  # pull` from Phase 5) read the real /etc/passwd HOME, which would be
  # missing without this mkdir.
  run install -d -m 0750 -o edgecitadel -g edgecitadel /home/edgecitadel
  run chown -R edgecitadel:edgecitadel /var/lib/edgecitadel /var/log/edgecitadel
  run chmod 0750 /var/lib/edgecitadel/backups
  run chgrp edgecitadel /var/lib/edgecitadel/backups
  run chmod 0750 /etc/edgecitadel
  if [[ -f /etc/edgecitadel/env ]]; then
    run chmod 0640 /etc/edgecitadel/env
    run chgrp edgecitadel /etc/edgecitadel/env
    run python3 "${LIB_DIR}/reconcile-env.py"
  fi

  # Sync /opt/edgecitadel from source-dir
  if [[ ! -d /opt/edgecitadel ]]; then
    log_info "creating /opt/edgecitadel from $SOURCE_DIR"
    run mkdir -p /opt/edgecitadel
    run rsync -a --delete --exclude=node_modules --exclude=.git --exclude=__pycache__ \
        --exclude=data/ --exclude=nats/data/ \
        "${SOURCE_DIR}/" /opt/edgecitadel/
    run chown -R edgecitadel:edgecitadel /opt/edgecitadel
  fi

  # apt/brew packages
  run "${LIB_DIR}/install-deps.sh" $DRY_RUN_FLAG

  # Ollama (pinned)
  run "${LIB_DIR}/install-ollama.sh" $DRY_RUN_FLAG

  # nats-cli (pinned)
  run "${LIB_DIR}/install-nats-cli.sh" $DRY_RUN_FLAG

  log_info "Phase 1: OK"
}
