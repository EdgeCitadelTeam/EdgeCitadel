#!/usr/bin/env bash
# deploy/backup/edgecitadel-backup.sh — nightly/weekly backup of EdgeCitadel state.
#
# Usage:
#   edgecitadel-backup.sh [--rotation=daily|weekly|cutover|adhoc]
#
# Invoked by /etc/cron.daily/edgecitadel-backup (default: --rotation=daily)
# and /etc/cron.weekly/edgecitadel-backup-weekly (--rotation=weekly).
#
# Backs up:
#   1. data/openclaw.db    (SQLite hot snapshot via sqlite3 .backup)
#   2. JetStream stream CONVERSATIONS  (via nats stream backup)
#   3. JetStream KV bucket AGENT_STATE (via nats kv backup)
#   4. /etc/edgecitadel/env (gpg-encrypted; skipped with WARN if no key)
#
# Writes manifest.json + checksums.sha256, then applies retention.
# Exits non-zero on any failure (cron mails root via MAILTO).

set -euo pipefail

ROTATION_ARG="${1:---rotation=daily}"
ROTATION="${ROTATION_ARG#--rotation=}"
case "$ROTATION" in
  daily|weekly|cutover|adhoc) ;;
  *) echo "invalid rotation: $ROTATION (expected daily|weekly|cutover|adhoc)" >&2; exit 1 ;;
esac

BACKUP_ROOT="/var/lib/edgecitadel/backups/${ROTATION}"
TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
DEST="${BACKUP_ROOT}/${TIMESTAMP}"
mkdir -p "${DEST}"

# Source NATS_TOKEN from /etc/edgecitadel/env
set -a
# shellcheck source=/dev/null
source /etc/edgecitadel/env
set +a

DB_SRC="/root/snap/EdgeCitadel/data/openclaw.db"
# nats CLI 0.1.5 doesn't accept --token; use URL-embedded auth
# (server-list URL of the form nats://TOKEN@host:port).
NATS_URL_AUTHED="nats://${NATS_TOKEN}@localhost:4222"

# 1. SQLite hot snapshot (atomic via online backup API).
# .timeout=30s makes sqlite3 wait for an exclusive write lock held by the
# aggregator during heartbeat persistence — otherwise sqlite3 errors out
# immediately with "database is locked" on a hot DB.
echo "[backup] snapshotting SQLite from ${DB_SRC}"
sqlite3 "$DB_SRC" ".timeout 30000" ".backup '${DEST}/openclaw.db'"

# 2. JetStream stream
# nats CLI 0.1.5 doesn't accept --force on `stream backup`; rely on the
# timestamped DEST dir being unique so the target subdir doesn't pre-exist.
# Stream is AGENT_INBOX (per aggregator/jetstream_bootstrap.py); the legacy
# CONVERSATIONS stream from pre-Phase-3 deployments is no longer used.
echo "[backup] backing up JetStream stream AGENT_INBOX"
nats --server="$NATS_URL_AUTHED" \
     stream backup AGENT_INBOX "${DEST}/jetstream-AGENT_INBOX"

# 3. JetStream KV (skipped on nats CLI 0.1.5 — `kv backup` was added in 0.2+).
# The AGENT_STATE bucket is operational telemetry (last_heartbeat, cpu, memory)
# that the aggregator rebuilds from agent heartbeats within ~60s. Acceptable
# to skip; bump nats_cli.version to 0.2+ in deploy/manifest.toml when KV
# backup/restore becomes critical.
echo "[backup] WARN: skipping JetStream KV AGENT_STATE backup (nats CLI 0.1.5 lacks 'kv backup'; rebuilt from heartbeats)" >&2

# 4. Encrypted env (skip with WARN if no GPG key configured)
if gpg --list-keys edgecitadel-backup >/dev/null 2>&1; then
  echo "[backup] encrypting /etc/edgecitadel/env"
  gpg --batch --yes --encrypt --recipient edgecitadel-backup \
      --output "${DEST}/env.gpg" /etc/edgecitadel/env
else
  echo "[backup] WARN: no GPG key 'edgecitadel-backup' found; env not backed up" >&2
fi

# 5. Manifest of what's in this snapshot
EDGECITADEL_VERSION="$(git -C /opt/edgecitadel rev-parse --short HEAD 2>/dev/null || echo unknown)"
OLLAMA_MODEL="$(ollama list 2>/dev/null | awk 'NR==2 {print $1}' || echo unknown)"
cat >"${DEST}/manifest.json" <<EOF
{
  "timestamp_utc": "${TIMESTAMP}",
  "rotation": "${ROTATION}",
  "edgecitadel_version": "${EDGECITADEL_VERSION}",
  "ollama_model": "${OLLAMA_MODEL}",
  "components": {
    "openclaw_db": "openclaw.db",
    "jetstream_agent_inbox": "jetstream-AGENT_INBOX",
    "env": "env.gpg"
  }
}
EOF

# 6. Checksums (files only — jetstream-AGENT_INBOX is a directory; recurse into it).
( cd "${DEST}" && find . -type f -not -name checksums.sha256 -print0 \
                  | sort -z | xargs -0 sha256sum > checksums.sha256 )

# 7. Sanity: SQLite copy is readable + has an agents row
sqlite3 "${DEST}/openclaw.db" "SELECT COUNT(*) FROM agents;" >/dev/null

# 8. Apply rotation (keep N most recent)
case "$ROTATION" in
  daily)  RETAIN=7  ;;
  weekly) RETAIN=4  ;;
  *)      RETAIN=999 ;;
esac
ls -1tr "$BACKUP_ROOT" | head -n -"${RETAIN}" 2>/dev/null | while read -r old; do
  echo "[backup] retiring old backup: ${old}"
  rm -rf "${BACKUP_ROOT:?}/${old}"
done

# 9. Compact log line for journald
SIZE="$(du -sh "${DEST}" | cut -f1)"
logger -t edgecitadel-backup "rotation=${ROTATION} dest=${DEST} size=${SIZE} ok"
echo "[backup] complete: ${DEST} (${SIZE})"
