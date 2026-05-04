# EdgeCitadel Backups

Phase 5 backups protect two persistent data stores against accidental loss
or corruption: the aggregator's SQLite database (`openclaw.db`) and the
NATS JetStream stream + KV.

## Schedule

- Nightly: `/etc/cron.daily/edgecitadel-backup` (default: 06:25 UTC)
- Weekly: `/etc/cron.weekly/edgecitadel-backup-weekly` (default: Sun 06:47 UTC)
- Cutover: one-shot during `deploy-host.sh` Phase 4 (preserved indefinitely)

## Retention

- Daily: 7 most recent
- Weekly: 4 most recent
- Cutover: kept until manually deleted

## Locations

```
/var/lib/edgecitadel/backups/
├── daily/{YYYYMMDDTHHMMSSZ}/
├── weekly/{YYYYMMDDTHHMMSSZ}/
└── cutover/{YYYYMMDDTHHMMSSZ}/
```

Each backup dir contains:
- `openclaw.db` — SQLite snapshot (atomic via `sqlite3 .backup`)
- `jetstream-CONVERSATIONS/` — JetStream stream archive (from `nats stream backup`)
- `kv-AGENT_STATE/` — JetStream KV archive
- `env.gpg` — encrypted secrets (skipped if no GPG key configured)
- `manifest.json` — what's in this snapshot + version metadata
- `checksums.sha256` — sha256 of every file in the dir

## Disk budget

~50 MB/snapshot. Total budget: ~600 MB (7 daily + 4 weekly + cutover).

## Restore procedure

If you need to roll back to a backup:

```bash
# 0. Decide which backup to restore from. List options:
ls -1 /var/lib/edgecitadel/backups/{daily,weekly,cutover}/

# 1. Stop everything that writes to the data stores
docker compose -f /root/snap/EdgeCitadel/docker-compose.yml --profile mqtt-ingress down
sudo systemctl stop edgecitadel-{ollama,gemma,watchdog}

# 2. Pick the backup
BACKUP=/var/lib/edgecitadel/backups/daily/20260504T062500Z   # adjust

# 3. Restore SQLite (overwrites the live DB)
sudo cp "${BACKUP}/openclaw.db" /root/snap/EdgeCitadel/data/openclaw.db
sudo chown root:root /root/snap/EdgeCitadel/data/openclaw.db

# 4. Bring NATS back up (broker only — stream restore needs a running broker)
docker compose -f /root/snap/EdgeCitadel/docker-compose.yml --profile mqtt-ingress up -d nats

# Wait for NATS to become healthy
until curl -sf http://localhost:8222/healthz >/dev/null; do sleep 1; done

# 5. Restore JetStream stream + KV
source /etc/edgecitadel/env
nats --server=nats://localhost:4222 --token="$NATS_TOKEN" \
     stream restore CONVERSATIONS "${BACKUP}/jetstream-CONVERSATIONS" --force
nats --server=nats://localhost:4222 --token="$NATS_TOKEN" \
     kv restore AGENT_STATE "${BACKUP}/kv-AGENT_STATE" --force

# 6. Bring everything else back
docker compose -f /root/snap/EdgeCitadel/docker-compose.yml --profile mqtt-ingress up -d
sudo systemctl start edgecitadel-{ollama,gemma,watchdog}

# 7. Verify
sudo /opt/edgecitadel/deploy/deploy-host.sh --check
```

## Read-only restore drill

`deploy-host.sh --check` runs a one-shot restore drill against the most
recent daily backup: copies `openclaw.db` to a tempdir, runs schema
integrity check + a SELECT count, cleans up. Catches "backups exist but
are corrupt" silently. ~1s.

## When you have somewhere to mirror to (Phase 5.x)

Off-host backup is a deferred improvement. When a second always-on host
joins the tailnet (or you wire up B2/S3/restic):

1. Set `BACKUP_MIRROR_HOST=<dest>` in `/etc/edgecitadel/env`
2. Re-render this script (a future PR will add an `rsync` step controlled
   by that env var)

For B2/restic specifically:
- Install `restic` from apt or brew
- Initialize the repo: `restic init --repo b2:bucket-name:/path`
- Add a wrapper that runs `restic backup /var/lib/edgecitadel/backups/`
  after the local backup completes

## What is NOT backed up (deliberate)

- `/opt/edgecitadel/` source tree — reproducible from `git`
- Docker images — rebuildable
- `/var/lib/edgecitadel/ollama/` model store — re-pullable from `ollama pull`
- Adapter venvs at `/var/lib/edgecitadel/venvs/` — re-installable

This keeps backups small and restore-fast. If `git` is unreachable AND
docker registry is unreachable AND ollama registry is unreachable, you
have problems beyond what local backups fix.

## Troubleshooting

- **"Backup failed last night"** → `journalctl -t edgecitadel-backup --since '2 days ago'`
- **"sqlite3 not found"** → `apt install sqlite3` (should already be in manifest.toml)
- **"nats: command not found"** → `deploy-host.sh --update` re-installs nats-cli
- **Restore drill in `--check` reports backup integrity FAIL** → SQLite file may be corrupted; try the `weekly/` snapshot instead

## See also

- Backup script: `/opt/edgecitadel/deploy/backup/edgecitadel-backup.sh`
- Cron files: `/etc/cron.daily/edgecitadel-backup`, `/etc/cron.weekly/edgecitadel-backup-weekly`
- Phase 5 spec: `docs/superpowers/specs/2026-05-04-host-deploy-design.md` § 7
