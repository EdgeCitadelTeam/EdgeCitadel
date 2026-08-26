# Server Setup — Linux (Ubuntu/Debian)

Production deploy guide for EdgeCitadel on Linux hosts. The macOS variant lives at `02-server-setup-macos.md`. For dev-stack-only setup, see `01-architecture.md`.

## 1. Audience & prerequisites

- Ubuntu 22.04 LTS or Debian 12+
- `sudo` access on this host
- Tailscale installed and signed in (we'll tag this host in §6)
- A working `.env` with `NATS_TOKEN` and `OPENCLAW_TOKEN` set

If any of those are missing, the deploy script's preflight will tell you which.

## 2. Quick install

```bash
# 1. Get the code (skip if already cloned)
git clone <repo-url> /opt/edgecitadel-source && cd /opt/edgecitadel-source

# 2. Configure secrets
sudo install -d -m 0750 /etc/edgecitadel
sudo install -m 0640 .env.example /etc/edgecitadel/env
sudoedit /etc/edgecitadel/env       # set NATS_TOKEN, OPENCLAW_TOKEN

# 3. Run preflight (read-only, no changes)
sudo ./deploy/deploy-host.sh --check

# 4. Run the install
sudo ./deploy/deploy-host.sh

# 5. Verify
sudo ./deploy/deploy-host.sh --check
curl http://localhost/api/system/status
```

## 3. What just happened

| What | Where | Owner |
|---|---|---|
| Production source | `/opt/edgecitadel/` | `edgecitadel:edgecitadel` |
| Adapter venvs | `/var/lib/edgecitadel/venvs/{gemma,watchdog,shell}/` | `edgecitadel` |
| Ollama models | `/var/lib/edgecitadel/ollama/` | `edgecitadel` |
| Backups | `/var/lib/edgecitadel/backups/{daily,weekly,cutover}/` | `root:edgecitadel` |
| Logs | `/var/log/edgecitadel/` + `journalctl -u edgecitadel-*` | `edgecitadel` |
| Secrets | `/etc/edgecitadel/env` (mode `0640 root:edgecitadel`) | `root` |
| Systemd units | `/etc/systemd/system/edgecitadel-*.service` | `root` |
| Cron files | `/etc/cron.{daily,weekly}/edgecitadel-backup*` | `root` |
| Manifest (single source of truth for deps) | `/opt/edgecitadel/deploy/manifest.toml` | tracked in git |
| Docker compose stack | stays at `/root/snap/EdgeCitadel/` (existing dev tree) | as-is |

## 4. Verifying the install

`deploy-host.sh --check` runs 41 checks across 13 categories. Expected output: `TOTAL: 41 checks  ✓ 41 passed  ✗ 0 failed`.

The most important single check is the round-trip smoke (last row): POST a `reasoning.chat` command to the gemma adapter, assert a result envelope arrives within 30s.

If `--check` reports drift, each failed row includes a remediation hint.

## 5. Operating

```bash
# Status
sudo systemctl status edgecitadel-gemma
sudo systemctl status edgecitadel-ollama edgecitadel-watchdog

# Live logs
sudo journalctl -u edgecitadel-gemma -f
sudo journalctl -t edgecitadel-backup --since today

# Restart one adapter
sudo systemctl restart edgecitadel-gemma

# Drain + restart the docker stack (preserves data, ~30s downtime)
cd /root/snap/EdgeCitadel
docker compose restart

# List backups
ls /var/lib/edgecitadel/backups/{daily,weekly}/

# Pull a new Ollama model (one-shot)
sudo -u edgecitadel ollama pull <model>
```

## 6. Tailnet ACL setup

This step gates which devices on your tailnet can reach the dashboard, NATS, and MQTT.

### 6.1 Tag this host

1. Open `https://login.tailscale.com/admin/machines` (signed in as the tailnet owner)
2. Find this host (look for the hostname `jim-eq` or whatever yours is)
3. `...` menu → Edit ACL tags → add `tag:edgecitadel-aggregator` → Save
4. The tag won't apply yet — it needs the next step's ACL change.

### 6.2 Apply the ACL stanza

1. Open `https://login.tailscale.com/admin/acls`
2. **Copy the current policy to a local backup** before any edits
3. Replace (or merge) with this stanza:

```json
{
  "groups": {
    "group:edgecitadel-admins": ["zhangyefan752@gmail.com"]
  },
  "tagOwners": {
    "tag:edgecitadel-aggregator": ["group:edgecitadel-admins"],
    "tag:edgecitadel-operator":   ["group:edgecitadel-admins"],
    "tag:edgecitadel-agent":      ["group:edgecitadel-admins"]
  },
  "acls": [
    { "action": "accept", "src": ["zhangyefan752@gmail.com"],
      "dst": ["tag:edgecitadel-aggregator:80,4222,1883"] },
    { "action": "accept", "src": ["tag:edgecitadel-operator"],
      "dst": ["tag:edgecitadel-aggregator:80,4222,1883"] },
    { "action": "accept", "src": ["tag:edgecitadel-agent"],
      "dst": ["tag:edgecitadel-aggregator:4222,1883"] },
    { "action": "accept", "src": ["zhangyefan752@gmail.com"],
      "dst": ["tag:edgecitadel-aggregator:22"] },
    { "action": "accept", "src": ["*"], "dst": ["*:*"], "proto": null }
  ],
  "ssh": [
    { "action": "accept", "src": ["zhangyefan752@gmail.com"],
      "dst": ["tag:edgecitadel-aggregator"], "users": ["root", "edgecitadel"] }
  ]
}
```

4. Click **Preview** — read every warning before saving.
5. Click **Save**.

### 6.3 Verify

From this host:

```bash
tailscale status --json | python3 -c "import sys,json; print('tags:', json.load(sys.stdin)['Self'].get('Tags', []))"
# Expected: tags: ['tag:edgecitadel-aggregator']
```

From an admin device (your laptop):

```bash
curl -sf http://<aggregator-tailnet-ip>/api/system/status
# Expected: 200 OK with {"status": "ok"}
```

### 6.4 Onboarding new agent hosts

For each future agent device (Pi, ESP32 with Tailscale, etc.):
1. Install Tailscale, sign in to the tailnet
2. Admin console → device → Edit ACL tags → add `tag:edgecitadel-agent`
3. Device gets NATS + MQTT access; no dashboard reach.

### Known trade-offs

The catch-all rule (`accept * → *:*`) at the bottom preserves your existing tailnet usage for invitee accounts. Two consequences worth understanding:

- **`:8222` (NATS monitoring) IS reachable from the tailnet.** Leaks subscription/stream/connection internals. Mitigation: bind monitoring to loopback in `nats/nats.conf` (one-line change). Deferred to Phase 5.x.
- **Invitee accounts can reach the dashboard.** v0.1 dashboard has no HTTP-level auth; ACL is the only access control. To restrict, replace the catch-all with explicit per-account accepts using `exceptDst: ["tag:edgecitadel-aggregator:*"]`. Requires understanding what each invitee's other tailnet usage is.

Both are deliberate; tighten when you're ready.

## 7. Updating

```bash
cd /opt/edgecitadel-source
git pull
sudo ./deploy/deploy-host.sh --update
```

`--update` rsyncs source → `/opt/edgecitadel/` and refreshes adapter venvs. Restarts adapter services to pick up new code.

## 8. Uninstalling

```bash
sudo ./deploy/deploy-host.sh --uninstall          # stop units, remove venvs
sudo ./deploy/deploy-host.sh --uninstall --purge  # ALSO removes backups + secrets (double-confirm prompt)
```

Uninstall does NOT touch the docker stack, the tailscale install, or `.env` (unless `--purge`).

## 9. Disaster recovery

See `/opt/edgecitadel/deploy/backup/README.md` for the full restore procedure. TL;DR: stop services → choose backup → restore SQLite (`cp`) → restore JetStream (`nats stream/kv restore`) → restart → verify with `--check`.

## 10. Troubleshooting

- **Gemma keeps restarting** → `sudo journalctl -u edgecitadel-gemma --since '2 min ago'`. Common cause: Ollama model not pulled. Fix: `sudo -u edgecitadel ollama pull gemma3:4b`.
- **`us-openclaw` went offline after cutover** → tailnet client probably needs to reconnect. Not our concern unless it stays offline > 30 min.
- **Backups failed last night** → `journalctl -t edgecitadel-backup --since '2 days ago'`.
- **`--check` reports `systemd-analyze security` regression** → a unit got edited. Diff against `/opt/edgecitadel/deploy/systemd/<name>.service.in`.
- **Stuck after cutover with healthcheck timeout** → docker rebuild may have failed. `docker compose logs aggregator | tail -50`.

## 11. See also

- ADR-0012: `docs/adr/0012-host-deploy-architecture.md`
- Manifest: `/opt/edgecitadel/deploy/manifest.toml`
- Backup runbook: `/opt/edgecitadel/deploy/backup/README.md`
- macOS variant: `02-server-setup-macos.md`
