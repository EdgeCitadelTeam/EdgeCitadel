# Server Setup — macOS (Mac Mini)

> ⚠️ **This variant has not yet been executed end-to-end against a real Mac Mini.** The Linux variant (`02-server-setup-linux.md`) is the validated path. Treat this guide as the design for when a Mac Mini joins the tailnet; expect minor command tweaks during first run.

Production deploy guide for EdgeCitadel on macOS. Same 11 sections as the Linux guide, different platform commands.

## 1. Audience & prerequisites

- macOS 13+ (Apple Silicon recommended; Intel Macs work but are slower for Ollama)
- Homebrew installed (`brew --version`)
- `sudo` access on this host
- Tailscale installed and signed in (tag in §6)
- `.env.example` rendered with real `NATS_TOKEN`/`OPENCLAW_TOKEN`

## 2. Quick install

```bash
# 1. Get the code
git clone <repo-url> /opt/edgecitadel-source && cd /opt/edgecitadel-source

# 2. Install brew prereqs not in deploy/manifest.toml's auto-install list
brew install python@3.12 jq coreutils gnupg

# 3. Configure secrets
sudo install -d -m 0750 /etc/edgecitadel
sudo install -m 0640 .env.example /etc/edgecitadel/env
sudoedit /etc/edgecitadel/env

# 4. Preflight + install
sudo ./deploy/deploy-host.sh --check
sudo ./deploy/deploy-host.sh

# 5. Verify
sudo ./deploy/deploy-host.sh --check
curl http://localhost/api/system/status
```

## 3. What just happened

| What | Where | Owner |
|---|---|---|
| Production source | `/opt/edgecitadel/` | `_edgecitadel:_edgecitadel` |
| Adapter venvs | `/var/lib/edgecitadel/venvs/{gemma,watchdog,shell}/` | `_edgecitadel` |
| Ollama models | `/var/lib/edgecitadel/ollama/` | `_edgecitadel` |
| Backups | `/var/lib/edgecitadel/backups/{daily,weekly,cutover}/` | `root:_edgecitadel` |
| Logs | `/var/log/edgecitadel/` + `log show --predicate ...` | `_edgecitadel` |
| Secrets | `/etc/edgecitadel/env` (mode `0640 root:_edgecitadel`) | `root` |
| LaunchDaemons | `/Library/LaunchDaemons/io.edgecitadel.{ollama,gemma,watchdog}.plist` | `root:wheel` |
| Manifest | `/opt/edgecitadel/deploy/manifest.toml` | tracked in git |
| Docker compose stack | location of source clone (see Linux guide §3 for context) | as-is |

## 4. Verifying the install

`deploy-host.sh --check` runs the same 41 checks. Platform-specific subset:
- `systemctl is-active` → `launchctl print system/io.edgecitadel.<name>`
- `systemd-analyze security` → equivalent: codesign + sandbox-profile reference check (TBD when first executed)

## 5. Operating

```bash
# Status
sudo launchctl print system/io.edgecitadel.gemma | grep state

# Live logs
log stream --predicate 'subsystem == "io.edgecitadel.gemma"' --info

# Restart one adapter
sudo launchctl kickstart -k system/io.edgecitadel.gemma

# Drain + restart the docker stack
cd /root/snap/EdgeCitadel
docker compose --profile mqtt-ingress restart
```

## 6. Tailnet ACL setup

Identical to the Linux guide §6 — Tailscale ACLs are platform-independent. Tag this host as `tag:edgecitadel-aggregator`, apply the stanza, verify.

The known trade-offs (`:8222` reachable, invitee dashboard reach) apply identically.

## 7. Updating

```bash
cd /opt/edgecitadel-source && git pull
sudo ./deploy/deploy-host.sh --update
```

## 8. Uninstalling

```bash
sudo ./deploy/deploy-host.sh --uninstall
sudo ./deploy/deploy-host.sh --uninstall --purge
```

Reverses launchd bootout for each daemon, removes plists, cleans up venvs. `--purge` also removes backups + secrets (double-confirm).

## 9. Disaster recovery

Same as Linux guide §9 — see `/opt/edgecitadel/deploy/backup/README.md`.

## 10. Troubleshooting

- **Gemma daemon won't load** → `sudo log show --predicate 'subsystem == "io.edgecitadel.gemma"' --last 5m`. Check Ollama is responsive: `curl http://localhost:11434/api/version`.
- **launchd daemon stuck "exited"** → check the plist path was loaded: `sudo launchctl list | grep edgecitadel`. Re-bootstrap if missing: `sudo launchctl bootstrap system /Library/LaunchDaemons/io.edgecitadel.gemma.plist`.
- **Sandbox / quarantine errors** → daemon binaries may need `xattr -d com.apple.quarantine`. Document the case when first hit.

## 11. See also

- Linux guide: `02-server-setup-linux.md`
- ADR-0009: `docs/adr/0009-host-deploy-architecture.md`
- Spec: `docs/superpowers/specs/2026-05-04-host-deploy-design.md`
