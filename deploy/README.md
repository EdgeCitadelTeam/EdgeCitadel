# deploy/

Production deploy artifacts for EdgeCitadel Phase 5.
Operator-facing setup guide: `docs/02-server-setup-linux.md`.
Architectural rationale: `docs/adr/0012-host-deploy-architecture.md`.
Spec: `docs/superpowers/specs/2026-05-04-host-deploy-design.md`.

## Single source of truth

- `manifest.toml` — every host-level dependency lives here.
- `lib/checks.yaml` — every `--check` check lives here.

## Deployment secret upgrades

Install and update operations reconcile `/etc/edgecitadel/env` before changing
or restarting services. Existing non-placeholder values are preserved; missing,
empty, or documented placeholder values for Core NATS, Leaf authentication, and
the enrollment administrator are generated atomically without printing their
values. Run `sudo python3 deploy/lib/reconcile-env.py` to repair drift manually,
or add `--check` for a read-only validation.

## Adding a new dependency

1. Add to `manifest.toml`:
   - apt package?         → `[apt_packages].common`
   - brew package?        → `[brew_packages].common`
   - new ollama model?    → `[ollama].models`
   - new bundled Plugin?  → `[plugins].enabled` AND create
                            `systemd/edgecitadel-<name>.service.in`
2. Run `python3 deploy/lib/parse-manifest.py get <key>` to confirm parser accepts the new key.
3. Test on a clean VM: `sudo ./deploy-host.sh --dry-run`, then real install.
4. Open PR. Reviewers verify manifest delta only — script and docs consume the manifest.

## Bumping a pinned version

Same as above for the `version =` field. `./deploy-host.sh` will upgrade idempotently on next run.

## Adding a new --check

1. Add an entry to `lib/checks.yaml` under the right category.
2. Run `python3 lib/run-checks.py --quiet` locally to verify it works.
3. No script changes needed — the runner picks it up.

## File map

| Path | What |
|---|---|
| `manifest.toml` | Dep declarations |
| `deploy-host.sh` | Main entrypoint (modes + arg parsing + phase dispatch) |
| `lib/parse-manifest.py` | Reads manifest with stdlib tomllib |
| `lib/platform.sh` | OS detect + log helpers + `run` wrapper |
| `lib/install-deps.sh` | apt/brew dispatch |
| `lib/install-ollama.sh` | Pinned Ollama install |
| `lib/install-nats-cli.sh` | Pinned nats CLI install |
| `lib/setup-venvs.sh` | Per-Plugin runtime creation |
| `lib/render-units.sh` | systemd template renderer |
| `lib/_phase_0_preflight.sh` … `_phase_7_cron.sh` | Phase implementations |
| `lib/_uninstall.sh`, `_update.sh` | Reverse + refresh |
| `lib/checks.yaml` | 41 checks for `--check` |
| `lib/run-checks.py` | Check runner |
| `lib/smoke.py` | Round-trip smoke test |
| `systemd/*.service.in` | Linux unit templates |
| `launchd/*.plist.in` | macOS daemon templates (forward-looking) |
| `backup/edgecitadel-backup.sh` | Cron-invoked backup |
| `backup/README.md` | Restore runbook |
| `tests/` | Python unit tests for the deploy scripts |

## Testing

```bash
# All Python tests
python3 deploy/tests/test_parse_manifest.py
python3 deploy/tests/test_run_checks.py
python3 deploy/tests/test_smoke.py
```

## See also

- `docs/02-server-setup-linux.md` — operator-facing setup
- `docs/02-server-setup-macos.md` — macOS variant (forward-looking)
- `docs/superpowers/specs/2026-05-04-host-deploy-design.md` — full spec
