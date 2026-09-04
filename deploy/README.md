# deploy/

Production deploy artifacts for EdgeCitadel Phase 5.
Operator-facing setup guide: [`../docs/onboarding.md`](../docs/onboarding.md).
Architecture: [`../docs/architecture/managed-agents-and-native-plugins.md`](../docs/architecture/managed-agents-and-native-plugins.md).

## Single source of truth

- `manifest.toml` — every host-level dependency lives here.
- `lib/checks.yaml` — every `--check` check lives here.

This production-host installer owns the Core stack and Ollama dependency. It
does not start Agents from packages directly. Install them through `edgecitadel agent
install` on an enrolled Edge so agentd is their sole lifecycle owner. During
update, obsolete direct Shell, Gemma, Home Assistant, and Watchdog units are
stopped and disabled without deleting state or logs.

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
| `lib/render-units.sh` | systemd template renderer |
| `lib/_phase_0_preflight.sh` … `_phase_7_cron.sh` | Phase implementations |
| `lib/_uninstall.sh`, `_update.sh` | Reverse + refresh |
| `lib/checks.yaml` | Checks for `--check` |
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

- [`../docs/onboarding.md`](../docs/onboarding.md) — operator-facing setup
- [`../docs/architecture/multi-mode-messaging.md`](../docs/architecture/multi-mode-messaging.md) — messaging modes
