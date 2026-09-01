# Homebrew distribution

The Homebrew package is the single CLI distribution for both node modes:

```text
brew install edgecitadel
├── edgecitadel create       # core; Docker prerequisite
├── edgecitadel join ...     # single-client Edge; no Docker
└── edgecitadel join ... --messaging-mode nats_leaf
                             # local NATS Leaf Edge; no Docker
```

The formula installs read-only application assets in its Cellar `libexec` and
uses `~/.edgecitadel` for node state, secrets, Supervisor files, plugins, logs,
SQLite, and JetStream data. Homebrew upgrades therefore do not replace runtime
state. The first plugin command automatically prepares the private Supervisor
Python environment; users do not install it separately. That first plugin
command needs internet access to download the Supervisor's Python dependencies.

The Formula depends on `nats-server` so `nats_leaf` can validate its generated
configuration before invitation redemption. `single-client` does not start that
binary. In `nats_leaf`, configuration, Leaf credentials, logs, PID/service
metadata, and JetStream data remain under `~/.edgecitadel/nats_leaf`, never the
Cellar. Uninstall and upgrade leave that state intact unless the operator
explicitly removes it.

## Current HEAD formula

There is no upstream tag or GitHub release yet, so the checked-in formula is
intentionally `HEAD`-only. Homebrew 6 no longer installs arbitrary local Formula
paths, so contributors test it through a temporary local tap after this work is
present on the configured remote:

```bash
brew tap-new --no-git local/edgecitadel
cp deploy/homebrew/Formula/edgecitadel.rb \
  "$(brew --repository local/edgecitadel)/Formula/edgecitadel.rb"
brew install --HEAD local/edgecitadel/edgecitadel
brew test edgecitadel
```

After testing, `brew uninstall edgecitadel` and `brew untap local/edgecitadel`
remove the temporary package and tap; runtime state under `~/.edgecitadel` is
deliberately retained.

Before uninstalling a `nats_leaf` Edge, run `edgecitadel supervisor stop` and
`edgecitadel messaging stop`. Formula upgrades retain all state; the next
`messaging restart` unloads and reloads the user job so its program path moves
from the old Cellar version to the current `nats-server` dependency.

Do not advertise `brew install edgecitadel` until a release archive and tap have
been published. Publishing is a separate, explicitly authorized operation.

## Stable release checklist

1. Run all source, simulated-Cellar, Docker, onboarding, and Playwright gates.
2. Tag the verified commit and publish its GitHub source archive.
3. Add the archive `url` and SHA-256 to the formula while retaining `head` for
   development builds. Add `version` only if Homebrew cannot infer it from the
   archive URL.
4. Put the formula in `zhonghaozhan/homebrew-edgecitadel` as
   `Formula/edgecitadel.rb`.
5. Verify a clean install, upgrade with preserved `~/.edgecitadel`, Core create,
   edge join, plugin installation, and uninstall.

After the tap exists, the intended public commands are:

```bash
brew tap zhonghaozhan/edgecitadel
brew install edgecitadel
```

## Docker boundary

Docker is checked only by `edgecitadel create`. Installing the Homebrew formula
does not install Docker Desktop: the `docker` Homebrew formula provides a client,
not the macOS Docker daemon required by the Core stack. An edge node never calls
Docker unless the operator explicitly asks it to become a Core, which is refused
after it has joined as an edge node.

The local NATS in `nats_leaf` is a user-level managed service and connects
outbound to Core port 7422; its client and monitoring listeners bind only to
loopback.
