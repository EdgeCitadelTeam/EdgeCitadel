# Homebrew distribution

The Homebrew package is the single CLI distribution for both node modes:

```text
brew tap EdgeCitadelTeam/edgecitadel
brew trust --tap EdgeCitadelTeam/edgecitadel
brew install edgecitadel
├── edgecitadel create       # core; Docker prerequisite
├── edgecitadel install --join ... --plugin codex --scope user --yes
│                            # single-client Edge; no Docker
└── edgecitadel install --join ... --messaging-mode nats_leaf \
      --plugin codex --scope user --yes
                             # local NATS Leaf Edge; no Docker
```

The formula installs read-only application assets in its Cellar `libexec` and
uses `~/.edgecitadel` for node state, secrets, agentd files, Agent Packages, logs,
SQLite, and JetStream data. Homebrew upgrades therefore do not replace runtime
state. The first Agent or connector command automatically prepares the private
agentd Python environment and loads a per-user LaunchAgent; users do not install
or run a root service. That first command needs internet access to download
agentd's declared Python dependencies.

`nats-server` is deliberately separate because only `nats_leaf` needs it. Install
it with `brew install nats-server` before choosing that mode; `single-client`
does not install or start it. In `nats_leaf`, configuration, Leaf credentials, logs, PID/service
metadata, and JetStream data remain under `~/.edgecitadel/nats_leaf`, never the
Cellar. Uninstall and upgrade leave that state intact unless the operator
explicitly removes it.

## Source and stable formulas

The formula checked into this repository is intentionally `HEAD`-only so it can
exercise the source tree without a circular archive checksum. Stable formulas
are published separately in `EdgeCitadelTeam/homebrew-edgecitadel`. Homebrew 6
no longer installs arbitrary local Formula paths, so contributors test source
changes through a temporary local tap after the work is present on the
configured remote:

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

Before uninstalling an Edge, run `edgecitadel service stop`; for `nats_leaf`,
also run `edgecitadel messaging stop`. Formula upgrades retain all state. Run
`edgecitadel service restart` after an upgrade so the LaunchAgent uses the new
agentd environment; `edgecitadel messaging restart` is needed only when the
local NATS configuration or binary changed.

Do not advertise a new version until its release archive and tap formula have
both been published and tested. Publishing is a separate, explicitly authorized
operation.

## Stable release checklist

1. Run all source, simulated-Cellar, Docker, onboarding, and Playwright gates.
2. Tag the verified commit and publish its GitHub source archive.
3. Add the archive `url` and SHA-256 to the formula while retaining `head` for
   development builds. Add `version` only if Homebrew cannot infer it from the
   archive URL.
4. Put the formula in `EdgeCitadelTeam/homebrew-edgecitadel` as
   `Formula/edgecitadel.rb`.
5. Verify a clean install, upgrade with preserved `~/.edgecitadel`, Core create,
   Edge join, Agent Package and Plugin installation, and uninstall.

After the tap exists, the intended public commands are:

```bash
brew tap EdgeCitadelTeam/edgecitadel
brew trust --tap EdgeCitadelTeam/edgecitadel
brew install edgecitadel
```

Homebrew 6 requires the trust step before loading formulas from a non-official
tap. Earlier Homebrew versions do not enforce this gate.

## Docker boundary

Docker is checked only by `edgecitadel create`. Installing the Homebrew formula
does not install Docker Desktop: the `docker` Homebrew formula provides a client,
not the macOS Docker daemon required by the Core stack. An edge node never calls
Docker unless the operator explicitly asks it to become a Core, which is refused
after it has joined as an edge node.

The local NATS in `nats_leaf` is a user-level managed service and connects
outbound to Core port 7422; its client and monitoring listeners bind only to
loopback.
