# EdgeCitadel

EdgeCitadel runs AI Agent Plugins across a Core and connected Edge hosts. Use
the dashboard to install, observe, and send tasks to your agents.

## Install

Choose one installation method:

```bash
# pip
pip install edgecitadel

# Homebrew
brew install edgecitadel
```

Confirm the installation:

```bash
edgecitadel --version
```

## Create a Core

Start Docker, then create the Core using a hostname or IP reachable by your
Edge hosts:

```bash
edgecitadel create --host core.example.internal
edgecitadel doctor
```

Open the Core URL printed by the command to use the dashboard.

## Join an Edge

Create an invitation on the Core:

```bash
edgecitadel invite --node-id studio-macmini --host core.example.internal
```

Run the returned command on the Edge. Choose how its Plugins connect:

```bash
# Simplest option; messaging pauses when the Core is unavailable.
edgecitadel join 'ecjoin://...' --messaging-mode single-client

# Keeps same-host messaging available when the Core is unavailable.
edgecitadel join 'ecjoin://...' --messaging-mode nats_leaf
```

`single-client` is the default, so its option may be omitted. Run
`edgecitadel doctor` after joining.

## Install and use a Plugin

Choose a Plugin and follow its setup guide:

- [Gemma](plugins/gemma/README.md)
- [Hermes](plugins/hermes/README.md)
- [Home Assistant](plugins/homeassistant/README.md)
- [Shell](plugins/shell/README.md)
- [Watchdog](plugins/watchdog/README.md)

Install a bundled Plugin by name, or install a local Plugin package by path:

```bash
edgecitadel plugin install PLUGIN_NAME
edgecitadel plugin install ./path/to/plugin
```

After installation, select the Plugin's agent in the Core dashboard to send it
a task and view the result.

## Common commands

```bash
edgecitadel status
edgecitadel doctor
edgecitadel plugin list
edgecitadel supervisor status
```

Stop a Core without deleting its data:

```bash
edgecitadel down
```

## Upgrade or uninstall

Use the same package manager that installed EdgeCitadel:

```bash
# pip
pip install --upgrade edgecitadel
pip uninstall edgecitadel

# Homebrew
brew upgrade edgecitadel
brew uninstall edgecitadel
```

Before uninstalling an Edge, stop its Plugins. A `nats_leaf` Edge also needs
its local messaging service stopped:

```bash
edgecitadel supervisor stop
edgecitadel messaging stop  # nats_leaf only
```

Uninstalling EdgeCitadel preserves local data in `~/.edgecitadel`.

## Documentation

- [Onboarding and troubleshooting](docs/onboarding.md)
- [Plugin development](plugin-toolkit/README.md)
- [Contributing](CONTRIBUTING.md)

## License

MIT
