# Placeholder plugin

This installable example is validation-only. It demonstrates the EdgeCitadel plugin package contract and is not a working agent runtime.

## Structure

- `plugin.yaml` declares package identity, compatibility, runtime metadata, agent identities, permissions, and security policy.
- `plugin.lock.json` records deterministic hashes for every other regular package file.
- `skills/placeholder/SKILL.md` contains portable procedure memory.
- `skills/placeholder/binding.yaml` binds that procedure to EdgeCitadel runtime and schema metadata.
- `skills/placeholder/schemas/` defines strict input and output objects.
- `skills/placeholder/{references,scripts,assets}/` demonstrates optional resource directories that deliberately contain no resources.
- `runtime/` contains a deliberately nonfunctional runtime stub.

## Identities

The package identity is `local.placeholder`, the agent identity is `placeholder-agent`, and the portable skill identity is `example.placeholder`. These identities serve different purposes and are intentionally distinct.

Learned memory is stored externally by a future knowledge service. It must not mutate the installed package or its portable procedure memory.

## Setup

The commands below assume an activated `plugin-toolkit` virtual environment. From `plugin-toolkit/`, create and install it with:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[test]'
```

## Safe commands

From `plugin-toolkit/`, `lock` validates package metadata and writes or regenerates `plugin.lock.json`. The `validate` command verifies the existing lock and emits deterministic inventory data; `validate` is read-only. Neither command imports or executes the runtime.

```bash
python -m edgecitadel_supervisor lock ../plugins/examples/placeholder
python -m edgecitadel_supervisor validate ../plugins/examples/placeholder
python -m pytest tests/test_example_package.py -q
```

The runtime intentionally fails and must not be run for this milestone.
