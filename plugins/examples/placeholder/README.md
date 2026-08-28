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

## Safe commands

From `plugin-system/`, the following commands inspect package data without importing or executing the runtime:

```bash
python -m edgecitadel_supervisor lock ../plugins/examples/placeholder
python -m edgecitadel_supervisor validate ../plugins/examples/placeholder
python -m pytest tests/test_example_package.py -q
```

The runtime intentionally fails and must not be run for this milestone.
