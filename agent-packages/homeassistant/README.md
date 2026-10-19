# Home Assistant Managed Adapter

This Managed Agent performs bounded, allowlisted Home Assistant reads, light actions,
camera luminance reduction, and experiment sequences.

Install without starting until credentials and allowlists are available:

```bash
edgecitadel agent install homeassistant --keep-disabled
```

Before `edgecitadel agent start edgecitadel.homeassistant`, provide
`HA_TOKEN_FILE`, `HA_BASE_URL`, and the applicable `HA_ALLOWED_*` environment
settings. The token remains in its private file and is never part of the Agent
package or lockfile. Runtime tests live in
`agent-runtime/tests/homeassistant_runtime/`.
