# Shell Plugin

Shell executes an explicitly requested command with bounded timeout and output.
Its manifest declares an unrestricted sandbox and the `host-shell` device so
operators see the risk before installation.

```bash
edgecitadel plugin install shell
```

The Supervisor owns its isolated Python runtime and supplies either the Core
NATS endpoint (`single-client`) or loopback Local NATS endpoint (`nats_leaf`).
Runtime tests live in `plugin-toolkit/tests/shell_runtime/`.
