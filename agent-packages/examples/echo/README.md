# Echo Managed Agent

This is the smallest executable proof of the complete agent join path. Install
it on any initialized core or edge node:

```bash
./scripts/edgecitadel agent install ./agent-packages/examples/echo
```

The package validator checks its integrity, asks for permission approval, copies
it to the local immutable store, and starts it under agentd. The runtime claims
tasks through the local managed-agent API and returns command bodies unchanged.
