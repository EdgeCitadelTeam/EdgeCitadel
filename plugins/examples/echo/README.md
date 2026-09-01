# Echo agent plugin

This is the smallest executable proof of the complete agent join path. Install
it on any initialized core or edge node:

```bash
./scripts/edgecitadel plugin install ./plugins/examples/echo
```

The Supervisor validates its integrity, asks for permission approval, copies it
to the local immutable store, starts it, and injects only the enrolled NATS
connection. The runtime publishes its Agent Card, sends heartbeats, consumes its
durable inbox, and returns command bodies unchanged.
