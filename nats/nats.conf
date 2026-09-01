server_name: "edgecitadel"
listen: 0.0.0.0:4222
http_port: 8222

jetstream {
    store_dir: "/data/jetstream"
    max_mem: 256MB
    max_file: 1GB
}

# MQTT ingress is deploy-time opt-in (ADR-0004). Uncommented by render script.
# MQTT_BEGIN
# mqtt {
#     port: 1883
#     ack_wait: "30s"
#     max_ack_pending: 1024
# }
# MQTT_END

authorization {
    # v0.1: token-only auth. Per ADR-0005, broker-level openclaw user scoping
    # is deferred to v0.2 (proper per-session NATS JWTs). In v0.1, browser
    # security relies on the aggregator-mediated translator (Task 14) and the
    # fact that OPENCLAW_TOKEN is held only by trusted browser sessions.
    token: $NATS_TOKEN
}

leafnodes {
    listen: 0.0.0.0:7422
    authorization {
        username: $NATS_LEAF_USERNAME
        password: $NATS_LEAF_PASSWORD
    }
}
