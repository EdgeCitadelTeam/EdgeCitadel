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
    token: $NATS_TOKEN
    users: [
        { user: openclaw, password: $OPENCLAW_TOKEN,
          permissions: {
            publish: { allow: ["openclaw.*.>"] },
            subscribe: { allow: ["openclaw.*.results.>"] }
          }
        }
    ]
}
