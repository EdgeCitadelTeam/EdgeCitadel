server_name: "edgecitadel-lab"
listen: 0.0.0.0:4222
http: 0.0.0.0:8222

jetstream {
  store_dir: "/data/jetstream"
  max_mem: 256MB
  max_file: 1GB
}

authorization {
  token: "$NATS_TOKEN"
}
