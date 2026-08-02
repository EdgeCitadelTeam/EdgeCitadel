# Lab Node Setup

## Supported Baseline

Use Ubuntu 24.04 x86_64 on every qualifying host with Docker Engine, Compose
v2, Git, Node 24.6.0, npm 11.5.1, Playwright 1.58.2, uv 0.8.13, and Python
3.12. Both hosts must use the same clean commit and the hash-locked
`scripts/research/run-python` launcher:

```bash
cd /home/lab/edge-research
test -z "$(git status --porcelain -- \
  .dockerignore aggregator frontend e2e nginx scripts/research schemas \
  docs/setup-lab-node.md \
  docs/research/task-aware-reliability-contract-design.md)"
test "$(uv --version)" = "uv 0.8.13"
scripts/research/run-python -c \
  'import sys; assert sys.version_info[:2] == (3, 12)'
```

The launcher synchronizes the locked research environment before executing the
requested command. A dirty scoped checkout, unsupported host, mutable fixture
tag, or mismatched source snapshot fails before node reservation.

## Controller

Run the controller only on an explicitly trusted LAN or Tailnet interface. The
advertised address must resolve to the bound IPv4 address. Fixed non-loopback
ports require `--trusted-network-confirm`:

```bash
scripts/research/run-python scripts/research/lab_controller.py start \
  --run-id ec-remote-01 \
  --host-id controller-lab-01 \
  --lab-variant lifecycle \
  --bind-host 100.64.10.10 \
  --advertise-host 100.64.10.10 \
  --http-port 18080 --nats-port 14222 --monitor-port 18222 \
  --trusted-network-confirm
```

The command prints `controller.json`, the mode-0600 credential path, the app
URL, and `controller: READY`. The configuration contains endpoints, immutable
image identity, source commit, and source snapshot, but not the credential
value.

## Same-Host Two-Node Gate

From a clean Ubuntu checkout, run the maintained lifecycle and operator gate:

```bash
scripts/research/run-python scripts/research/lab_gate.py \
  --clean-checkout-gate \
  --repo-root /home/lab/edge-research \
  --run-id lab-clean-01 \
  --host-id controller-lab-01 \
  --receipt /home/lab/clean-gate-receipt.json
```

This runs one two-node lifecycle and the unchanged Chromium operator journey,
validates both finalized bundles, and verifies cleanup. Same-machine evidence
is useful for deterministic development but remains `preliminary`; it cannot
establish a physical two-host claim.

## Second Ubuntu Host Qualification

Provision the remote SSH key and known-host entry before the experiment. No
command in the run may prompt. On the controller, use one fail-closed shell:

```bash
set -euo pipefail
umask 077
cd /home/lab/edge-research

REMOTE_HOST="gateway-lab.internal"
REMOTE_REPO="/home/lab/edge-research"
SSH_ARGS="-o BatchMode=yes -o StrictHostKeyChecking=yes -o ConnectTimeout=10"
ssh $SSH_ARGS "$REMOTE_HOST" true

REMOTE_TMP="$(ssh $SSH_ARGS "$REMOTE_HOST" \
  'umask 077; mktemp -d /tmp/edgecitadel-remote.XXXXXX')"
case "$REMOTE_TMP" in
  /tmp/edgecitadel-remote.*) ;;
  *) echo "unsafe remote path" >&2; exit 1 ;;
esac

REMOTE_STATE_ROOT="$REMOTE_TMP/state"
CONTROLLER_CONFIG="$PWD/tmp/research/ec-remote-01/controller.json"
LOCAL_CREDENTIAL=""
REMOTE_IMAGE_ID=""
REMOTE_TMP_CREATED=1
REMOTE_FILES_READY=0

cleanup_lab() {
  cleanup_rc=0
  if [ "$REMOTE_FILES_READY" -eq 1 ]; then
    if ! ssh $SSH_ARGS "$REMOTE_HOST" \
      "cd '$REMOTE_REPO' && \
       scripts/research/run-python scripts/research/lab_node.py stop \
         --controller-config '$REMOTE_TMP/controller.json' \
         --credential-file '$REMOTE_TMP/nats.creds' \
         --state-root '$REMOTE_STATE_ROOT' \
         --agent-id shell-remote"; then
      cleanup_rc=1
    fi
    if [ "$cleanup_rc" -eq 0 ] && [ -n "$REMOTE_IMAGE_ID" ]; then
      if ! ssh $SSH_ARGS "$REMOTE_HOST" \
        "if docker image inspect '$REMOTE_IMAGE_ID' >/dev/null 2>&1; then \
           docker image rm '$REMOTE_IMAGE_ID'; \
         fi"; then
        cleanup_rc=1
      fi
    fi
    if [ "$cleanup_rc" -eq 0 ]; then
      if ssh $SSH_ARGS "$REMOTE_HOST" "rm -rf '$REMOTE_TMP'"; then
        REMOTE_FILES_READY=0
        REMOTE_TMP_CREATED=0
      else
        cleanup_rc=1
      fi
    else
      echo "remote cleanup incomplete; recovery files retained at $REMOTE_TMP" >&2
    fi
  elif [ "$REMOTE_TMP_CREATED" -eq 1 ]; then
    ssh $SSH_ARGS "$REMOTE_HOST" "rm -rf '$REMOTE_TMP'" || cleanup_rc=1
  fi
  if [ -n "$LOCAL_CREDENTIAL" ] && [ -f "$LOCAL_CREDENTIAL" ] && \
     [ -f "$CONTROLLER_CONFIG" ]; then
    scripts/research/run-python scripts/research/lab_node.py stop \
      --controller-config "$CONTROLLER_CONFIG" \
      --credential-file "$LOCAL_CREDENTIAL" \
      --agent-id shell-controller || cleanup_rc=1
  fi
  scripts/research/run-python scripts/research/lab_controller.py stop \
    --run-id ec-remote-01 || cleanup_rc=1
  return "$cleanup_rc"
}

on_signal() {
  signal_status="$1"
  trap - EXIT HUP INT TERM
  cleanup_lab || true
  exit "$signal_status"
}
trap cleanup_lab EXIT
trap 'on_signal 129' HUP
trap 'on_signal 130' INT
trap 'on_signal 143' TERM

scripts/research/run-python scripts/research/lab_controller.py start \
  --run-id ec-remote-01 \
  --host-id controller-lab-01 \
  --lab-variant lifecycle \
  --bind-host 100.64.10.10 \
  --advertise-host 100.64.10.10 \
  --http-port 18080 --nats-port 14222 --monitor-port 18222 \
  --trusted-network-confirm

LOCAL_CREDENTIAL="$(scripts/research/run-python -c \
  'import json,sys; print(json.load(open(sys.argv[1]))["credential_file"])' \
  "$CONTROLLER_CONFIG")"
test -f "$LOCAL_CREDENTIAL"
REMOTE_IMAGE_ID="$(scripts/research/run-python -c \
  'import json,sys; print(json.load(open(sys.argv[1]))["fixture_image_id"])' \
  "$CONTROLLER_CONFIG")"

scripts/research/run-python scripts/research/lab_node.py start \
  --controller-config "$CONTROLLER_CONFIG" \
  --credential-file "$LOCAL_CREDENTIAL" \
  --host-id controller-lab-01 \
  --agent-id shell-controller \
  --behavior echo --delay-ms 1000

scripts/research/run-python scripts/research/lab_controller.py export-image \
  --run-id ec-remote-01 \
  --output /tmp/ec-remote-01-fixture.tar \
  --result-file tmp/research/ec-remote-01/export.json

SOURCE_COMMIT="$(git rev-parse HEAD)"
test "$(ssh $SSH_ARGS "$REMOTE_HOST" \
  "git -C '$REMOTE_REPO' rev-parse HEAD")" = "$SOURCE_COMMIT"
test -z "$(ssh $SSH_ARGS "$REMOTE_HOST" \
  "git -C '$REMOTE_REPO' status --porcelain -- \
    .dockerignore aggregator frontend e2e nginx scripts/research schemas \
    docs/setup-lab-node.md \
    docs/research/task-aware-reliability-contract-design.md")"
ssh $SSH_ARGS "$REMOTE_HOST" \
  "cd '$REMOTE_REPO' && \
   test \"\$(uv --version)\" = 'uv 0.8.13' && \
   scripts/research/run-python -c \
     'import sys; assert sys.version_info[:2] == (3, 12)'"

scp $SSH_ARGS "$CONTROLLER_CONFIG" \
  "$REMOTE_HOST:$REMOTE_TMP/controller.json"
scp $SSH_ARGS "$LOCAL_CREDENTIAL" \
  "$REMOTE_HOST:$REMOTE_TMP/nats.creds"
scp $SSH_ARGS /tmp/ec-remote-01-fixture.tar \
  "$REMOTE_HOST:$REMOTE_TMP/fixture.tar"
ssh $SSH_ARGS "$REMOTE_HOST" "chmod 0600 '$REMOTE_TMP/nats.creds'"
REMOTE_FILES_READY=1

ssh $SSH_ARGS "$REMOTE_HOST" \
  "docker load --input '$REMOTE_TMP/fixture.tar' && \
   cd '$REMOTE_REPO' && \
   scripts/research/run-python scripts/research/lab_node.py start \
     --controller-config '$REMOTE_TMP/controller.json' \
     --credential-file '$REMOTE_TMP/nats.creds' \
     --state-root '$REMOTE_STATE_ROOT' \
     --host-id gateway-lab-02 --agent-id shell-remote \
     --behavior echo --delay-ms 1000"
```

The remote launcher verifies that the loaded image ID equals the immutable ID
in `controller.json`; it never falls back to a pull or host-Python fixture.

## Doctor

Publish one bound report from each active reservation:

```bash
scripts/research/run-python scripts/research/lab_node.py doctor \
  --controller-config "$CONTROLLER_CONFIG" \
  --credential-file "$LOCAL_CREDENTIAL" \
  --host-id controller-lab-01 \
  --agent-id shell-controller --publish

ssh $SSH_ARGS "$REMOTE_HOST" \
  "cd '$REMOTE_REPO' && \
   scripts/research/run-python scripts/research/lab_node.py doctor \
     --controller-config '$REMOTE_TMP/controller.json' \
     --credential-file '$REMOTE_TMP/nats.creds' \
     --state-root '$REMOTE_STATE_ROOT' \
     --host-id gateway-lab-02 --agent-id shell-remote --publish"
```

Doctor verifies the credential binding, immutable image, clean launcher source,
reservation identity, machine fingerprint, and controller route. The server
records the observed peer independently of the node-reported route.

## Command And Reconnect Evidence

Issue one completed command to each host, then retain the remote reservation,
queue one command while it is disconnected, and resume it:

```bash
scripts/research/run-python scripts/research/lab_controller.py command \
  --run-id ec-remote-01 --agent-id shell-controller \
  --body controller-01 --expected-output edgecitadel:controller-01 --wait \
  --result-file tmp/research/ec-remote-01/controller-command.json
scripts/research/run-python scripts/research/lab_controller.py command \
  --run-id ec-remote-01 --agent-id shell-remote \
  --body remote-01 --expected-output edgecitadel:remote-01 --wait \
  --result-file tmp/research/ec-remote-01/remote-command.json
scripts/research/run-python scripts/research/lab_controller.py command \
  --run-id ec-remote-01 --agent-id shell-remote \
  --body duplicate-remote-01 \
  --expected-output edgecitadel:duplicate-remote-01 --wait \
  --wire-copies 2 \
  --result-file tmp/research/ec-remote-01/remote-duplicate-command.json

ssh $SSH_ARGS "$REMOTE_HOST" \
  "cd '$REMOTE_REPO' && \
   scripts/research/run-python scripts/research/lab_node.py stop \
     --controller-config '$REMOTE_TMP/controller.json' \
     --credential-file '$REMOTE_TMP/nats.creds' \
     --state-root '$REMOTE_STATE_ROOT' \
     --agent-id shell-remote --retain-reservation"

scripts/research/run-python scripts/research/lab_controller.py command \
  --run-id ec-remote-01 --agent-id shell-remote \
  --body queued-remote-01 \
  --expected-output edgecitadel:queued-remote-01 --no-wait \
  --result-file tmp/research/ec-remote-01/queued-command.json

ssh $SSH_ARGS "$REMOTE_HOST" \
  "cd '$REMOTE_REPO' && \
   scripts/research/run-python scripts/research/lab_node.py start \
     --controller-config '$REMOTE_TMP/controller.json' \
     --credential-file '$REMOTE_TMP/nats.creds' \
     --state-root '$REMOTE_STATE_ROOT' \
     --host-id gateway-lab-02 --agent-id shell-remote \
     --behavior echo --delay-ms 1000"

QUEUED_TASK_ID="$(scripts/research/run-python -c \
  'import json,sys; print(json.load(open(sys.argv[1]))["task_id"])' \
  tmp/research/ec-remote-01/queued-command.json)"
scripts/research/run-python scripts/research/lab_controller.py await \
  --run-id ec-remote-01 --task-id "$QUEUED_TASK_ID" \
  --expected-output edgecitadel:queued-remote-01 \
  --qualification-kind queued-reconnect \
  --result-file tmp/research/ec-remote-01/queued-terminal.json

ssh $SSH_ARGS "$REMOTE_HOST" \
  "cd '$REMOTE_REPO' && \
   scripts/research/run-python scripts/research/lab_node.py doctor \
     --controller-config '$REMOTE_TMP/controller.json' \
     --credential-file '$REMOTE_TMP/nats.creds' \
     --state-root '$REMOTE_STATE_ROOT' \
     --host-id gateway-lab-02 --agent-id shell-remote --publish"
```

The second remote doctor binds the resumed reservation to the same host,
machine, source, image, and route before finalization.

## Teardown And Qualification

Run the cleanup function before checking or classifying the bundle. It stops
the remote node, removes its immutable image, deletes its private transfer
directory, stops the local node, and finalizes controller cleanup in that
order. A failed strict-SSH stop retains the recovery directory and prevents a
qualification label:

```bash
cleanup_lab
scripts/research/run-python scripts/research/check_artifact.py \
  --bundle /home/lab/edge-research/docs/research/results/lab/ec-remote-01 \
  --require-kind lab \
  --source-root /home/lab/edge-research
scripts/research/run-python scripts/research/lab_controller.py qualify \
  --run-id ec-remote-01
trap - EXIT HUP INT TERM
```

The qualifying output is exactly:

```text
lab qualification: REMOTE QUALIFIED
```

Any result short of successful remote cleanup, a valid finalized bundle, and
that line is `remote-capable` or `preliminary`, not remote-qualified.
Controller stop removes the journaled export tar. The cleanup trap repeats the
same ownership-safe sequence on failures and signals.

## Security And Platform Limits

This artifact uses one run-scoped shared NATS token and an unauthenticated HTTP
command/dashboard API on one explicitly bound trusted LAN or Tailnet address.
It does not provide per-agent identity, TLS, HTTP authentication, revocation,
rotation, firewall configuration, or Internet-safe access. The inventory
prevents duplicate IDs only for maintained launchers; any process holding the
shared credential can bypass it and claim a NATS subject or durable consumer.
The lab-only aggregator trusts nginx's overwritten `X-Forwarded-For` value
because nginx is its sole published HTTP ingress; this is an evidence
observation boundary, not an Internet-grade identity mechanism.

OpenClaw onboarding, MQTT firmware, macOS, ARM64 performance, and production
fleet deployment are outside this runbook.
