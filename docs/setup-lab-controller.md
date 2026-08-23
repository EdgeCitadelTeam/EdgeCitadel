# Lab Controller Setup

Status: preliminary research launcher. It is not a production deployment path,
remote-host qualification, or an edge-device performance result.

## Supported Host

Run the controller only on a trusted Ubuntu 24.04 x86_64 host with Docker Engine
and Compose v2, Python 3.12, and Git already installed. The launcher requires
`/etc/machine-id`, uses Docker host networking for nodes, and rejects macOS and
other unsupported hosts before it creates or builds lab resources. Keep the
controller bound to `127.0.0.1` for same-host work; a non-loopback controller
requires the maintained remote-node workflow and is not yet qualified.

The lab uses an experiment-scoped NATS credential. It is stored in an owner-only
file, never accepted as an argument, and is not per-agent authorization. Use it
only on a trusted experiment network.

## Start

Use a clean source tree. The controller fails closed when its research source
paths are modified, when an earlier run needs recovery, or when the result
directory already exists.

```bash
scripts/research/run-python scripts/research/lab_controller.py start \
  --run-id ec-lab-01 \
  --host-id controller-lab-01 \
  --lab-variant lifecycle \
  --bind-host 127.0.0.1 \
  --advertise-host 127.0.0.1
```

The launcher creates an isolated Compose project, assigns loopback ports,
waits for the NATS health check and `/api/system/status`, then prints the private
`controller.json` handoff path followed by the local dashboard URL. The handoff
contains resolved endpoints and a credential file path, but never the credential
value. It records private ownership state at
`tmp/research/lab/ec-lab-01/controller-state.json`. Do not edit that file.

## Inspect And Stop

```bash
scripts/research/run-python scripts/research/lab_controller.py status \
  --state-file tmp/research/lab/ec-lab-01/controller-state.json --json

scripts/research/run-python scripts/research/lab_controller.py stop \
  --state-file tmp/research/lab/ec-lab-01/controller-state.json
```

`stop` uses the persisted Compose project and artifact owner record, so it can
recover after the original launcher process exits. A repeated stop returns the
recorded cleanup result and does not issue a second Docker teardown.

## Export The Fixture Image

For a remote node, export the immutable fixture image after the controller is
active. The tar and its receipt are user-owned paths; copy the tar to the remote
host before stopping the controller.

```bash
scripts/research/run-python scripts/research/lab_controller.py export-image \
  --state-file /absolute/path/to/controller-state.json \
  --output /absolute/path/to/ec-lab-01-fixture.tar \
  --result-file /absolute/path/to/ec-lab-01-export.json
```

The receipt records the controller's immutable image ID and tar SHA-256. The
controller rejects an existing or active output path and never includes the
transport credential in the tar command or receipt.

## Limits

The current launcher is a controller foundation only. It does not yet establish
the two-node lifecycle, remote-host qualification, ARM64 qualification,
controlled-network experiments, or a paper-ready lab evidence bundle. Existing
operator evidence remains separate from this lab path. In particular, it records
the immutable fixture image ID but does not automate image export, trusted
transfer, or remote-node execution; provision a matching image on each remote
host before its node preflight.
