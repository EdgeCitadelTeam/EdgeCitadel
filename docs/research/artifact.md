# Research Artifact Guide

This guide runs the task-aware reliability artifact described in
[`task-aware-reliability-contract-design.md`](task-aware-reliability-contract-design.md).
It is separate from the development stack and uses run-owned Docker resources.

## What To Run

| Profile | Purpose | Claim limit |
| --- | --- | --- |
| `quick` | Fast local wiring and cleanup diagnostic. | No statistics or performance claim. |
| `matrix-smoke` | Executes the declared workload/mode matrix at small scale. | Functional and integration evidence only. |
| `paper` | Uses the declared campaign configuration and measured blocks. | No performance claim until the Linux collector reaches publication-valid `collected` status; then preliminary x86_64 evidence. |
| Lab lifecycle | Exercises deterministic multi-agent controller/node ownership. | `PRELIMINARY` until the two-host qualification contract passes. |
| Operator journey | Captures desktop/mobile screenshots, video, trace, and API correlation. | Functional operator evidence, not a latency benchmark. |

`paper` does not by itself establish paper readiness. ARM64 repetitions,
controlled-network repetitions, and a qualified remote two-host lifecycle remain
separate evidence gates.

## Workload Index

Every profile uses the same predeclared W1-W8 workload contract. The table is a
runbook index; the [reliability design](task-aware-reliability-contract-design.md)
defines the complete matrix, invariants, and evidence fields.

| Workload | Scenario | Required observation |
| --- | --- | --- |
| W1 | One command and terminal result | One correlated terminal outcome. |
| W2 | One-hop delegation | Parent and fresh child task IDs both execute. |
| W3 | Progress stream with terminal result | Twenty progress observations are accounted for. |
| W4 | Worker reconnect | Mode-specific offline-delivery outcome is observed. |
| W5 | Crash at each recovery boundary | Each crash subtrial retains recovery counts. |
| W6a | Byte-identical wire retry | Same envelope ID receives duplicate acknowledgement. |
| W6b | New-envelope semantic retry | Same task ID remains protected after broker dedup expiry. |
| W6c | Task-ID collision | Changed sender or payload is rejected without execution. |
| W7 | Coordinator restart in flight | Mode-specific restart outcome is observed. |
| W8 | Crash after non-idempotent effect | Side effect and prepared outcome remain accounted for. |

## Prerequisites

Use a clean checkout, Docker Engine with Compose v2, Git, and the pinned Python
environment exposed by `scripts/research/run-python`. The preliminary campaign
is defined for Ubuntu 24.04 x86_64 in
`scripts/research/configs/campaigns/preliminary-x86-lan.yaml`. macOS and ARM64
quick or smoke output is development evidence, not the preliminary campaign.
The `paper` command fail-closes before allocating a campaign directory or Docker
resources unless the host identifies as Ubuntu 24.04 x86_64; manifests retain
the observed system, architecture, release, and OS identity for audit. The
publication checker independently requires those same sealed host facts.

## Run A Campaign

Set paths to absolute locations outside the checkout for scratch state and to the
generated result root inside the checkout only when the source tree is clean:

```bash
scripts/research/run-python scripts/research/run_artifact.py run \
  --profile quick \
  --source-root /absolute/path/to/clean-checkout \
  --output-root /absolute/path/to/clean-checkout/docs/research/results/raw \
  --scratch-root /absolute/path/to/artifact-scratch \
  --result-file /absolute/path/to/quick-result.json
```

Use `--profile matrix-smoke` for the broader functional matrix. Run the
preliminary measured campaign only on the supported host:

```bash
scripts/research/run-python scripts/research/run_artifact.py run \
  --profile paper \
  --campaign-config scripts/research/configs/campaigns/preliminary-x86-lan.yaml \
  --source-root /absolute/path/to/clean-checkout \
  --output-root /absolute/path/to/clean-checkout/docs/research/results/raw \
  --scratch-root /absolute/path/to/artifact-scratch \
  --result-file /absolute/path/to/paper-result.json
```

Each result file names the campaign directory and its sealed per-repetition
bundles. A valid task failure is retained as experiment evidence; a failed
preflight, leaked resource, dirty source, invalid manifest, or checker failure
invalidates the run instead of becoming a measurement.

## Check And Clean Up

Validate every selected bundle against the same clean checkout used to capture
it:

```bash
scripts/research/run-python scripts/research/check_artifact.py \
  --bundle /absolute/path/to/bundle \
  --require-kind benchmark \
  --source-root /absolute/path/to/clean-checkout
```

The checker prints `artifact: PASS` only when schema, artifact hashes, kind, and
source provenance agree. It does not rerun an experiment or modify evidence.
Validate a complete paper campaign, including its fixed schedule, semantic
observations, immutable images, and resource contract, with:

```bash
scripts/research/run-python scripts/research/check_artifact.py \
  --campaign /absolute/path/to/results/raw/preliminary-x86-lan \
  --publication
```

After an interruption, clean only the recorded run ID:

```bash
scripts/research/run-python scripts/research/run_artifact.py cleanup \
  --run-id ec-run-id \
  --scratch-root /absolute/path/to/artifact-scratch
```

Raw campaign output belongs under `docs/research/results/raw/`; derived analysis
belongs under `docs/research/results/derived/`. Reproduce deterministic tables,
figure data, and the input-bound report only from a publication-valid campaign:

```bash
scripts/research/run-python scripts/research/analyze_artifact.py \
  --campaign preliminary-x86-lan \
  --input-root /absolute/path/to/results/raw \
  --output-root /absolute/path/to/results/derived
```

The analyzer refuses development, incomplete, dirty, hash-invalid, or
harness-invalid campaigns before creating the final output directory. The
maintained runner records Docker Engine CPU/RSS/network snapshots plus a
two-second idle baseline as explicitly `partial` host trial-window evidence. A
runner-to-host handshake captures the start snapshot immediately before T0 and
the end snapshot after terminal observation but before transport teardown. It
retains event-derived application bytes, paired transport deltas, and raw W5/W6
workload records, which the publication checker cross-validates against aggregate
counts. It still cannot produce input accepted by `--publication`: the host
collector remains explicitly partial until a real Linux Docker campaign proves
complete, stable logical protocol/storage/message metrics. No cost or comparative
performance result is publication-ready. Real-run manifests declare this partial
component/cadence contract; `not_collected` is reserved for runs without host
sampling.

## Related Evidence

- [Operator screenshots, video, trace, and API evidence](results/README.md)
- [Lab controller setup](../setup-lab-controller.md)
- [Lab node setup](../setup-lab-node.md)
- [Reliability design and evidence requirements](task-aware-reliability-contract-design.md)
