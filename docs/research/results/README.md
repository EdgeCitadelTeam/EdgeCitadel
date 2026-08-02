# Research Evidence Results

Raw research outputs are generated from a clean source checkout and are not
hand-edited. A finalized bundle contains its manifest, canonical API snapshots,
desktop/mobile screenshots, video, trace, Playwright report, runtime cleanup
receipt, and a hash for every non-manifest artifact.

## Operator Journey

Capture the deterministic operator journey into a new output directory:

```bash
scripts/research/run-python scripts/research/capture_operator_journey.py \
  --output-root /absolute/path/to/docs/research/results/operator \
  --source-root /absolute/path/to/clean-checkout
```

The wrapper owns a temporary Compose project, executes one desktop and one mobile
Playwright project without retries, copies Playwright's temporary video and trace
into the bundle, verifies teardown, then seals `manifest.json`. It refuses a
dirty source tree, missing media, missing API snapshots, a non-drained queue, or
different desktop/mobile task IDs.
It also requires the completed task card to be inside the captured viewport, so
the mobile task-board screenshot demonstrates the terminal state rather than an
off-screen horizontal column.

Validate a sealed bundle against its exact source checkout:

```bash
scripts/research/run-python - <<'PY'
from pathlib import Path
from scripts.research.check_artifact import check_bundle

report = check_bundle(
    Path("/absolute/path/to/operator-bundle"),
    expected_kind="operator",
    source_root=Path("/absolute/path/to/clean-checkout"),
)
report.require_valid()
PY
```

Use only a checked `PASS` bundle as operator evidence. A passing local browser
run or an unsealed directory is diagnostic output, not a publication artifact.

`operator/20260727T194508Z-448a7118129a` retains a desktop/mobile functional
capture with screenshots, video, and traces. Its manifest names the exact clean
capture snapshot, but that snapshot is not a durable revision of this branch;
the retained media is therefore visual functionality evidence only. Re-capture
from a durable clean commit before using it as paper-source provenance.

## Multi-Agent Lab Evidence

Lab bundles use the same immutable manifest and artifact-hash mechanism. A
valid lab bundle records its `lab_variant`, controller facts, node facts, command
facts, and reservation-event history. The independent checker requires the
source checkout used for capture:

```bash
scripts/research/run-python - <<'PY'
from pathlib import Path
from scripts.research.check_artifact import check_bundle

report = check_bundle(
    Path("/absolute/path/to/lab-bundle"),
    expected_kind="lab",
    source_root=Path("/absolute/path/to/clean-checkout"),
)
report.require_valid()
PY
```

`PASS` means the retained artifact is structurally valid, not that it is a
remote-host measurement. `scripts/research/lab_qualification.py` classifies a
finalized lifecycle bundle from retained facts: a same-machine two-node run is
`PRELIMINARY`; `REMOTE QUALIFIED` requires two distinct Ubuntu 24.04 x86_64
machine fingerprints, a non-loopback route whose source equals the ingress-
observed peer, command evidence for both hosts, ordered retain/queue/resume/
terminal events, and matching clean source provenance. ARM64 and controlled-
network repetitions remain separate paper-evidence gates.

```bash
scripts/research/run-python scripts/research/lab_qualification.py \
  --bundle /absolute/path/to/lab-bundle \
  --source-root /absolute/path/to/clean-checkout
```
