# Recorded acceptance

`20260913-macos-arm64.json` and its JUnit XML record a completed run on
2026-09-13: **9 passed, 0 failures, 0 errors, 0 skipped**.

- Platform actually executed: macOS arm64, Python 3.12.14.
- Dependencies: NATS Server 2.14.6; nats-py 2.15.0.
- Isolation: copied only this validation directory outside the Git checkout;
  created a new virtual environment with only requirements.txt; downloaded the
  broker using bootstrap.py and verified its archive checksum.
- No application imports, Docker, production endpoints, model calls, or agents.
- The JSON includes SHA256 hashes of all executable source files, the broker
  binary hash, per-test timing, stored synthetic records, and duplicate ACKs.
- After this run, source hashes were checked against the committed-source
  candidates. A missing-binary negative control returned exit 1 and
  `all_passed: false`.

An earlier in-repository run also passed the same nine contracts. Bootstrap
supports macOS/Linux on amd64/arm64, but this acceptance record makes no claim
that Linux or amd64 was exercised. Runtime logs and local paths are not copied
into this tracked record. The main README defines the exact claim boundaries.

This is the standalone successor of the successful original audit sequence:
`(10,20,10)` connected; `(21,10)` on the Leaves during Core outage;
`(10,21,11)` after explicit retry/deduplication, with Leaf A retaining 21 records
after restart. The new suite additionally checks stored payloads and subjects.
