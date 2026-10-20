"""Experiment harness for the containment paper's mechanism (tier-1) experiments.

A workload instance (``spec``) runs on a real EdgeCitadel runtime with
deterministic workers (``agent-packages/det-worker``), the Core monitor
answers one alert per run with the strategy under test, and independent
ground truth (an effect ledger, worker events and the workload itself) scores
the run. The same instance also runs in the offline simulator.

Layout (``experiments/containment``):

- ``harness/spec.py``: workload format, validation, runtime realization, plan
- ``harness/simexport.py``: simulator workflows <-> specs (sim-to-real)
- ``harness/groundtruth.py``, ``metrics.py``: ground truth and per-run metrics
- ``harness/services.py``: ledger, worker events and readiness (one per run)
- ``harness/deploy.py``: agent grants, slots and the monitor configuration
- ``harness/stack.py``, ``nodectl.py``, ``coreq.py``, ``aggregator_shim.py``:
  the per-run stack on local or SSH hosts
- ``harness/runner.py``: the paired grid and one run's control loop
- ``harness/simbackend.py``, ``report.py``, ``names.py``: simulator rows,
  CSVs, statistics and figures; ``netem.py``: lab latency profiles
- ``workloads/``: exported instances; ``experiments/``: grids; ``configs/``:
  stacks (``dryrun.json`` local, ``lab.json`` jim-eq + Endurance + Cluster)

Dry run (everything on this machine; from ``experiments/containment``)::

  export HARNESS_PYTHON=<python with the runtime's dependencies>
  export HARNESS_RUNTIME_ROOT=<EdgeCitadel checkout with agentd + monitor (m3/integration)>
  export HARNESS_WORK_ROOT=<scratch directory> HARNESS_NATS_SERVER=<nats-server binary>
  python -m harness run --stack configs/dryrun.json \\
      --experiment experiments/dryrun-c2.json --out <results>
  python -m harness score --out <results>
  CONTAIN_SIM_ROOT=<simulator> python -m harness sim --experiment ... --out <results>
  python -m harness report --out <results>     # pandas, numpy, matplotlib

``configs/dryrun-nats.json`` is the same stack with the lab's control path: the
monitor signs each command with a per-run owner key and sends it on the run's
NATS (agentd's ``edgecitadel.control.v1.<node>``), which needs a monitor with
``NatsControlClient`` (m3/integration ab174ed or later). ``dryrun.json`` uses
co-located admin sockets and works with earlier monitors too.

Lab: run the same commands on jim-eq with ``configs/lab.json``; set
``HARNESS_SSH_ENDURANCE``/``HARNESS_SSH_CLUSTER`` (``user@host``),
``HARNESS_REPO`` and the ``HARNESS_REMOTE_*`` paths of the same checkouts on
the edge hosts. Every run uses its own ports, NATS, owner key and
``/var/tmp/edgecitadel-containment/<run>`` state on each host and never
touches the resident EdgeCitadel services. ``control_transport:
ssh_forward`` forwards remote admin sockets over SSH for monitors without
NATS control.
"""
