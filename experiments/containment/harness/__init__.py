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
- ``harness/svc.py``: the pre-registered service-continuity scenario (its
  generator, monitor settings and metrics)
- ``workloads/``: exported instances; ``experiments/``: grids (``lab-smoke``
  169 runs: every strategy x scenario x early/late alert once under the
  deployed profile, C2 under the shared profile and the C2 phase-1 ablation
  (Firebreak with ``freeze`` and ``freeze_plus_cut``); ``lab-pilot`` the same
  with 3 repetitions, 507; ``lab-main`` a draft sized after the pilot;
  ``lab-stress`` and ``lab-rootcause`` in the ranked-root mode; ``svc-smoke``
  21 and ``svc-pilot`` 105 runs of ``svc``); ``configs/``: stacks
  (``dryrun.json`` local sockets, ``dryrun-nats.json`` the lab's NATS control
  path locally, ``lab.json`` jim-eq + Endurance + Cluster;
  ``*-homeassistant.json`` the physical case)
- ``agent-packages/det-worker``: the deterministic worker. agentd's effect
  gateway performs its effects (``effect.execute``) on the stack's
  ``effect_sink``: the run's ledger (http adapter) or Home Assistant, fronted
  by the run's services, which record what reached it. A consuming step that
  takes a pushed input from the agent's memory reports it (``trace.use``);
  an agent in ``shared_memory_agents`` keeps one memory across invocations.
  An effect agentd holds under an egress label is recorded ``held``

External requests (root tasks of any agent, as in ``svc``) are submitted at
their release times by a ``nodectl schedule`` process on the agent's node,
which records each submission (``submitted``/``refused`` marks).

Every run: agentd enforces the fail-closed controller lease
(``controller_lease_ms``, 30 s) on every node and the monitor renews it from
its start, also in runs without containment (``none``, ``reference``), which
configure the monitor with ``enforce: false``. Each run records its mechanism
``profile`` (``deployed`` by default, ``shared`` as the ablation) and
``phase1_mode`` (by default the profile's: adaptive for the deployed cut
strategies, freeze under ``shared``), ``phase1_labels`` and
``receding_during_waits``; experiments set them per grid or part.

Latency settings and netem profiles above the measured LAN round trip
(1.5-4 ms between sites) must be labelled ``stress``; the runner refuses
unlabelled ones and records ``latency_regime`` per run.

Dry run (everything on this machine; from ``experiments/containment``)::

  export HARNESS_PYTHON=<python with the runtime's dependencies>
  export HARNESS_RUNTIME_ROOT=<this checkout (agentd and the monitor of this revision)>
  export HARNESS_WORK_ROOT=<scratch directory> HARNESS_NATS_SERVER=<nats-server binary>
  python -m harness run --stack configs/dryrun.json \\
      --experiment experiments/dryrun-c2.json --out <results>
  python -m harness score --out <results>
  CONTAIN_SIM_ROOT=<simulator> python -m harness sim --experiment ... --out <results>
  python -m harness report --out <results>     # pandas, numpy, matplotlib

``configs/dryrun-nats.json`` is the same stack with the lab's control path: the
monitor signs each command with a per-run owner key and sends it on the run's
NATS (agentd's ``edgecitadel.control.v1.<node>``). ``dryrun.json`` uses
co-located admin sockets.

Lab: run the same commands on jim-eq with ``configs/lab.json``; set
``HARNESS_SSH_ENDURANCE``/``HARNESS_SSH_CLUSTER`` (``user@host``),
``HARNESS_REPO`` and the ``HARNESS_REMOTE_*`` paths of the same checkouts on
the edge hosts. Every run uses its own ports, NATS, owner key and
``/var/tmp/edgecitadel-containment/<run>`` state on each host and never
touches the resident EdgeCitadel services. ``control_transport:
ssh_forward`` forwards remote admin sockets over SSH. The physical case
(``configs/lab-homeassistant.json``) also needs ``HARNESS_HA_URL`` (the Home
Assistant the services forward to), ``HARNESS_HA_TOKEN_FILE`` (the path, the
same on every edge host, of a mode-600 file with its long-lived token, owned by
the user running the experiment's agentd) and ``HARNESS_HA_TARGET`` (the
service and entity every effect acts on, ``<domain>.<service>/<entity_id>``,
for example a test ``input_boolean``). ``configs/lab-svc-homeassistant.json``
routes only the svc lamp (``dev_lamp``) to Home Assistant, only on ``core``
(site K), with ``HARNESS_HA_TARGET`` the desk lamp; every other class goes to
the ledger.
"""
