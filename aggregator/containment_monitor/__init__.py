"""Core containment monitor: provenance follower, root cause, firebreak and enforcement.

The monitor tails ``trace_raw_events`` like the projector, but keeps its own SQLite
file so Core's collector and projector writers are never contended. Modules:

- ``provenance``: the append-only provenance graph G_P (persisted) plus the live
  state and taint index (in memory), built from task/dispatch/run/lineage events.
- ``detectors``: deterministic alerts derived from events.
- ``rootcause``: backward slice, spectrum ranking and forward closure.
- ``projection``: the current control graph G_C (live state plus one layer of
  authority-bounded next events through scope hubs) with deadlines.
- ``solver``: deadline-aware minimum cut (gated gadget, enumeration, optional ILP).
- ``strategies``: Firebreak and the experiment baselines, one action vocabulary.
- ``control`` and ``enforcement``: the ControlClient interface and the two-phase
  confirm-then-release driver.
- ``service`` and ``routes``: the background thread, alert intake and status API.

Nothing here is imported by the aggregator unless the monitor is enabled.
"""
