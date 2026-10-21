"""Paper names, families, colours and markers (the simulator's ``contain/names.py``).

Kept identical to the simulator's v2 figures so runtime and simulator results
read as one system; ``tests/test_report.py`` checks the copy against the
simulator when ``CONTAIN_SIM_ROOT`` is set. Monitor strategy codes map to the
display names in ``CODE_NAME``.
"""

FAMILY_NO = "no lineage"
FAMILY_PAST = "past lineage"
FAMILY_FUT = "past lineage + future authority"

MAIN = [
    ("Reject source", FAMILY_NO, "#c2c2c2", "v"),
    ("Stop team", FAMILY_NO, "#8c8c8c", "s"),
    ("Stop everything downstream", FAMILY_NO, "#525252", "X"),
    ("Trace & stop", FAMILY_PAST, "#0072B2", "D"),
    ("Greedy cut", FAMILY_FUT, "#E69F00", "^"),
    ("Cut without deadlines", FAMILY_FUT, "#CC79A7", "P"),
    ("Firebreak (ours)", FAMILY_FUT, "#D55E00", "*"),
]
# Runtime-only strategies (team-approved 2026-10-21; in the monitor since
# m4/design). They have no simulator pipeline yet, so they are not part of the
# simulator's MAIN list; their colours and markers are the Okabe-Ito entries
# MAIN leaves free. Sink guard traces nothing (it guards every protected sink
# on alert), so it is a no-lineage baseline; Trace & quarantine traces the
# declared closure like Trace & stop.
EXTRA = [
    ("Trace & quarantine", FAMILY_PAST, "#56B4E9", "o"),
    ("Sink guard", FAMILY_NO, "#009E73", "h"),
]
# The Google A2A baseline's own controls (harness.a2a_control): no lineage,
# run on the A2A stack only. Greys and markers close to their nearest
# EdgeCitadel strategy (Reject source, Stop team, Stop everything downstream).
SYSTEM_BASELINES = [
    ("A2A: block bad input", FAMILY_NO, "#d9d9d9", "1"),
    ("A2A: cancel faulty agent", FAMILY_NO, "#a6a6a6", "2"),
    ("A2A: cancel all", FAMILY_NO, "#3a3a3a", "3"),
]
MAIN_NAMES = [m[0] for m in MAIN]
PLOTTED = [*MAIN, *EXTRA, *SYSTEM_BASELINES]
PLOTTED_NAMES = [m[0] for m in PLOTTED]
COLOR = {m[0]: m[2] for m in PLOTTED}
MARKER = {m[0]: m[3] for m in PLOTTED}
FAMILY = {m[0]: m[1] for m in PLOTTED}

CODE_NAME = {
    "reject_source": "Reject source",
    "stop_team": "Stop team",
    "stop_downstream": "Stop everything downstream",
    "trace_and_stop": "Trace & stop",
    "greedy_cut": "Greedy cut",
    "cut_without_deadlines": "Cut without deadlines",
    "firebreak": "Firebreak (ours)",
    "trace_and_quarantine": "Trace & quarantine",
    "sink_guard": "Sink guard",
    "a2a_block_input": "A2A: block bad input",
    "a2a_cancel_agent": "A2A: cancel faulty agent",
    "a2a_cancel_all": "A2A: cancel all",
    "none": "No containment",
    "reference": "No fault (reference)",
}
# Monitor strategy codes the harness can run (``none`` is no alert at all).
STRATEGY_CODES = tuple(c for c in CODE_NAME if c not in {"none", "reference"})
# Codes the A2A stack runs (and the EdgeCitadel monitor does not).
NATIVE_CODES = ("a2a_block_input", "a2a_cancel_agent", "a2a_cancel_all")
# The simulator's internal codes of the same strategies (its v2 registry);
# strategies without one are skipped by the simulator backend.
SIM_CODE = {
    "reject_source": "B1",
    "stop_team": "B2",
    "stop_downstream": "B3",
    "trace_and_stop": "B3-decl",
    "greedy_cut": "B4-edge/h1-sink/rh",
    "cut_without_deadlines": "B5-static/h1-sink/rh",
    "firebreak": "B5-edge/h1-sink/rh",
    "none": "NONE",
}
