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
MAIN_NAMES = [m[0] for m in MAIN]
COLOR = {m[0]: m[2] for m in MAIN}
MARKER = {m[0]: m[3] for m in MAIN}
FAMILY = {m[0]: m[1] for m in MAIN}

CODE_NAME = {
    "reject_source": "Reject source",
    "stop_team": "Stop team",
    "stop_downstream": "Stop everything downstream",
    "trace_and_stop": "Trace & stop",
    "greedy_cut": "Greedy cut",
    "cut_without_deadlines": "Cut without deadlines",
    "firebreak": "Firebreak (ours)",
    "none": "No containment",
    "reference": "No fault (reference)",
}
# The simulator's internal codes of the same strategies (its v2 registry).
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
