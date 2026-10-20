"""Verify retained snapshots while both source daemons are stopped, then restore."""

import json
import platform
import subprocess
import sys
from urllib.parse import urlencode
from urllib.request import urlopen

assert platform.node().lower() == "jim-eq"
trace = sys.argv[1]


def get(path, **params):
    with urlopen(
        "http://127.0.0.1" + path + ("?" + urlencode(params) if params else ""),
        timeout=15,
    ) as response:
        return json.load(response)


def events(snapshot):
    result = []
    after = None
    while True:
        page = get(
            f"/api/traces/{trace}/events",
            as_of=snapshot,
            limit=100,
            **({"after": after} if after else {}),
        )
        result.extend(page["events"])
        after = page["next_cursor"]
        if not after:
            return result


graph = get(f"/api/traces/{trace}")
before = events(graph["at"])
assert before
stopped = []
try:
    for name, uid, unit in [
        ("leaf", 993, "edgecitadel-agentd-b5184c2bc72c"),
        ("core", 994, "edgecitadel-agentd-ee44ba7c372a"),
    ]:
        cmd = [
            "runuser",
            "-u",
            f"edgecitadel-{name}",
            "--",
            "env",
            f"XDG_RUNTIME_DIR=/run/user/{uid}",
            "systemctl",
            "--user",
        ]
        # Track restoration before stopping, including a partially failed stop.
        stopped.append((cmd, unit))
        subprocess.run(cmd + ["stop", unit], check=True, timeout=30)
        assert subprocess.run(cmd + ["is-active", "--quiet", unit]).returncode != 0
    retained = get(f"/api/traces/{trace}", at=graph["at"])
    assert retained["nodes"] == graph["nodes"] and retained["edges"] == graph["edges"]
    assert events(graph["at"]) == before
    assert any(e.get("content", {}).get("fields") for e in before)
    print(
        json.dumps(
            {
                "trace_id": trace,
                "source_daemons_offline": ["core", "leaf"],
                "retained_graph_equal": True,
                "retained_events_equal": True,
                "durable_content": True,
                "event_count": len(before),
            }
        )
    )
finally:
    for cmd, unit in reversed(stopped):
        subprocess.run(cmd + ["start", unit], check=True, timeout=30)
        subprocess.run(cmd + ["is-active", "--quiet", unit], check=True)
