# Source this file in bash on jim-eq. Defining these functions submits no tasks.
# Every ec_send is ONE explicit submission; ec_wait only observes it.
EC_BASE=${EC_BASE:-http://127.0.0.1}

ec_id() {
    python3 -c 'import uuid; print(uuid.uuid4())'
}

ec_send() {
    if [ "$#" -lt 5 ] || [ "$#" -gt 6 ]; then
        echo 'usage: ec_send AGENT SKILL SESSION REQUEST BODY [CLAIMED_SENDER]' >&2
        return 2
    fi
    python3 - "$EC_BASE" "$@" <<'PY'
import json, sys, urllib.parse, urllib.request
base, agent, skill, session, request, body, *sender = sys.argv[1:]
url = base.rstrip('/') + '/api/command/' + urllib.parse.quote(agent, safe='')
if sender:
    url += '?' + urllib.parse.urlencode({'sender_id': sender[0]})
payload = {'body': body, 'skill_id': skill, 'args': {'session_id': session, 'request_id': request}}
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
req = urllib.request.Request(url, json.dumps(payload).encode(), {'Content-Type': 'application/json'})
with opener.open(req, timeout=20) as response:
    receipt = json.load(response)
print(receipt['task_id'])
PY
}

ec_wait() {
    if [ "$#" -ne 2 ]; then
        echo 'usage: ec_wait TASK AGENT' >&2
        return 2
    fi
    python3 - "$EC_BASE" "$1" "$2" <<'PY'
import json, sys, time, urllib.parse, urllib.request
base, task, agent = sys.argv[1:]
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
url = base.rstrip('/') + '/api/messages?' + urllib.parse.urlencode({'task_id': task, 'limit': 200})
deadline = time.monotonic() + 180
while time.monotonic() < deadline:
    with opener.open(url, timeout=15) as response:
        messages = json.load(response)
    results = [m for m in messages if m['type'] == 'result' and m['sender_id'] == agent]
    if results:
        print(json.dumps(max(results, key=lambda m: m.get('observation_index', 0)), indent=2, ensure_ascii=False))
        break
    time.sleep(2)
else:
    print('UNCONFIRMED: waiting ended, remote work was not cancelled. Re-run ec_wait with the same task ID.', file=sys.stderr)
    sys.exit(2)
PY
}

ec_graph() {
    python3 - "$EC_BASE" "$1" <<'PY'
import json, sys, urllib.parse, urllib.request
base, task = sys.argv[1:]
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
def get(path):
    with opener.open(base.rstrip('/') + path, timeout=15) as response:
        return json.load(response)
listing = get('/api/traces?' + urllib.parse.urlencode({'task_id': task, 'limit': 10}))
graphs = [get('/api/traces/' + item['trace_id']) for item in listing.get('items', [])]
print(json.dumps(graphs, indent=2, ensure_ascii=False))
PY
}
