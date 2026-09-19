// Read-only jim-eq diagnostic. The credential and signed cursors stay in memory.
const { execFileSync } = require('node:child_process');
const { writeFileSync } = require('node:fs');
const path = require('node:path');

const [trace, output] = process.argv.slice(2);
if (!/^[0-9a-f]{32}$/.test(trace) || !output || !path.isAbsolute(output)) {
  throw new Error('Expected retained trace ID and absolute report path');
}
const credential = execFileSync('ssh', ['-o', 'BatchMode=yes', 'root@jim-eq',
  `python3 -c 'from pathlib import Path; print(next(line.split("=",1)[1] for line in Path("/root/.edgecitadel/core/.env").read_text().splitlines() if line.startswith("EDGECITADEL_TRACE_READ_TOKEN=")))'`], { encoding: 'utf8' }).trim();
const timings = [];
async function read(suffix, kind) {
  const start = performance.now();
  const response = await fetch(`http://jim-eq/api/traces/${trace}${suffix}`, {
    headers: { Authorization: `Bearer ${credential}` }, signal: AbortSignal.timeout(20_000),
  });
  const value = await response.json();
  timings.push({ kind, status: response.status, milliseconds: performance.now() - start });
  if (!response.ok) throw new Error(`Read failed: ${response.status} ${value.code}`);
  return value;
}
async function main() {
  const history = await read('/history?limit=100', 'history');
  const selected = history.items.at(-1);
  if (!selected?.at) throw new Error('Retained initial graph unavailable');
  const graph = await read('?at=' + encodeURIComponent(selected.at), 'graph');
  let cursor = graph.resume_cursor;
  const counts = { patch: 0, snapshot: 0, clear: 0 };
  let pages = 0;
  while (true) {
    if (++pages > 200) throw new Error('Replay exceeds diagnostic scan bound');
    const response = await read('/changes?after=' + encodeURIComponent(cursor), 'changes');
    for (const change of response.changes) counts[change.mode]++;
    if (!response.next_cursor) break;
    if (response.next_cursor === cursor) throw new Error('Replay made no cursor progress');
    cursor = response.next_cursor;
  }
  const report = { host: 'jim-eq', trace_id: trace, start_position: selected.position,
    history_upper: history.upper_position, initial_nodes: graph.total_nodes,
    change_pages: pages, modes: counts, timings,
    scope: 'single retained replay diagnostic; no browser latency, p95 or memory claim',
  };
  writeFileSync(output, JSON.stringify(report, null, 2) + '\n');
  console.log(JSON.stringify(report));
}
main().catch(error => { console.error(error.message); process.exitCode = 1; });
