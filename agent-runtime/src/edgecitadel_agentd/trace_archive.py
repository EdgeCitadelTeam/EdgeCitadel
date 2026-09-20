"""Import validated JSONL historical observations through the local admin socket."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from uuid import uuid4

from .client import AgentdClient, AgentdClientError
from .service import socket_path_for
from .trace_contract import (
    MAX_EVENT_BYTES,
    TraceContractError,
    validate_import_request,
    validate_rpc_reply,
)

MAX_ARCHIVE_BYTES = 16 * 1024 * 1024


def read_archive(path: Path, source_id: str) -> list[dict]:
    """Validate the entire bounded archive before changing grants or records.

    Lines use the existing historical observation contract: agent_id,
    historical_run_id, record_id and observation. The import source is selected
    by the operator and remains stable across retries. Request IDs are transport
    identities only; durable deduplication uses the archive's record IDs.
    """
    with path.open("rb") as source:
        encoded = source.read(MAX_ARCHIVE_BYTES + 1)
    if len(encoded) > MAX_ARCHIVE_BYTES:
        raise ValueError("Trace archive exceeds 16 MiB; split it into smaller files")
    records = []
    identities = {}
    for line_number, line in enumerate(encoded.splitlines(), 1):
        if not line.strip():
            continue
        try:
            if len(line) > MAX_EVENT_BYTES:
                raise ValueError("record too large")
            record = json.loads(line)
            if not isinstance(record, dict) or set(record) != {
                "agent_id",
                "historical_run_id",
                "record_id",
                "observation",
            }:
                raise ValueError("unexpected record fields")
            params = {
                **record,
                "schema_version": 1,
                "request_id": str(uuid4()),
                "import_source_id": source_id,
            }
            validate_import_request(params)
            key = (record["agent_id"], record["historical_run_id"], record["record_id"])
            if key in identities and identities[key] != record:
                raise ValueError("conflicting duplicate record")
            if key not in identities:
                identities[key] = record
                records.append(params)
        except (ValueError, RecursionError, TraceContractError) as error:
            raise ValueError(
                f"Invalid trace archive record at line {line_number}"
            ) from error
    if not records:
        raise ValueError("Trace archive contains no records")
    return records


def import_archive(client: AgentdClient, records: list[dict]) -> dict:
    granted = set()
    traces = set()
    completed = 0
    for params in records:
        scope = (params["import_source_id"], params["agent_id"])
        try:
            if scope not in granted:
                client.call(
                    "trace.import.configure",
                    import_source_id=scope[0],
                    agent_id=scope[1],
                    enabled=True,
                )
                granted.add(scope)
            result = client.call("trace.import", **params)
            validate_rpc_reply(
                result, operation="import", request_id=params["request_id"]
            )
            if result["status"] != "ok":
                raise AgentdClientError(result["code"])
        except (AgentdClientError, TraceContractError) as error:
            raise AgentdClientError(
                f"Import stopped after {completed} records: {error}. "
                "Retry the same archive and source ID to resume without duplicates."
            ) from error
        traces.add(result["result"]["trace_id"])
        completed += 1
    return {"records": completed, "trace_ids": sorted(traces)}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Import a historical trace archive")
    parser.add_argument("archive", type=Path)
    parser.add_argument("--source-id", required=True)
    parser.add_argument("--state-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        records = read_archive(args.archive, args.source_id)
        token = (args.state_dir / "admin.token").read_text().strip()
        client = AgentdClient(socket_path_for(args.state_dir), admin_token=token)
        result = import_archive(client, records)
    except (OSError, ValueError, TraceContractError, AgentdClientError) as error:
        print(f"Trace import failed: {error}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
