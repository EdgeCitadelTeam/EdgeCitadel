"""Authenticated, lab-only node reservation inventory."""

from __future__ import annotations

import hashlib
import hmac
import json
import sqlite3
import threading
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Request, Response

from scripts.research.lab_config import (
    LabConfigError,
    qualified_agent_id,
    validate_agent_id,
    validate_declared_host_id,
    validate_run_id,
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS reservations (
    run_id TEXT NOT NULL, agent_id TEXT NOT NULL, qualified_agent_id TEXT NOT NULL,
    reservation_id TEXT NOT NULL, declared_host_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('active', 'retained')), updated_at TEXT NOT NULL,
    PRIMARY KEY (run_id, agent_id)
);
CREATE TABLE IF NOT EXISTS reservation_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT NOT NULL, agent_id TEXT NOT NULL,
    qualified_agent_id TEXT NOT NULL, reservation_id TEXT NOT NULL, declared_host_id TEXT NOT NULL,
    event TEXT NOT NULL CHECK (event IN ('reserved', 'retained', 'resumed', 'released')),
    observed_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS node_reports (
    run_id TEXT NOT NULL, agent_id TEXT NOT NULL, reservation_id TEXT NOT NULL,
    declared_host_id TEXT NOT NULL, report_json TEXT NOT NULL,
    PRIMARY KEY (run_id, agent_id)
);
"""


def _timestamp() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def build_lab_router(*, run_id: str, token_sha256: str, inventory_path: Path) -> APIRouter:
    validated_run_id = validate_run_id(run_id)
    if not inventory_path.is_absolute():
        raise LabConfigError("lab inventory path must be absolute")
    inventory_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(inventory_path, check_same_thread=False)
    connection.row_factory = sqlite3.Row
    connection.executescript(_SCHEMA)
    connection.commit()
    lock = threading.Lock()
    router = APIRouter(prefix="/api/lab")

    def _authorize(authorization: Annotated[str | None, Header()] = None) -> None:
        if authorization is None or not authorization.startswith("Bearer "):
            raise HTTPException(401, "invalid lab authorization")
        token = authorization.removeprefix("Bearer ")
        candidate = hashlib.sha256(token.encode()).hexdigest()
        if not hmac.compare_digest(candidate, token_sha256):
            raise HTTPException(401, "invalid lab authorization")

    def _body(body: dict[str, object]) -> tuple[str, str, str, str]:
        try:
            agent_id = validate_agent_id(str(body["agent_id"]))
            reservation_id = str(body["reservation_id"])
            declared_host_id = validate_declared_host_id(str(body["declared_host_id"]))
            expected_qualified = qualified_agent_id(validated_run_id, agent_id)
        except (KeyError, LabConfigError) as error:
            raise HTTPException(422, "invalid lab reservation") from error
        if body.get("qualified_agent_id") != expected_qualified or not reservation_id:
            raise HTTPException(422, "invalid lab reservation")
        return agent_id, expected_qualified, reservation_id, declared_host_id

    def _event(agent_id: str, qualified: str, reservation: str, host: str, event: str) -> None:
        connection.execute(
            "INSERT INTO reservation_events VALUES (NULL, ?, ?, ?, ?, ?, ?, ?)",
            (validated_run_id, agent_id, qualified, reservation, host, event, _timestamp()),
        )

    @router.post("/reservations", dependencies=[])
    def reserve(body: dict[str, object], _: None = Depends(_authorize)):
        agent_id, qualified, reservation_id, host = _body(body)
        with lock:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute("SELECT * FROM reservations WHERE run_id=? AND agent_id=?", (validated_run_id, agent_id)).fetchone()
                if row is None:
                    connection.execute("INSERT INTO reservations VALUES (?, ?, ?, ?, ?, 'active', ?)", (validated_run_id, agent_id, qualified, reservation_id, host, _timestamp()))
                    _event(agent_id, qualified, reservation_id, host, "reserved")
                    connection.commit()
                    return Response(status_code=201)
                if row["state"] == "active":
                    raise HTTPException(409, "agent_id has an active reservation")
                if row["reservation_id"] != reservation_id or row["declared_host_id"] != host:
                    raise HTTPException(409, "reservation owner does not match")
                connection.execute("UPDATE reservations SET state='active', updated_at=? WHERE run_id=? AND agent_id=?", (_timestamp(), validated_run_id, agent_id))
                _event(agent_id, qualified, reservation_id, host, "resumed")
                connection.commit()
                return Response(status_code=200)
            except BaseException:
                connection.rollback()
                raise

    @router.patch("/reservations/{agent_id}/retain")
    def retain(agent_id: str, body: dict[str, object], _: None = Depends(_authorize)):
        claimed_agent, qualified, reservation_id, host = _body(body)
        if validate_agent_id(agent_id) != claimed_agent:
            raise HTTPException(409, "reservation owner does not match")
        with lock:
            row = connection.execute("SELECT * FROM reservations WHERE run_id=? AND agent_id=?", (validated_run_id, agent_id)).fetchone()
            if (
                row is None
                or row["qualified_agent_id"] != qualified
                or row["reservation_id"] != reservation_id
                or row["declared_host_id"] != host
            ):
                raise HTTPException(409, "reservation owner does not match")
            if row["state"] == "retained":
                return Response(status_code=200)
            connection.execute("UPDATE reservations SET state='retained', updated_at=? WHERE run_id=? AND agent_id=?", (_timestamp(), validated_run_id, agent_id))
            _event(agent_id, qualified, reservation_id, host, "retained")
            connection.commit()
        return Response(status_code=200)

    @router.delete("/reservations/{agent_id}")
    def release(agent_id: str, body: dict[str, object], _: None = Depends(_authorize)):
        claimed_agent, qualified, reservation_id, host = _body(body)
        if validate_agent_id(agent_id) != claimed_agent:
            raise HTTPException(409, "reservation owner does not match")
        with lock:
            row = connection.execute("SELECT * FROM reservations WHERE run_id=? AND agent_id=?", (validated_run_id, agent_id)).fetchone()
            if row is None:
                released = connection.execute("SELECT 1 FROM reservation_events WHERE run_id=? AND agent_id=? AND reservation_id=? AND declared_host_id=? AND event='released'", (validated_run_id, agent_id, reservation_id, host)).fetchone()
                if released is None:
                    raise HTTPException(404, "reservation not found")
                return Response(status_code=204)
            if row["reservation_id"] != reservation_id or row["declared_host_id"] != host:
                raise HTTPException(409, "reservation owner does not match")
            _event(agent_id, qualified, reservation_id, host, "released")
            connection.execute("DELETE FROM reservations WHERE run_id=? AND agent_id=?", (validated_run_id, agent_id))
            connection.commit()
        return Response(status_code=204)

    @router.post("/node-reports")
    def node_report(body: dict[str, object], request: Request, _: None = Depends(_authorize)):
        agent_id, qualified, reservation_id, host = _body(body)
        required = {
            "machine_id_sha256", "hostname", "os_release", "architecture",
            "launcher_source_commit", "source_snapshot_sha256", "network_path",
            "preflight_valid", "lifecycle_state", "cleanup", "checked_at",
        }
        if not required.issubset(body) or body.get("lifecycle_state") not in {"active", "retained", "released"}:
            raise HTTPException(422, "invalid lab node report")
        network_path = body.get("network_path")
        if not isinstance(network_path, dict) or set(network_path) != {
            "source_ip", "destination_ip", "interface", "route_output_sha256", "controller_dns_name",
        }:
            raise HTTPException(422, "invalid lab node report")
        with lock:
            reservation = connection.execute("SELECT * FROM reservations WHERE run_id=? AND agent_id=?", (validated_run_id, agent_id)).fetchone()
            if reservation is None or reservation["reservation_id"] != reservation_id or reservation["declared_host_id"] != host or reservation["qualified_agent_id"] != qualified:
                raise HTTPException(409, "node report reservation does not match")
            saved = dict(body)
            saved["server_observed_peer_ip"] = request.client.host if request.client else "unknown"
            connection.execute(
                "INSERT INTO node_reports VALUES (?, ?, ?, ?, ?) ON CONFLICT(run_id, agent_id) DO UPDATE SET reservation_id=excluded.reservation_id, declared_host_id=excluded.declared_host_id, report_json=excluded.report_json",
                (validated_run_id, agent_id, reservation_id, host, json.dumps(saved, sort_keys=True, separators=(",", ":"))),
            )
            connection.commit()
        return saved

    @router.get("/status")
    def status(_: None = Depends(_authorize)):
        with lock:
            reservations = [dict(row) for row in connection.execute("SELECT * FROM reservations ORDER BY agent_id")]
            events = [dict(row) for row in connection.execute("SELECT * FROM reservation_events ORDER BY sequence")]
            reports = [json.loads(row["report_json"]) for row in connection.execute("SELECT * FROM node_reports ORDER BY agent_id")]
        return {
            "run_id": validated_run_id,
            "reservations": reservations,
            "reservation_events": events,
            "node_reports": reports,
        }

    return router


__all__ = ["build_lab_router"]
