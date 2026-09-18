"""Bounded wire ingestion and commit-before-ACK delivery adapter."""

from __future__ import annotations

import json
import sqlite3
import time
from collections.abc import Callable

from edgecitadel_agentd.trace_contract import (
    MAX_WRAPPER_BYTES,
    TraceContractError,
    validate_export,
    validate_export_header,
)
from nats.aio.msg import Msg

from .trace_store import IngestResult, ingest, record_poison, reject_payload


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate_json_key")
        result[key] = value
    return result


def ingest_wire(
    connection: sqlite3.Connection,
    subject: str,
    payload: bytes,
    *,
    received_at_ms: int,
) -> IngestResult:
    """Return only after durable disposition; exceptions must prevent broker ACK.

    Unknown/invalid envelope origins never receive a source receipt. Valid envelope
    payload rejection retains a hash-only receipt, subject to an explicit hard cap.
    """
    reason = None
    if len(payload) > MAX_WRAPPER_BYTES:
        reason = "wire"
    else:
        try:
            record = json.loads(payload.decode("utf-8"), object_pairs_hook=_object)
        except (ValueError, UnicodeError, RecursionError):
            reason = "wire"
        else:
            if not isinstance(record, dict):
                reason = "wrapper"
            else:
                try:
                    validate_export_header(record)
                except TraceContractError:
                    reason = "wrapper"
                else:
                    if (
                        subject != f"edgecitadel.telemetry.v1.{record['node_id']}"
                        or any(
                            record["event"].get(key) != record[key]
                            for key in ("node_id", "source_epoch")
                        )
                    ):
                        reason = "origin"
                    else:
                        try:
                            validate_export(record)
                        except TraceContractError:
                            return reject_payload(
                                connection,
                                subject,
                                record,
                                received_at_ms=received_at_ms,
                            )
                        return ingest(
                            connection, subject, record, received_at_ms=received_at_ms
                        )
    return record_poison(connection, reason, received_at_ms=received_at_ms)


async def ingest_delivery(
    connection: sqlite3.Connection,
    message: Msg,
    *,
    on_commit: Callable[[IngestResult], None] | None = None,
) -> IngestResult:
    """Commit a durable disposition before ACK; failures leave redelivery possible.

    The owning collector must give this adapter a dedicated connection and keep
    blocking persistence off the command-handling loop when integrating startup.
    """
    result = ingest_wire(
        connection,
        message.subject,
        message.data,
        received_at_ms=time.time_ns() // 1_000_000,
    )
    if on_commit is not None:
        try:
            on_commit(result)
        except Exception:  # noqa: BLE001, S110 - observation must never change ACK semantics
            pass
    await message.ack_sync(timeout=5)
    return result
