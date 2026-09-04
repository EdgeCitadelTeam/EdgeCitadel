"""Transport-neutral task execution value types."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PublicationReceipt:
    envelope_id: str
    accepted: bool
    transport: str
    stream: str | None
    stream_sequence: int | None
    duplicate: bool | None
    accepted_ns: int
    application_bytes: int
    wire_bytes: int | None


__all__ = ["PublicationReceipt"]
