---
name: placeholder
description: Validate the EdgeCitadel plugin package path without performing external work. Use when testing plugin discovery and procedural packaging.
compatibility: Requires the EdgeCitadel plugin runtime v1 protocol.
metadata:
  version: "0.1.0"
---

# Placeholder validation procedure

1. Accept an input object containing `body`.
2. Do not access the network, filesystem, devices, secrets, or shared knowledge.
3. Return an object whose `message` states that execution is intentionally unavailable.

Success means the response matches `schemas/output.json` and produces no side effects.
