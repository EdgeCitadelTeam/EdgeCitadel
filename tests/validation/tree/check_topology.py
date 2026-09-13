"""Independent broker contracts, using fixed subjects and exact stored-message oracles."""
from __future__ import annotations

import asyncio
import os
import shutil
import tempfile
import unittest
from pathlib import Path

from brokers import NAMES, UNAVAILABLE, Tree


class TreeContracts(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.evidence = {}
        self.temporary = tempfile.TemporaryDirectory(prefix="edgecitadel-tree-")
        self.addCleanup(self.temporary.cleanup)
        self.tree = Tree(Path(os.environ["TREE_NATS_BINARY"]), Path(self.temporary.name))
        self.addAsyncCleanup(self.clean_tree)
        await self.tree.setup()

    async def clean_tree(self):
        try:
            await self.tree.close()
        finally:
            output = Path(os.environ["TREE_OUTPUT"]) / self._testMethodName
            output.mkdir()
            for log in Path(self.temporary.name).glob("*.log"):
                shutil.copyfile(log, output / log.name)
            self.evidence["client_errors"] = self.tree.errors

    async def exact_records(self, expected, names=NAMES, label="stored"):
        snapshot = await self.tree.snapshot(names)
        self.assertEqual(snapshot, expected)
        self.evidence[label] = snapshot

    async def route(self, sender, subject, owner):
        expected = {name: [] for name in NAMES}
        for i in range(10):
            ident = f"{sender}-{owner}-{i}"
            ack = await self.tree.publish(sender, subject, ident)
            self.assertFalse(ack.duplicate)
            expected[owner].append({"subject": subject, "body": ident, "message_id": ident})
        await self.exact_records(expected)

    async def test_local_destination_owns_records(self):
        await self.route("a", "agents.a2.inbox", "a")

    async def test_core_to_leaf_destination_owns_records(self):
        await self.route("core", "agents.a1.inbox", "a")

    async def test_leaf_to_core_destination_owns_records(self):
        await self.route("a", "agents.core.inbox", "core")

    async def test_leaf_to_leaf_destination_owns_records(self):
        await self.route("a", "agents.b1.inbox", "b")

    async def disconnect(self):
        self.tree.stop("core")
        await self.tree.wait_links({"a": 0, "b": 0})

    async def restore(self):
        self.tree.start("core")
        await self.tree.connect("core")
        await self.tree.wait_links({"core": 2, "a": 1, "b": 1})
        await self.tree.barrier("a", "b")

    async def test_both_leaves_remain_locally_writable_without_core(self):
        await self.disconnect()
        expected = {}
        for name, subject in (("a", "agents.a2.inbox"), ("b", "agents.b1.inbox")):
            ident = f"offline-{name}"
            await self.tree.publish(name, subject, ident)
            expected[name] = [{"subject": subject, "body": ident, "message_id": ident}]
        await self.exact_records(expected, ("a", "b"))

    async def test_remote_publish_fails_without_destination_route(self):
        await self.disconnect()
        with self.assertRaises(UNAVAILABLE) as caught:
            await self.tree.publish("a", "agents.b1.inbox", "offline-remote")
        self.evidence["expected_error"] = type(caught.exception).__name__
        await self.exact_records({"a": [], "b": []}, ("a", "b"))

    async def test_reconnect_requires_retry_and_deduplicates_message_id(self):
        await self.disconnect()
        with self.assertRaises(UNAVAILABLE):
            await self.tree.publish("a", "agents.b1.inbox", "retry-me")
        await self.restore()
        # Bounded negative observation, not a universal claim about future delivery.
        for _ in range(5):
            await self.exact_records({"core": [], "a": [], "b": []}, label="before_retry")
            await asyncio.sleep(0.1)
        first = await self.tree.publish("a", "agents.b1.inbox", "retry-me")
        second = await self.tree.publish("a", "agents.b1.inbox", "retry-me")
        self.assertFalse(first.duplicate)
        self.assertTrue(second.duplicate)
        self.assertEqual(first.seq, second.seq)
        self.evidence["publish_ack"] = {"first_seq": first.seq, "retry_seq": second.seq,
                                        "duplicate": second.duplicate}
        await self.exact_records({"core": [], "a": [], "b": [
            {"subject": "agents.b1.inbox", "body": "retry-me", "message_id": "retry-me"}
        ]})

    async def test_leaf_restart_preserves_exact_records_and_accepts_new_writes(self):
        await self.tree.publish("a", "agents.a2.inbox", "before-restart")
        before = await self.tree.snapshot()
        self.tree.stop("a")
        self.tree.start("a")
        await self.tree.connect("a")
        await self.exact_records(before, label="after_restart")
        await self.tree.publish("a", "agents.a1.inbox", "after-restart")
        before["a"].append({"subject": "agents.a1.inbox", "body": "after-restart",
                            "message_id": "after-restart"})
        await self.exact_records(before, label="new_write")

    async def test_original_audit_sequence(self):
        # Reproduce the original 2026-09-13 diagnostic, with independent payload checks.
        expected = {name: [] for name in NAMES}
        for i in range(10):
            for sender, subject, owner, prefix in (
                ("a", "agents.a2.inbox", "a", "local"),
                ("core", "agents.a1.inbox", "a", "core-a"),
                ("a", "agents.core.inbox", "core", "a-core"),
                ("a", "agents.b1.inbox", "b", "a-b"),
            ):
                ident = f"{prefix}-{i}"
                await self.tree.publish(sender, subject, ident)
                expected[owner].append({"subject": subject, "body": ident, "message_id": ident})
        await self.exact_records(expected, label="connected")
        self.assertEqual([len(expected[n]) for n in NAMES], [10, 20, 10])
        await self.disconnect()
        await self.tree.publish("a", "agents.a1.inbox", "offline-local")
        expected["a"].append({"subject": "agents.a1.inbox", "body": "offline-local",
                               "message_id": "offline-local"})
        with self.assertRaises(UNAVAILABLE):
            await self.tree.publish("a", "agents.b1.inbox", "offline-remote")
        await self.exact_records({n: expected[n] for n in ("a", "b")}, ("a", "b"), "partitioned")
        await self.restore()
        await self.exact_records(expected, label="before_retry")
        await self.tree.publish("a", "agents.b1.inbox", "offline-remote")
        self.assertTrue((await self.tree.publish("a", "agents.b1.inbox", "offline-remote")).duplicate)
        expected["b"].append({"subject": "agents.b1.inbox", "body": "offline-remote",
                              "message_id": "offline-remote"})
        await self.exact_records(expected, label="recovered")
        self.assertEqual([len(expected[n]) for n in NAMES], [10, 21, 11])
        self.tree.stop("a")
        self.tree.start("a")
        await self.tree.connect("a")
        await self.exact_records(expected, label="persisted")
