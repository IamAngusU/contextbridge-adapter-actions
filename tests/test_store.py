from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from contextbridge_actions_adapter.contracts import ActionEnvelope
from contextbridge_actions_adapter.store import ActionStore, ActionStoreError, AmbiguousActionError


class StoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = ActionStore(Path(self.temp.name) / "actions.db")
        self.owner = "channel-primary"
        self.tenant = "tenant-a"
        self.destination = self.store.add_github_destination(self.owner, self.tenant, "IamAngusU/ContextBridge")
        self.payload = self.store.stage_payload(
            self.owner,
            self.tenant,
            self.destination,
            "github.issue.comment",
            b'{"issue_number":126,"body":"bounded"}',
            ttl=timedelta(hours=1),
        )
        now = datetime.now(timezone.utc)
        self.envelope = ActionEnvelope(
            "sact_" + "a" * 32,
            "adp_" + "b" * 32,
            1,
            "github.issue.comment",
            self.destination,
            self.payload,
            now - timedelta(seconds=1),
            now + timedelta(minutes=5),
            self.owner,
            self.tenant,
        )

    def test_scope_binding_and_unknown_fence(self) -> None:
        prepared = self.store.prepare(self.envelope)
        self.assertEqual(prepared.repository, "IamAngusU/ContextBridge")
        self.store.mark_mutating(self.envelope, prepared.request_digest)
        self.store.mark_unknown(self.envelope, prepared.request_digest, "timeout")
        self.assertEqual(self.store.attempt_state(self.envelope.schedule_id, 1), "unknown")
        with self.assertRaises(AmbiguousActionError):
            self.store.prepare(self.envelope)

    def test_completed_receipt_is_replayed_without_a_second_mutation(self) -> None:
        prepared = self.store.prepare(self.envelope)
        self.store.mark_mutating(self.envelope, prepared.request_digest)
        receipt = {"provider": "github", "external_id": "42", "url": "https://github.com/o/r/issues/1"}
        self.store.mark_completed(self.envelope, prepared.request_digest, receipt)
        cached = self.store.prepare(self.envelope)
        self.assertEqual(cached.cached_receipt, receipt)

    def test_wrong_scope_and_rebound_occurrence_fail_closed(self) -> None:
        wrong = ActionEnvelope(**{**self.envelope.__dict__, "owner_subject": "other-owner"})
        with self.assertRaises(ActionStoreError):
            self.store.prepare(wrong)
        prepared = self.store.prepare(self.envelope)
        rebound = ActionEnvelope(**{**self.envelope.__dict__, "payload_ref": "ref_" + "f" * 32})
        with self.assertRaises(ActionStoreError):
            self.store.prepare(rebound)
        self.assertEqual(self.store.attempt_state(self.envelope.schedule_id, 1), "prepared")
        self.store.mark_mutating(self.envelope, prepared.request_digest)

    def test_inactive_destination_cannot_be_staged_or_executed(self) -> None:
        self.assertFalse(self.store.set_destination_active(self.destination, False, "another-owner", self.tenant))
        self.assertTrue(self.store.set_destination_active(self.destination, False, self.owner, self.tenant))
        with self.assertRaises(ActionStoreError):
            self.store.stage_payload(
                self.owner,
                self.tenant,
                self.destination,
                "github.issue.create",
                b'{"title":"x","body":"y"}',
                ttl=timedelta(hours=1),
            )
        with self.assertRaises(ActionStoreError):
            self.store.prepare(self.envelope)

    def test_destination_listing_is_scope_bound(self) -> None:
        listed = self.store.list_destinations(self.owner, self.tenant)
        self.assertEqual([item["destination_ref"] for item in listed], [self.destination])
        self.assertEqual(self.store.list_destinations("another-owner", self.tenant), [])
        self.assertEqual(self.store.list_destinations(self.owner, "another-tenant"), [])


if __name__ == "__main__":
    unittest.main()
