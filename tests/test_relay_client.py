from __future__ import annotations

import io
import json
import unittest
from email.message import Message
from typing import Any

from contextbridge_actions_adapter.relay_client import RelayClient, RelayProtocolError


class FakeResponse:
    def __init__(self, value: Any) -> None:
        self.headers = Message()
        self.raw = io.BytesIO(json.dumps(value).encode())

    def read(self, size: int = -1) -> bytes:
        return self.raw.read(size)

    def __enter__(self) -> FakeResponse:
        return self

    def __exit__(self, *_args: object) -> None:
        return None


class SequenceOpener:
    def __init__(self, *responses: Any) -> None:
        self.responses = list(responses)
        self.requests: list[Any] = []

    def open(self, request: Any, timeout: int) -> Any:
        self.requests.append(request)
        return FakeResponse(self.responses.pop(0))


class RelayClientTests(unittest.TestCase):
    def test_identity_and_presence_are_scoped(self) -> None:
        uid = "adp_" + "a" * 32
        opener = SequenceOpener(
            {
                "schema": "contextbridge.identity.v1",
                "role": "producer",
                "subject": "channel-primary",
                "permissions": ["scheduled-actions:write-own"],
                "producer_limits": {"allowed_tenants": ["tenant-a"]},
            },
            {
                "schema": "contextbridge.adapter-presence-lease.v1",
                "adapter_uid": uid,
                "enabled": True,
                "available": True,
                "heartbeat_after_seconds": 20,
            },
        )
        client = RelayClient("http://127.0.0.1:32150", "t" * 40)
        client._opener = opener  # type: ignore[assignment]
        identity = client.whoami()
        lease = client.heartbeat(adapter_id="actions-primary", instance_id="host-actions")
        self.assertEqual(identity.subject, "channel-primary")
        self.assertEqual(identity.allowed_tenants, ("tenant-a",))
        self.assertTrue(identity.scheduled_actions)
        self.assertEqual(lease.adapter_uid, uid)
        sent = json.loads(opener.requests[1].data)
        self.assertEqual(sent["capabilities"], ["scheduled-action"])
        self.assertEqual(sent["kind"], "control")

    def test_remote_cleartext_and_malformed_uid_fail_closed(self) -> None:
        with self.assertRaises(RelayProtocolError):
            RelayClient("http://relay.example.test", "t" * 40)
        client = RelayClient("http://127.0.0.1:32150", "t" * 40)
        client._opener = SequenceOpener(  # type: ignore[assignment]
            {
                "schema": "contextbridge.adapter-presence-lease.v1",
                "adapter_uid": "adp_short",
                "enabled": True,
                "available": True,
                "heartbeat_after_seconds": 20,
            }
        )
        with self.assertRaises(RelayProtocolError):
            client.heartbeat(adapter_id="actions-primary", instance_id="host-actions")


if __name__ == "__main__":
    unittest.main()
