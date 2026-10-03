from __future__ import annotations

import json
import ssl
from dataclasses import dataclass
from typing import IO, Any
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlsplit, urlunsplit
from urllib.request import HTTPRedirectHandler, HTTPSHandler, Request, build_opener

from .strictjson import StrictJSONError, loads

MAX_RELAY_RESPONSE_BYTES = 256 * 1024


class RelayProtocolError(RuntimeError):
    """The ContextBridge relay rejected or violated the scoped API contract."""


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(
        self,
        req: Request,
        fp: IO[bytes],
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> None:
        return None


@dataclass(frozen=True)
class RelayIdentity:
    subject: str
    allowed_tenants: tuple[str, ...]
    scheduled_actions: bool


@dataclass(frozen=True)
class PresenceLease:
    adapter_uid: str
    enabled: bool
    available: bool
    heartbeat_after_seconds: int


def _origin(raw: str) -> str:
    try:
        parsed = urlsplit(raw)
    except ValueError as exc:
        raise RelayProtocolError("relay URL is invalid") from exc
    loopback = parsed.hostname in {"localhost", "127.0.0.1", "::1"}
    if parsed.scheme not in {"http", "https"} or (parsed.scheme == "http" and not loopback):
        raise RelayProtocolError("relay URL must use HTTPS or loopback HTTP")
    if not parsed.hostname or parsed.username is not None or parsed.password is not None:
        raise RelayProtocolError("relay URL must be a credential-free origin")
    if parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
        raise RelayProtocolError("relay URL must not contain a path, query, or fragment")
    return urlunsplit((parsed.scheme, parsed.netloc, "", "", ""))


def _bounded(response: Any) -> bytes:
    chunks: list[bytes] = []
    total = 0
    while True:
        chunk = response.read(min(64 * 1024, MAX_RELAY_RESPONSE_BYTES + 1 - total))
        if not chunk:
            break
        total += len(chunk)
        if total > MAX_RELAY_RESPONSE_BYTES:
            raise RelayProtocolError("relay response exceeds the byte limit")
        chunks.append(chunk)
    return b"".join(chunks)


class RelayClient:
    def __init__(self, base_url: str, token: str, *, timeout_seconds: int = 20) -> None:
        self.base_url = _origin(base_url)
        clean = token.strip()
        if not 32 <= len(clean) <= 4_096 or any(character in clean for character in "\r\n\x00"):
            raise RelayProtocolError("producer credential must contain one 32-4096 character secret")
        self.token = clean
        self.timeout_seconds = timeout_seconds
        self._opener = build_opener(_NoRedirect(), HTTPSHandler(context=ssl.create_default_context()))

    def _request(self, path: str, *, method: str = "GET", body: Any = None) -> Any:
        encoded = None if body is None else json.dumps(body, separators=(",", ":")).encode("utf-8")
        request = Request(  # noqa: S310  # nosec B310
            urljoin(self.base_url + "/", path.lstrip("/")),
            data=encoded,
            method=method,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Accept": "application/json",
                **({"Content-Type": "application/json"} if encoded is not None else {}),
            },
        )
        try:
            with self._opener.open(request, timeout=self.timeout_seconds) as response:
                raw = _bounded(response)
        except HTTPError as exc:
            raise RelayProtocolError(f"relay rejected the request (HTTP {exc.code})") from exc
        except (OSError, URLError) as exc:
            raise RelayProtocolError("relay connection failed") from exc
        try:
            return loads(raw, max_bytes=MAX_RELAY_RESPONSE_BYTES)
        except StrictJSONError as exc:
            raise RelayProtocolError("relay returned invalid JSON") from exc

    def whoami(self) -> RelayIdentity:
        value = self._request("v1/cluster/whoami")
        if not isinstance(value, dict) or value.get("schema") != "contextbridge.identity.v1":
            raise RelayProtocolError("relay identity response is malformed")
        if value.get("role") != "producer":
            raise RelayProtocolError("adapter presence requires a producer credential")
        subject = value.get("subject")
        permissions = value.get("permissions")
        producer = value.get("producer_limits", {})
        if (
            not isinstance(subject, str)
            or not subject
            or not isinstance(permissions, list)
            or not isinstance(producer, dict)
        ):
            raise RelayProtocolError("relay identity response omitted its producer scope")
        tenants = producer.get("allowed_tenants", [])
        if not isinstance(tenants, list) or any(not isinstance(item, str) for item in tenants):
            raise RelayProtocolError("relay identity tenant scope is malformed")
        return RelayIdentity(subject, tuple(tenants), "scheduled-actions:write-own" in permissions)

    def heartbeat(
        self,
        *,
        adapter_id: str,
        instance_id: str,
        state: str = "ready",
        active: int = 0,
        last_error_code: str = "",
    ) -> PresenceLease:
        body: dict[str, Any] = {
            "schema": "contextbridge.adapter-presence.v1",
            "adapter_id": adapter_id,
            "instance_id": instance_id,
            "display_name": "ContextBridge Actions",
            "kind": "control",
            "version": "0.1.0",
            "state": state,
            "capabilities": ["scheduled-action"],
            "active": active,
            "capacity": 1,
            "lease_seconds": 60,
        }
        if last_error_code:
            body["last_error_code"] = last_error_code
        value = self._request("v1/cluster/adapters/heartbeat", method="POST", body=body)
        if not isinstance(value, dict) or value.get("schema") != "contextbridge.adapter-presence-lease.v1":
            raise RelayProtocolError("relay presence lease is malformed")
        uid = value.get("adapter_uid")
        enabled = value.get("enabled")
        available = value.get("available")
        after = value.get("heartbeat_after_seconds")
        valid_uid = isinstance(uid, str) and len(uid) == 36 and uid.startswith("adp_")
        if (
            not valid_uid
            or not isinstance(enabled, bool)
            or not isinstance(available, bool)
            or isinstance(after, bool)
            or not isinstance(after, int)
            or not 5 <= after <= 100
        ):
            raise RelayProtocolError("relay presence lease fields are invalid")
        return PresenceLease(str(uid), enabled, available, after)
