from __future__ import annotations

import hashlib
import json
import os
import secrets
import sqlite3
import stat
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .contracts import ActionEnvelope, ContractError, parse_staged_payload, validate_repository


class ActionStoreError(RuntimeError):
    """The durable action store rejected an operation."""


class AmbiguousActionError(ActionStoreError):
    """An earlier attempt may already have crossed the provider boundary."""


@dataclass(frozen=True)
class PreparedAction:
    repository: str
    payload: dict[str, Any]
    request_digest: str
    cached_receipt: dict[str, Any] | None = None


def _utc_text(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _scope(value: str, name: str, maximum: int, *, empty: bool = False) -> str:
    clean = value.strip()
    if (not clean and not empty) or len(clean.encode("utf-8")) > maximum or any(ord(char) < 32 for char in clean):
        qualifier = "0" if empty else "1"
        raise ActionStoreError(f"{name} must contain {qualifier}..{maximum} UTF-8 bytes without controls")
    return clean


class ActionStore:
    def __init__(self, path: Path) -> None:
        self.path = path.expanduser()
        self._prepare_path()
        self._initialize()

    def _prepare_path(self) -> None:
        parent = self.path.parent
        parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        try:
            parent.chmod(0o700)
        except OSError:
            pass
        if self.path.exists() or self.path.is_symlink():
            info = self.path.lstat()
            if self.path.is_symlink() or not stat.S_ISREG(info.st_mode):
                raise ActionStoreError("database path must be a regular non-symlink file")

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=5, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=5000")
        return connection

    def _initialize(self) -> None:
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=FULL")
            version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            if version not in {0, 1}:
                raise ActionStoreError("database schema version is unsupported")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS destinations (
                    destination_ref TEXT PRIMARY KEY,
                    owner_subject TEXT NOT NULL,
                    tenant_id TEXT NOT NULL,
                    provider TEXT NOT NULL CHECK (provider = 'github'),
                    repository TEXT NOT NULL,
                    active INTEGER NOT NULL CHECK (active IN (0, 1)),
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS destinations_scope
                    ON destinations(owner_subject, tenant_id, active);
                CREATE TABLE IF NOT EXISTS payloads (
                    payload_ref TEXT PRIMARY KEY,
                    owner_subject TEXT NOT NULL,
                    tenant_id TEXT NOT NULL,
                    destination_ref TEXT NOT NULL REFERENCES destinations(destination_ref),
                    action_kind TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    payload_sha256 TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS payloads_scope
                    ON payloads(owner_subject, tenant_id, destination_ref, action_kind);
                CREATE TABLE IF NOT EXISTS attempts (
                    schedule_id TEXT NOT NULL,
                    occurrence INTEGER NOT NULL,
                    owner_subject TEXT NOT NULL,
                    tenant_id TEXT NOT NULL,
                    destination_ref TEXT NOT NULL,
                    payload_ref TEXT NOT NULL,
                    action_kind TEXT NOT NULL,
                    request_digest TEXT NOT NULL,
                    state TEXT NOT NULL CHECK (state IN ('prepared', 'mutating', 'completed', 'failed', 'unknown')),
                    receipt_json TEXT,
                    failure_code TEXT,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (schedule_id, occurrence)
                );
                PRAGMA user_version=1;
                """
            )
        try:
            os.chmod(self.path, 0o600)
        except OSError:
            pass

    def add_github_destination(self, owner_subject: str, tenant_id: str, repository: str) -> str:
        owner = _scope(owner_subject, "owner_subject", 120)
        tenant = _scope(tenant_id, "tenant_id", 200, empty=True)
        try:
            repo = validate_repository(repository)
        except ContractError as exc:
            raise ActionStoreError(str(exc)) from exc
        reference = "dst_" + secrets.token_hex(16)
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "INSERT INTO destinations VALUES (?, ?, ?, 'github', ?, 1, ?)",
                (reference, owner, tenant, repo, _utc_text(datetime.now(timezone.utc))),
            )
            connection.commit()
        return reference

    def set_destination_active(
        self, reference: str, active: bool, owner_subject: str, tenant_id: str
    ) -> bool:
        owner = _scope(owner_subject, "owner_subject", 120)
        tenant = _scope(tenant_id, "tenant_id", 200, empty=True)
        with closing(self._connect()) as connection:
            result = connection.execute(
                """UPDATE destinations SET active = ?
                   WHERE destination_ref = ? AND owner_subject = ? AND tenant_id = ?""",
                (1 if active else 0, reference, owner, tenant),
            )
            return result.rowcount == 1

    def list_destinations(self, owner_subject: str, tenant_id: str) -> list[dict[str, Any]]:
        owner = _scope(owner_subject, "owner_subject", 120)
        tenant = _scope(tenant_id, "tenant_id", 200, empty=True)
        with closing(self._connect()) as connection:
            rows = connection.execute(
                """SELECT destination_ref, provider, repository, active, created_at
                   FROM destinations WHERE owner_subject = ? AND tenant_id = ?
                   ORDER BY created_at, destination_ref LIMIT 256""",
                (owner, tenant),
            ).fetchall()
        return [
            {
                "destination_ref": row["destination_ref"],
                "provider": row["provider"],
                "repository": row["repository"],
                "active": bool(row["active"]),
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def stage_payload(
        self,
        owner_subject: str,
        tenant_id: str,
        destination_ref: str,
        action_kind: str,
        raw_payload: str | bytes,
        *,
        ttl: timedelta,
    ) -> str:
        owner = _scope(owner_subject, "owner_subject", 120)
        tenant = _scope(tenant_id, "tenant_id", 200, empty=True)
        if ttl < timedelta(minutes=1) or ttl > timedelta(days=30):
            raise ActionStoreError("payload TTL must be between one minute and 30 days")
        try:
            payload = parse_staged_payload(raw_payload, action_kind)
        except ContractError as exc:
            raise ActionStoreError(str(exc)) from exc
        canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        reference = "ref_" + secrets.token_hex(16)
        now = datetime.now(timezone.utc)
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            destination = connection.execute(
                """SELECT 1 FROM destinations
                   WHERE destination_ref = ? AND owner_subject = ? AND tenant_id = ? AND active = 1""",
                (destination_ref, owner, tenant),
            ).fetchone()
            if destination is None:
                connection.rollback()
                raise ActionStoreError("destination is missing, inactive, or belongs to another scope")
            connection.execute(
                """INSERT INTO payloads
                   (payload_ref, owner_subject, tenant_id, destination_ref, action_kind,
                    payload_json, payload_sha256, created_at, expires_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    reference,
                    owner,
                    tenant,
                    destination_ref,
                    action_kind,
                    canonical,
                    digest,
                    _utc_text(now),
                    _utc_text(now + ttl),
                ),
            )
            connection.commit()
        return reference

    def prepare(self, envelope: ActionEnvelope, *, now: datetime | None = None) -> PreparedAction:
        instant = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT * FROM attempts WHERE schedule_id = ? AND occurrence = ?",
                (envelope.schedule_id, envelope.occurrence),
            ).fetchone()
            if existing is not None:
                self._verify_attempt_binding(existing, envelope)
                if existing["state"] == "completed":
                    receipt = json.loads(existing["receipt_json"])
                    connection.commit()
                    return PreparedAction("", {}, existing["request_digest"], receipt)
                if existing["state"] in {"mutating", "unknown"}:
                    connection.rollback()
                    raise AmbiguousActionError("the occurrence may already have mutated its provider")
                if existing["state"] == "failed":
                    connection.rollback()
                    raise ActionStoreError("the occurrence already failed definitively")
            destination = connection.execute(
                """SELECT repository FROM destinations
                   WHERE destination_ref = ? AND owner_subject = ? AND tenant_id = ?
                     AND provider = 'github' AND active = 1""",
                (envelope.destination_ref, envelope.owner_subject, envelope.tenant_id),
            ).fetchone()
            payload = connection.execute(
                """SELECT payload_json, payload_sha256, expires_at FROM payloads
                   WHERE payload_ref = ? AND owner_subject = ? AND tenant_id = ?
                     AND destination_ref = ? AND action_kind = ?""",
                (
                    envelope.payload_ref,
                    envelope.owner_subject,
                    envelope.tenant_id,
                    envelope.destination_ref,
                    envelope.action_kind,
                ),
            ).fetchone()
            if destination is None or payload is None:
                connection.rollback()
                raise ActionStoreError(
                    "staged action references are missing, inactive, or outside the authenticated scope"
                )
            expires_at = datetime.fromisoformat(payload["expires_at"].replace("Z", "+00:00"))
            if instant >= expires_at:
                connection.rollback()
                raise ActionStoreError("staged payload has expired")
            canonical = payload["payload_json"]
            parsed = parse_staged_payload(canonical, envelope.action_kind)
            digest_input = (
                destination["repository"] + "\x00" + envelope.action_kind + "\x00" + payload["payload_sha256"]
            )
            request_digest = hashlib.sha256(digest_input.encode()).hexdigest()
            if existing is None:
                connection.execute(
                    """INSERT INTO attempts
                       (schedule_id, occurrence, owner_subject, tenant_id, destination_ref, payload_ref,
                        action_kind, request_digest, state, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'prepared', ?)""",
                    (
                        envelope.schedule_id,
                        envelope.occurrence,
                        envelope.owner_subject,
                        envelope.tenant_id,
                        envelope.destination_ref,
                        envelope.payload_ref,
                        envelope.action_kind,
                        request_digest,
                        _utc_text(instant),
                    ),
                )
            elif existing["request_digest"] != request_digest:
                connection.rollback()
                raise ActionStoreError("prepared occurrence no longer matches its staged request")
            connection.commit()
            return PreparedAction(destination["repository"], parsed, request_digest)

    @staticmethod
    def _verify_attempt_binding(row: sqlite3.Row, envelope: ActionEnvelope) -> None:
        expected = (
            envelope.owner_subject,
            envelope.tenant_id,
            envelope.destination_ref,
            envelope.payload_ref,
            envelope.action_kind,
        )
        binding_names = ("owner_subject", "tenant_id", "destination_ref", "payload_ref", "action_kind")
        actual = tuple(row[name] for name in binding_names)
        if actual != expected:
            raise ActionStoreError("occurrence identity was reused with another action binding")

    def mark_mutating(self, envelope: ActionEnvelope, request_digest: str) -> None:
        self._transition(envelope, "prepared", "mutating", request_digest=request_digest)

    def mark_completed(self, envelope: ActionEnvelope, request_digest: str, receipt: dict[str, Any]) -> None:
        encoded = json.dumps(receipt, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        if len(encoded.encode("utf-8")) > 16 * 1024:
            raise ActionStoreError("provider receipt exceeds the durable limit")
        self._transition(envelope, "mutating", "completed", request_digest=request_digest, receipt=encoded)

    def mark_failed(self, envelope: ActionEnvelope, request_digest: str, code: str) -> None:
        self._transition(envelope, "mutating", "failed", request_digest=request_digest, failure=code[:80])

    def mark_unknown(self, envelope: ActionEnvelope, request_digest: str, code: str) -> None:
        self._transition(envelope, "mutating", "unknown", request_digest=request_digest, failure=code[:80])

    def _transition(
        self,
        envelope: ActionEnvelope,
        old: str,
        new: str,
        *,
        request_digest: str,
        receipt: str | None = None,
        failure: str | None = None,
    ) -> None:
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            result = connection.execute(
                """UPDATE attempts SET state = ?, receipt_json = ?, failure_code = ?, updated_at = ?
                   WHERE schedule_id = ? AND occurrence = ? AND state = ? AND request_digest = ?""",
                (
                    new,
                    receipt,
                    failure,
                    _utc_text(datetime.now(timezone.utc)),
                    envelope.schedule_id,
                    envelope.occurrence,
                    old,
                    request_digest,
                ),
            )
            if result.rowcount != 1:
                connection.rollback()
                raise ActionStoreError(f"occurrence cannot transition from {old} to {new}")
            connection.commit()

    def attempt_state(self, schedule_id: str, occurrence: int) -> str | None:
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT state FROM attempts WHERE schedule_id = ? AND occurrence = ?", (schedule_id, occurrence)
            ).fetchone()
        return None if row is None else str(row["state"])
