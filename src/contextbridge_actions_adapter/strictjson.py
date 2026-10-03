from __future__ import annotations

import json
from typing import Any


class StrictJSONError(ValueError):
    """Raised when JSON is invalid or ambiguous."""


def _object_without_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    folded: set[str] = set()
    for key, value in pairs:
        normalized = key.casefold()
        if normalized in folded:
            raise StrictJSONError(f"duplicate or case-ambiguous property: {key}")
        folded.add(normalized)
        result[key] = value
    return result


def loads(raw: str | bytes, *, max_bytes: int) -> Any:
    encoded = raw.encode("utf-8") if isinstance(raw, str) else raw
    if not encoded or len(encoded) > max_bytes:
        raise StrictJSONError(f"JSON must contain 1..{max_bytes} UTF-8 bytes")
    try:
        text = encoded.decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        raise StrictJSONError("JSON is not valid UTF-8") from exc
    try:
        return json.loads(
            text,
            object_pairs_hook=_object_without_duplicates,
            parse_constant=lambda value: (_ for _ in ()).throw(StrictJSONError(f"invalid number: {value}")),
        )
    except StrictJSONError:
        raise
    except (json.JSONDecodeError, TypeError, ValueError) as exc:
        raise StrictJSONError("invalid JSON") from exc
