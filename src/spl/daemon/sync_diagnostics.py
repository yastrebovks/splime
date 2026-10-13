"""Closed, non-sensitive diagnostics for outbound synchronization failures."""

from __future__ import annotations

import re
from typing import Any

MAX_DIAGNOSTIC_BYTE_COUNT = (1 << 63) - 1


class SyncPayloadError(ValueError):
    """A local payload failure, independent of server connectivity."""

    def __init__(
        self,
        code: str,
        *,
        operation: str,
        identifier: str | None = None,
        actual_bytes: int | None = None,
        limit_bytes: int | None = None,
        next_action: str = "correct_payload_then_retry",
    ) -> None:
        self.diagnostic = {
            "code": code,
            "operation": operation,
            "identifier": identifier,
            "actual_bytes": actual_bytes,
            "limit_bytes": limit_bytes,
            "retryable": False,
            "next_action": next_action,
            "category": (
                "unsupported_capability"
                if "capability_required" in code
                else "oversized_payload"
                if "too_large" in code
                else "invalid_payload"
            ),
        }
        super().__init__(f"{operation}: {code} ({actual_bytes} bytes; limit {limit_bytes} bytes)")


def sync_failure(exc: BaseException, *, operation: str, identifier: str | None = None) -> dict[str, Any]:
    """Classify failures without copying arbitrary exception bodies or source."""

    if isinstance(exc, SyncPayloadError):
        return exc.diagnostic
    status = getattr(exc, "status_code", None)
    status = int(status) if isinstance(status, int) and not isinstance(status, bool) and 100 <= status <= 599 else None
    code = getattr(exc, "code", None)
    payload = getattr(exc, "payload", None)
    payload = payload if isinstance(payload, dict) else {}
    if not isinstance(code, str) or re.fullmatch(r"[a-zA-Z0-9_.:-]{1,128}", code) is None:
        code = None
    transport = getattr(exc, "transport_failure", None)
    if status == 401 or code == "authentication_failed":
        category, retryable, action = "authentication", False, "reconnect_with_valid_credentials"
    elif status == 403:
        category, retryable, action = "authorization", False, "check_operation_permissions"
    elif getattr(exc, "response_failure", False) is True:
        category, retryable, action = "invalid_response", True, "retry_with_backoff"
    elif status in {404, 405, 501}:
        category, retryable, action = "unsupported_capability", False, "upgrade_server"
    elif status == 413:
        category, retryable, action = "oversized_payload", False, "correct_payload_then_retry"
    elif transport is True or (transport is None and status in {502, 503, 504}):
        category, retryable, action = "connectivity", True, "retry_when_server_reachable"
    elif isinstance(status, int) and (status >= 500 or status in {408, 429}):
        category, retryable, action = "transient_server", True, "retry_with_backoff"
    else:
        category, retryable, action = "invalid_payload", False, "correct_payload_then_retry"
    # HTTP authentication and locally known transport/response failures are
    # authoritative. Optional upstream hints cannot contradict that evidence.
    authoritative = category in {"authentication", "authorization", "connectivity", "invalid_response"}
    result = {
        "code": code or f"sync_{category}",
        "operation": operation,
        "identifier": identifier,
        "category": category,
        "retryable": retryable,
        "next_action": action,
        "status_code": status,
    }
    for key in ("actual_bytes", "limit_bytes"):
        value = payload.get(key)
        if type(value) is int and 0 <= value <= MAX_DIAGNOSTIC_BYTE_COUNT:
            result[key] = value
    for key in ("event_id", "snapshot_id"):
        value = payload.get(key)
        if isinstance(value, str) and re.fullmatch(r"[a-zA-Z0-9_.:-]{1,256}", value):
            result[key] = value
            if result["identifier"] is None:
                result["identifier"] = value
    advertised_category = payload.get("category")
    if (
        not authoritative
        and isinstance(advertised_category, str)
        and advertised_category
        in {
            "authentication",
            "authorization",
            "unsupported_capability",
            "invalid_payload",
            "oversized_payload",
            "transient_server",
        }
    ):
        result["category"] = advertised_category
    advertised_action = payload.get("recommended_action")
    if (
        not authoritative
        and isinstance(advertised_action, str)
        and re.fullmatch(r"[a-zA-Z0-9_.:-]{1,128}", advertised_action)
    ):
        result["next_action"] = advertised_action
    if (
        not authoritative
        and type(payload.get("retryable")) is bool
        and result["category"] not in {"authentication", "authorization"}
    ):
        result["retryable"] = payload["retryable"]
    return result
