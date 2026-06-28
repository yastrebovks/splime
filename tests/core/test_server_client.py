from __future__ import annotations

from pathlib import Path
from typing import Any

from spl.server_client import SPLExternalTokenClient, SPLServerClient


class RecordingServerClient(SPLServerClient):
    def __init__(self) -> None:
        super().__init__("external-token", base_url="http://server.local")
        self.requests: list[tuple[str, str, dict[str, Any] | None]] = []
        self.byte_paths: list[str] = []

    def _json_request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
    ) -> Any:
        self.requests.append((method, path, payload))
        if method == "POST" and path == "/remote-runs":
            return {"id": "run-1", "status": "queued"}
        if method == "GET" and path == "/remote-runs/run-1":
            return {"id": "run-1", "status": "succeeded", "result": {"value": 7}}
        if method == "GET" and path == "/remote-runs/run-1/detail":
            return {
                "run": {"id": "run-1", "status": "succeeded"},
                "result": {"value": {"score": 0.91}, "artifacts": []},
                "artifacts": [],
            }
        if method == "GET" and path == "/remote-runs/run-1/artifacts":
            return [{"name": "score.json"}]
        return []

    def _bytes_request(self, path: str) -> bytes:
        self.byte_paths.append(path)
        return b'{"score": 0.91}'


def test_server_client_uses_bearer_token_headers() -> None:
    client = RecordingServerClient()

    assert client._headers() == {
        "Accept": "application/json",
        "Authorization": "Bearer external-token",
    }


def test_server_client_signature_uses_owner_library_path() -> None:
    client = RecordingServerClient()

    client.signature("fraud_score", owner="alice", library="risk")

    assert client.requests[-1] == (
        "GET",
        "/owners/alice/libraries/risk/objects/fraud_score/signature",
        None,
    )


def test_server_client_signature_can_select_internal_function() -> None:
    client = RecordingServerClient()

    client.signature("demo_pipeline", library="risk", function="inner_add")

    assert client.requests[-1] == (
        "GET",
        "/objects/demo_pipeline/signature?function=inner_add&library=risk",
        None,
    )


def test_server_client_library_token_paths_without_owner() -> None:
    client = RecordingServerClient()

    client.objects(library="risk", compact=True)
    client.decomposition("fraud_score", library="risk", version=3)

    assert client.requests[0] == ("GET", "/objects?library=risk&view=summary", None)
    assert client.requests[1] == (
        "GET",
        "/objects/fraud_score/decomposition?version=3&library=risk",
        None,
    )


def test_server_client_decomposition_uses_owner_library_path() -> None:
    client = RecordingServerClient()

    client.decomposition("fraud_score", owner="alice", library="risk", version=3)

    assert client.requests[-1] == (
        "GET",
        "/owners/alice/libraries/risk/objects/fraud_score/decomposition?version=3",
        None,
    )


def test_server_client_start_sends_remote_run_payload() -> None:
    client = RecordingServerClient()

    run = client.start(
        "fraud_score",
        target_machine="gpu-a",
        owner="alice",
        library="risk",
        kwargs={"customer_id": 42},
        output="score",
        function="score_customer",
        offline_policy="queue",
    )

    assert run.id == "run-1"
    assert client.requests[-1] == (
        "POST",
        "/remote-runs",
        {
            "object": "fraud_score",
            "target_machine_id": "gpu-a",
            "object_owner_id": "alice",
            "library": "risk",
            "kwargs": {"customer_id": 42},
            "output": "score",
            "function": "score_customer",
            "offline_policy": "queue",
        },
    )


def test_server_client_call_waits_and_returns_value() -> None:
    client = RecordingServerClient()

    result = client.call(
        "fraud_score",
        library="risk",
        kwargs={"customer_id": 42},
        poll_interval=0,
        wait_timeout_seconds=1,
    )

    assert result.value == {"score": 0.91}
    assert result.artifacts == []
    assert [request[:2] for request in client.requests] == [
        ("POST", "/remote-runs"),
        ("GET", "/remote-runs/run-1"),
        ("GET", "/remote-runs/run-1/detail"),
    ]


def test_external_token_facade_exposes_restricted_surface() -> None:
    external = SPLServerClient.external_token(
        "external-token",
        base_url="http://server.local",
    )
    assert isinstance(external, SPLExternalTokenClient)
    external._client = RecordingServerClient()

    signature = external.signature("fraud_score", library="risk")
    result = external.call(
        "fraud_score",
        library="risk",
        kwargs={"customer_id": 42},
        poll_interval=0,
        wait_timeout_seconds=1,
    )

    assert signature == []
    assert result.value == {"score": 0.91}
    assert [request[:2] for request in external._client.requests] == [
        ("GET", "/objects/fraud_score/signature?library=risk"),
        ("POST", "/remote-runs"),
        ("GET", "/remote-runs/run-1"),
        ("GET", "/remote-runs/run-1/detail"),
    ]
    assert not hasattr(external, "cancel_run")
    assert not hasattr(external, "retry_run")
    assert not hasattr(external, "objects")


def test_server_remote_run_artifact_helpers(tmp_path: Path) -> None:
    client = RecordingServerClient()
    run = client.start("fraud_score", library="risk")

    assert run.artifact_names() == ["score.json"]
    assert run.artifact_bytes("score.json") == b'{"score": 0.91}'
    downloaded = run.download_artifact("score.json", tmp_path)

    assert downloaded == tmp_path / "score.json"
    assert downloaded.read_bytes() == b'{"score": 0.91}'
    assert client.byte_paths == [
        "/remote-runs/run-1/artifacts/score.json",
        "/remote-runs/run-1/artifacts/score.json",
    ]
