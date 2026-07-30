from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from spl.daemon import worker
from spl.daemon.store import RegistryStore


FIXTURE = Path(__file__).parent / "fixtures" / "v0_4_5.sql"
V045_MIGRATIONS = (
    "20260702_object_identity_v1",
    "20260715_run_retention_v1",
    "20260715_run_retention_delivery_v2",
    "20260715_sync_event_telemetry_v1",
)
VERSION_HASHES = (
    (
        "fixture-local-version-1",
        1,
        "8c2e95f6fb45dd6e4963a590f574656a3612b55f07b32f8c12efc2d5f63f4c23",
        "1611bcdf5fbaca3a163e369a073b5134b140fd5346edfeb3dda33063c9a6c809",
    ),
    (
        "fixture-local-version-2",
        2,
        "ec8736dee59a6aaa5725cc9a7e3ff3b2f1a9b80a612e5353b9b4d4a98966abe1",
        "b18dff861a65823061c8e63592e9b686d5b39246712b19ead426a6dfc3f34ef8",
    ),
    (
        "fixture-pipeline-version",
        1,
        "02fd2479b4e4189d736bf5d08396b6da2c59fc00b7b5af5231bea81249bbbd3b",
        "708fc8889d660ad579d8c46b164d79fc803b06a29680ad26c3ac7d18af4204e6",
    ),
)


def _install_fixture(home: Path) -> Path:
    home.mkdir(parents=True, exist_ok=True)
    db_path = home / "daemon.sqlite3"
    with sqlite3.connect(db_path) as conn:
        conn.executescript(FIXTURE.read_text(encoding="utf-8"))
    return db_path


def _sqlite_backup(source_path: Path, target_path: Path) -> None:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    source_uri = f"{source_path.absolute().as_uri()}?mode=ro"
    with (
        sqlite3.connect(source_uri, uri=True) as source,
        sqlite3.connect(target_path) as target,
    ):
        source.backup(target)


def _integrity(conn: sqlite3.Connection) -> None:
    assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
    assert [str(row[0]) for row in conn.execute("PRAGMA integrity_check").fetchall()] == ["ok"]


def _identity_snapshot(conn: sqlite3.Connection) -> dict[str, tuple[tuple[object, ...], ...]]:
    statements = {
        "envs": """
            SELECT name, python, created_at, updated_at
            FROM envs ORDER BY name
        """,
        "objects": """
            SELECT id, owner_id, library, name, kind, origin, current_version_id
            FROM objects ORDER BY id
        """,
        "object_versions": """
            SELECT id, object_id, version, yaml_sha256, content_hash, env, env_python
            FROM object_versions ORDER BY object_id, version
        """,
        "environment_builds": """
            SELECT spec_hash, base_python, python_version, distributions_json,
                   runtime_packages_json, spec_json, status, builder, runtime_type
            FROM environment_builds ORDER BY spec_hash
        """,
        "runs": """
            SELECT id, object_id, object_version_id, object_name, object_version,
                   env, env_python, env_build_hash, status
            FROM runs ORDER BY id
        """,
        "sync_events": """
            SELECT id, kind, status, attempts, retryable, local_run_id, payload_json
            FROM sync_events ORDER BY id
        """,
    }
    return {name: tuple(tuple(row) for row in conn.execute(sql).fetchall()) for name, sql in statements.items()}


def test_v045_fixture_is_release_bound_and_credential_free(tmp_path: Path) -> None:
    fixture_text = FIXTURE.read_text(encoding="utf-8")
    assert "v0.4.5, commit 4a4231e959ec35776c2c874cf4fbb75c7b8864ae" in fixture_text
    for forbidden in (
        "/Users/",
        "fixture-connection-token",
        "Authorization: Bearer",
        "BEGIN PRIVATE KEY",
        "BEGIN OPENSSH PRIVATE KEY",
    ):
        assert forbidden not in fixture_text

    db_path = _install_fixture(tmp_path / "fixture")
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        _integrity(conn)
        assert conn.execute("PRAGMA user_version").fetchone() == (4,)
        assert tuple(
            row[0] for row in conn.execute("SELECT id FROM schema_migrations ORDER BY applied_at, id")
        ) == tuple(sorted(V045_MIGRATIONS))
        assert conn.execute("SELECT COUNT(*) FROM server_connections").fetchone() == (0,)
        assert conn.execute("SELECT COUNT(*) FROM remote_signatures").fetchone() == (0,)
        assert (
            tuple(
                tuple(row)
                for row in conn.execute(
                    """
                SELECT id, version, yaml_sha256, content_hash
                FROM object_versions
                ORDER BY object_id, version
                """
                )
            )
            == VERSION_HASHES
        )
        assert conn.execute("SELECT COUNT(*) FROM objects").fetchone() == (2,)
        assert conn.execute(
            """
            SELECT object_id, object_version_id, node_id, node_kind,
                   name, function_name
            FROM object_pipeline_nodes
            """
        ).fetchone() == (
            "fixture-pipeline-object",
            "fixture-pipeline-version",
            "00000000-0000-0000-0000-000000000001",
            "function",
            "fixture_step",
            "fixture_step",
        )
        assert tuple(
            tuple(row)
            for row in conn.execute(
                """
                SELECT id, status, object_id, object_version_id, returncode, keep
                FROM runs ORDER BY id
                """
            )
        ) == (
            (
                "fixture-failed-run",
                "failed",
                "fixture-pipeline-object",
                "fixture-pipeline-version",
                1,
                "true",
            ),
            (
                "fixture-local-run",
                "succeeded",
                "fixture-local-object",
                "fixture-local-version-1",
                0,
                "true",
            ),
        )


def test_v045_state_survives_bootstrap_reopen_backup_and_restore(
    tmp_path: Path,
) -> None:
    home = tmp_path / "daemon"
    db_path = _install_fixture(home)
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        baseline = _identity_snapshot(conn)

    backup_path = tmp_path / "backups" / "daemon.sqlite3.v0_4_5.bak"
    _sqlite_backup(db_path, backup_path)
    with sqlite3.connect(backup_path) as backup:
        backup.execute("PRAGMA foreign_keys = ON")
        _integrity(backup)
        assert backup.execute("PRAGMA user_version").fetchone() == (4,)
        assert _identity_snapshot(backup) == baseline

    with RegistryStore(home) as store:
        current = store.get_object(
            "fixture_function",
            owner_id="fixture-owner",
            library="default",
        )
        assert (
            current["id"],
            current["version_id"],
            current["version"],
            current["content_hash"],
        ) == (
            "fixture-local-object",
            "fixture-local-version-2",
            2,
            VERSION_HASHES[1][3],
        )
        pipeline = store.get_object(
            "fixture_pipeline",
            owner_id="fixture-owner",
            library="default",
        )
        assert (
            pipeline["id"],
            pipeline["version_id"],
            pipeline["kind"],
        ) == (
            "fixture-pipeline-object",
            "fixture-pipeline-version",
            "pipeline",
        )
        assert [(node["node_id"], node["kind"], node["function"]) for node in pipeline["pipeline_nodes"]] == [
            (
                "00000000-0000-0000-0000-000000000001",
                "function",
                "fixture_step",
            )
        ]
        assert [
            (item["version_id"], item["version"], item["content_hash"])
            for item in store.list_object_versions(
                "fixture-local-object",
                owner_id="fixture-owner",
                library="default",
            )
        ] == [
            ("fixture-local-version-2", 2, VERSION_HASHES[1][3]),
            ("fixture-local-version-1", 1, VERSION_HASHES[0][3]),
        ]
        run = store.get_run("fixture-local-run")
        assert (
            run["object_id"],
            run["object_version_id"],
            run["object_version"],
            run["env_build_hash"],
        ) == (
            "fixture-local-object",
            "fixture-local-version-1",
            1,
            "a" * 64,
        )
        assert run["status"] == "succeeded"
        failed = store.get_run("fixture-failed-run")
        assert (
            failed["status"],
            failed["object_id"],
            failed["object_version_id"],
            failed["returncode"],
            failed["error"],
        ) == (
            "failed",
            "fixture-pipeline-object",
            "fixture-pipeline-version",
            1,
            "synthetic retained failure",
        )
        build = store.get_environment_build("a" * 64)
        assert build is not None
        assert (build["spec_hash"], build["status"], build["builder"]) == (
            "a" * 64,
            "ready",
            "pip",
        )
        assert (
            store.get_sync_event("fixture-sync-pending")["status"],
            store.get_sync_event("fixture-sync-sent")["status"],
        ) == ("pending", "sent")
        assert store.get_sync_event("fixture-sync-sent")["local_run_id"] == ("fixture-local-run")
        store._conn.execute("PRAGMA foreign_keys = ON")
        _integrity(store._conn)
        first_bootstrap = _identity_snapshot(store._conn)
        assert first_bootstrap == baseline
        assert set(row[0] for row in store._conn.execute("SELECT id FROM schema_migrations")).issuperset(
            V045_MIGRATIONS
        )

    with RegistryStore(home) as reopened:
        reopened._conn.execute("PRAGMA foreign_keys = ON")
        _integrity(reopened._conn)
        assert _identity_snapshot(reopened._conn) == first_bootstrap

    restored_home = tmp_path / "restored"
    restored_db = restored_home / "daemon.sqlite3"
    _sqlite_backup(backup_path, restored_db)
    with RegistryStore(restored_home) as restored:
        restored._conn.execute("PRAGMA foreign_keys = ON")
        _integrity(restored._conn)
        assert _identity_snapshot(restored._conn) == first_bootstrap


def test_v045_object_can_be_described_and_executed_by_current_worker(
    tmp_path: Path,
) -> None:
    home = tmp_path / "daemon"
    _install_fixture(home)
    with RegistryStore(home) as store:
        described = store.get_object(
            "fixture_function",
            version=1,
            include_yaml=True,
            owner_id="fixture-owner",
            library="default",
        )
        assert (
            described["id"],
            described["version_id"],
            described["entrypoint"],
            described["content_hash"],
        ) == (
            "fixture-local-object",
            "fixture-local-version-1",
            "fixture_function",
            VERSION_HASHES[0][3],
        )

        execution_dir = tmp_path / "execution"
        object_yaml = execution_dir / "object.yaml"
        input_path = execution_dir / "input.json"
        result_path = execution_dir / "result.json"
        artifacts_dir = execution_dir / "artifacts"
        execution_dir.mkdir()
        object_yaml.write_text(described["yaml"], encoding="utf-8")
        input_path.write_text(
            json.dumps({"args": [], "kwargs": {}, "keep": True}),
            encoding="utf-8",
        )
        result = worker.execute(
            object_yaml=object_yaml,
            entrypoint=described["entrypoint"],
            input_path=input_path,
            result_path=result_path,
            artifacts_dir=artifacts_dir,
        )
        assert result == {"result": 1, "artifacts": {}}
        assert json.loads(result_path.read_text(encoding="utf-8")) == result
