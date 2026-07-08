"""`spl-daemon doctor` diagnostics (Stage 2.3 / TODO #35).

Each check must classify its slice of the setup without raising, the report
must aggregate to a shell exit code, and the CLI must expose both the human
and the ``--json`` renderings.
"""

from __future__ import annotations

import json
import sys
import warnings
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from spl import lift
from spl.core.adapter_compat import AdapterCompatibilityWarning
from spl.core.entities.adapter import Adapter, make_key
from spl.core.ir.utils import spl_export_to_file
from spl.daemon import doctor as doctor_module
from spl.daemon.doctor import (
    FAIL,
    OK,
    SKIP,
    WARN,
    CheckResult,
    DoctorReport,
    check_daemon,
    check_daemon_home,
    check_disk_space,
    check_environment_builds,
    check_interpreter_substitutions,
    check_pipeline_adapter_probe,
    check_pipeline_adapter_tags,
    check_python,
    check_server_connection,
    check_uv_builder,
    check_venv_tooling,
    run_doctor,
)
from spl.daemon.store import RegistryStore


def _doctor_make_text() -> str:
    return "hello"


def _doctor_consume_text(value: str) -> str:
    return value


def _doctor_make_number() -> int:
    return 7


def _doctor_consume_number(value: int) -> int:
    return value


def _doctor_save_text(path: str, obj: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(obj)


def _doctor_load_text(path: str) -> str:
    with open(path, encoding="utf-8") as f:
        return f.read()


def _doctor_save_should_not_run(path: str, obj: str) -> None:
    del path, obj
    raise AssertionError("static adapter tag checks must not call save")


def _doctor_load_should_not_run(path: str) -> str:
    del path
    raise AssertionError("static adapter tag checks must not call load")


class MismatchedRuntimeAdapter(Adapter):
    @property
    def accepted_tags(self) -> frozenset[str]:
        """Return a deliberately incompatible accepted tag set."""

        return frozenset({"tsv"})


def _mismatched_pipeline() -> Any:
    producer = lift(_doctor_make_text).alias("producer")
    pipeline = lift(_doctor_consume_text).bind(value=producer.as_format("csv")).alias("consumer").render()
    adapter = MismatchedRuntimeAdapter(
        key=make_key(str, "csv"),
        save=_doctor_save_should_not_run,
        load=_doctor_load_should_not_run,
        py_type=str,
        format="csv",
    )
    return replace(pipeline, adapters={adapter.key: adapter})


def _matching_pipeline() -> Any:
    producer = lift(_doctor_make_text).alias("producer")
    pipeline = lift(_doctor_consume_text).bind(value=producer.as_format("csv")).alias("consumer").render()
    return pipeline.add_adapter(str, "csv", save=_doctor_save_text, load=_doctor_load_text)


def _json_probe_pipeline() -> Any:
    producer = lift(_doctor_make_number).alias("producer")
    return lift(_doctor_consume_number).bind(value=producer).alias("consumer").render("json_probe_pipeline")


MISMATCH_PIPELINE_YAML = """
- !DPipeline
  name: mismatch_pipeline
  nodes:
  - !DNodeFunction
    uuid: 00000000-0000-0000-0000-000000000001
    func: make_text
  - !DNodeFunction
    uuid: 00000000-0000-0000-0000-000000000002
    func: consume_text
  links:
  - - !DNodeInputRef
      uuid: 00000000-0000-0000-0000-000000000002
      port: value
    - !DFormattedOutputRef
      uuid: 00000000-0000-0000-0000-000000000001
      port: default
      format: csv
  aliases:
  - [producer, 00000000-0000-0000-0000-000000000001]
  - [consumer, 00000000-0000-0000-0000-000000000002]
  adapters:
  - !DSaveAdapter
    key: builtins.str@csv
    tag: csv
    save: save_csv
    distributions: []
  - !DLoadAdapter
    key: builtins.str@csv
    accepted_tags: [tsv]
    load: load_tsv
    distributions: []
- !DFunction
  name: make_text
  inputs: []
  outputs:
  - name: default
    type: str
  body: |-
    return "hello"
- !DFunction
  name: consume_text
  inputs:
  - name: value
    type: str
    default: null
  outputs:
  - name: default
    type: str
  body: |-
    return value
- !DFunction
  name: save_csv
  inputs: []
  outputs: []
  body: |-
    pass
- !DFunction
  name: load_tsv
  inputs: []
  outputs:
  - name: default
    type: str
  body: |-
    return ""
"""


class FakeClient:
    base_url = "http://127.0.0.1:8765"

    def __init__(self, health: dict[str, Any] | None = None):
        self._health = health

    def health(self) -> dict[str, Any]:
        if self._health is None:
            raise RuntimeError("local SPL daemon is not reachable")
        return self._health


@pytest.fixture(autouse=True)
def no_docker_probe(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep doctor tests hermetic: never shell out to a real `docker info`."""

    monkeypatch.setattr(doctor_module.shutil, "which", lambda name: None)


def _healthy_payload(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "ok": True,
        "counts": {"objects": 3, "runs": 7, "environment_builds": 2},
        "server": {"connected": False, "offline": False, "connection": None},
        "environment_builds": {"by_status": {"ready": 2}},
        "interpreter_substitutions": {"items": [], "count": 0, "minor_mismatches": 0},
    }
    payload.update(overrides)
    return payload


class TestIndividualChecks:
    def test_python_check_reports_interpreter(self) -> None:
        result = check_python()
        assert result.status == OK
        assert "Python" in result.detail

    def test_venv_tooling_present(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            doctor_module.importlib.util,
            "find_spec",
            lambda module: object(),
        )
        assert check_venv_tooling().status == OK

    def test_venv_tooling_missing_is_fail(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(
            doctor_module.importlib.util,
            "find_spec",
            lambda module: None if module == "ensurepip" else object(),
        )
        result = check_venv_tooling()
        assert result.status == FAIL
        assert "ensurepip" in result.detail
        assert "python3-venv" in (result.hint or "")

    def test_uv_builder_missing_is_warn(self) -> None:
        result = check_uv_builder()
        assert result.status == WARN
        assert "uv not found" in result.detail
        assert result.hint is not None

    def test_uv_builder_available_is_ok(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(doctor_module.shutil, "which", lambda name: "/usr/bin/uv")
        result = check_uv_builder()
        assert result.status == OK
        assert "/usr/bin/uv" in result.detail

    def test_daemon_home_missing_is_warn_with_hint(self, tmp_path: Path) -> None:
        result = check_daemon_home(tmp_path / "absent")
        assert result.status == WARN
        assert result.hint is not None

    def test_daemon_home_file_is_fail(self, tmp_path: Path) -> None:
        not_a_dir = tmp_path / "file"
        not_a_dir.touch()
        assert check_daemon_home(not_a_dir).status == FAIL

    def test_daemon_home_writable_dir_is_ok(self, tmp_path: Path) -> None:
        result = check_daemon_home(tmp_path)
        assert result.status == OK
        assert result.detail == str(tmp_path)

    def test_disk_space_walks_to_existing_ancestor(self, tmp_path: Path) -> None:
        result = check_disk_space(tmp_path / "not" / "created" / "yet")
        assert result.status in {OK, WARN, FAIL}
        assert "free at" in result.detail

    def test_disk_space_thresholds(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        class Usage:
            def __init__(self, free: int):
                self.free = free

        monkeypatch.setattr(
            doctor_module.shutil,
            "disk_usage",
            lambda path: Usage(free=doctor_module.DISK_FAIL_BYTES // 2),
        )
        assert check_disk_space(tmp_path).status == FAIL

        monkeypatch.setattr(
            doctor_module.shutil,
            "disk_usage",
            lambda path: Usage(free=doctor_module.DISK_WARN_BYTES - 1),
        )
        assert check_disk_space(tmp_path).status == WARN

        monkeypatch.setattr(
            doctor_module.shutil,
            "disk_usage",
            lambda path: Usage(free=doctor_module.DISK_WARN_BYTES * 10),
        )
        assert check_disk_space(tmp_path).status == OK

    def test_daemon_check_ok_and_payload_passthrough(self) -> None:
        client = FakeClient(_healthy_payload())
        result, health = check_daemon(client)
        assert result.status == OK
        assert "3 objects" in result.detail
        assert health is not None

    def test_daemon_check_failure_has_serve_hint(self) -> None:
        result, health = check_daemon(FakeClient(None))
        assert result.status == FAIL
        assert health is None
        assert "serve" in (result.hint or "")

    def test_server_connection_states(self) -> None:
        assert check_server_connection(None).status == SKIP

        local_only = check_server_connection(_healthy_payload())
        assert local_only.status == OK
        assert "local-only" in local_only.detail

        connected = check_server_connection(
            _healthy_payload(
                server={
                    "connected": True,
                    "offline": False,
                    "connection": {"server_url": "https://splime.io/api"},
                }
            )
        )
        assert connected.status == OK
        assert "https://splime.io/api" in connected.detail

        offline = check_server_connection(
            _healthy_payload(
                server={
                    "connected": False,
                    "offline": True,
                    "connection": {
                        "server_url": "https://splime.io/api",
                        "error": "401: bad token",
                    },
                }
            )
        )
        assert offline.status == FAIL
        assert "401: bad token" in offline.detail

    def test_environment_builds_states(self) -> None:
        assert check_environment_builds(None).status == SKIP

        healthy = check_environment_builds(_healthy_payload())
        assert healthy.status == OK

        empty = check_environment_builds(_healthy_payload(environment_builds={"by_status": {}}))
        assert empty.status == OK
        assert "no cached builds" in empty.detail

        failed = check_environment_builds(_healthy_payload(environment_builds={"by_status": {"ready": 1, "failed": 2}}))
        assert failed.status == WARN
        assert "2 of 3" in failed.detail
        assert "env-build-rebuild" in (failed.hint or "")

    def test_interpreter_substitution_states(self) -> None:
        assert check_interpreter_substitutions(None).status == SKIP

        healthy = check_interpreter_substitutions(_healthy_payload())
        assert healthy.status == OK

        same_minor = check_interpreter_substitutions(
            _healthy_payload(
                interpreter_substitutions={
                    "items": [
                        {
                            "object": "demo_obj",
                            "authored_python_version": "Python 3.13.0",
                            "resolved_python_version": "Python 3.13.2",
                        }
                    ],
                    "count": 1,
                    "minor_mismatches": 0,
                }
            )
        )
        assert same_minor.status == OK

        mismatch = check_interpreter_substitutions(
            _healthy_payload(
                interpreter_substitutions={
                    "items": [
                        {
                            "object": "demo_obj",
                            "authored_python_version": "Python 3.11.9",
                            "resolved_python_version": "Python 3.13.0",
                        }
                    ],
                    "count": 1,
                    "minor_mismatches": 1,
                }
            )
        )
        assert mismatch.status == WARN
        assert "demo_obj" in mismatch.detail
        assert "matching local env" in (mismatch.hint or "")

    def test_pipeline_build_warns_once_for_adapter_tag_mismatch(self) -> None:
        pipeline = _mismatched_pipeline()

        with warnings.catch_warnings(record=True) as captured:
            warnings.simplefilter("always", AdapterCompatibilityWarning)
            pipeline._validate_consistency()
            pipeline._validate_consistency()

        adapter_warnings = [item for item in captured if issubclass(item.category, AdapterCompatibilityWarning)]
        assert len(adapter_warnings) == 1
        assert "producer.default -> consumer.value" in str(adapter_warnings[0].message)
        assert "save tag `csv`" in str(adapter_warnings[0].message)
        assert "accepted tags: tsv" in str(adapter_warnings[0].message)
        assert ".as_format()" in str(adapter_warnings[0].message)

    def test_pipeline_build_does_not_warn_for_matching_adapter_tags(self) -> None:
        pipeline = _matching_pipeline()

        with warnings.catch_warnings(record=True) as captured:
            warnings.simplefilter("always", AdapterCompatibilityWarning)
            pipeline._validate_consistency()

        assert [item for item in captured if issubclass(item.category, AdapterCompatibilityWarning)] == []

    def test_pipeline_adapter_tag_check_reports_warning_and_ok(self) -> None:
        mismatch = check_pipeline_adapter_tags(_mismatched_pipeline())
        compatible = check_pipeline_adapter_tags(_matching_pipeline())

        assert mismatch.status == WARN
        assert "producer.default -> consumer.value" in mismatch.detail
        assert ".as_format()" in (mismatch.hint or "")
        assert compatible.status == OK

    def test_pipeline_adapter_probe_runs_builtin_json_example(self) -> None:
        result = check_pipeline_adapter_probe(_json_probe_pipeline())

        assert result.status == OK
        assert "1 adapter example probe" in result.detail

    def test_object_registration_warns_for_serialized_adapter_tag_mismatch(self, tmp_path: Path) -> None:
        store = RegistryStore(tmp_path)
        store.register_env("default", sys.executable)

        with pytest.warns(AdapterCompatibilityWarning, match="producer.default -> consumer.value"):
            store.register_object("mismatch_pipeline", "mismatch_pipeline", "default", yaml_text=MISMATCH_PIPELINE_YAML)


class TestDockerCheck:
    def test_not_installed_is_ok(self) -> None:
        result = doctor_module.check_docker()
        assert result.status == OK
        assert "not installed" in result.detail

    def test_unreachable_docker_daemon_is_warn(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(doctor_module.shutil, "which", lambda name: "/usr/bin/docker")

        class Completed:
            returncode = 1
            stderr = "Cannot connect to the Docker daemon\nmore detail"

        monkeypatch.setattr(
            doctor_module.subprocess,
            "run",
            lambda *args, **kwargs: Completed(),
        )
        result = doctor_module.check_docker()
        assert result.status == WARN
        assert "Cannot connect" in result.detail

    def test_available_docker_is_ok(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(doctor_module.shutil, "which", lambda name: "/usr/bin/docker")

        class Completed:
            returncode = 0
            stderr = ""

        monkeypatch.setattr(
            doctor_module.subprocess,
            "run",
            lambda *args, **kwargs: Completed(),
        )
        result = doctor_module.check_docker()
        assert result.status == OK
        assert "/usr/bin/docker" in result.detail

    def test_docker_probe_timeout_is_warn(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(doctor_module.shutil, "which", lambda name: "/usr/bin/docker")

        def raise_timeout(*args: Any, **kwargs: Any) -> None:
            raise doctor_module.subprocess.TimeoutExpired(cmd="docker", timeout=10)

        monkeypatch.setattr(doctor_module.subprocess, "run", raise_timeout)
        result = doctor_module.check_docker()
        assert result.status == WARN
        assert result.hint is not None


class TestReport:
    def test_exit_code_zero_without_failures(self) -> None:
        report = DoctorReport(
            checks=[
                CheckResult(name="a", status=OK, detail="fine"),
                CheckResult(name="b", status=WARN, detail="meh"),
                CheckResult(name="c", status=SKIP, detail="skipped"),
            ]
        )
        assert report.exit_code == 0

    def test_exit_code_one_with_failure(self) -> None:
        report = DoctorReport(checks=[CheckResult(name="a", status=FAIL, detail="broken", hint="fix")])
        assert report.exit_code == 1

    def test_render_lists_checks_hints_and_summary(self) -> None:
        report = DoctorReport(
            checks=[
                CheckResult(name="daemon", status=FAIL, detail="down", hint="serve"),
                CheckResult(name="python", status=OK, detail="Python 3.13"),
            ]
        )
        rendered = report.render()
        assert "daemon" in rendered
        assert "hint: serve" in rendered
        assert "1 ok, 0 warnings, 1 failures, 0 skipped" in rendered

    def test_payload_is_json_friendly(self) -> None:
        report = run_doctor(FakeClient(_healthy_payload()))
        payload = report.to_payload()
        json.dumps(payload)
        assert {check["name"] for check in payload["checks"]} >= {
            "python",
            "venv tooling",
            "uv builder",
            "daemon home",
            "disk space",
            "daemon",
            "server connection",
            "environment builds",
            "interpreter versions",
            "docker",
        }


class TestRunDoctor:
    def test_unreachable_daemon_skips_dependent_checks(self, tmp_path: Path) -> None:
        report = run_doctor(FakeClient(None), home=tmp_path)
        by_name = {check.name: check for check in report.checks}
        assert by_name["daemon"].status == FAIL
        assert by_name["server connection"].status == SKIP
        assert by_name["environment builds"].status == SKIP
        assert report.exit_code == 1

    def test_healthy_daemon_reports_ok(self, tmp_path: Path) -> None:
        report = run_doctor(FakeClient(_healthy_payload()), home=tmp_path)
        by_name = {check.name: check for check in report.checks}
        assert by_name["daemon"].status == OK
        assert by_name["server connection"].status == OK
        assert by_name["environment builds"].status == OK

    def test_never_raises_even_for_hostile_health_payload(
        self,
        tmp_path: Path,
    ) -> None:
        report = run_doctor(FakeClient({"counts": None, "server": None}), home=tmp_path)
        assert isinstance(report, DoctorReport)
        assert len(report.checks) == 10


class TestCli:
    def test_doctor_json_output_and_exit_code(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        import spl.daemon.cli as cli_module

        monkeypatch.setattr(
            cli_module,
            "Client",
            lambda url=None: FakeClient(_healthy_payload()),
        )
        exit_code = cli_module.main(["doctor", "--home", str(tmp_path), "--json"])
        payload = json.loads(capsys.readouterr().out)
        assert exit_code == 0
        assert payload["ok"] is True

    def test_doctor_pipeline_flag_runs_adapter_probe(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        import spl.daemon.cli as cli_module

        yaml_path = tmp_path / "json-probe.yaml"
        spl_export_to_file(yaml_path, [_json_probe_pipeline()])
        monkeypatch.setattr(
            cli_module,
            "Client",
            lambda url=None: FakeClient(_healthy_payload()),
        )

        exit_code = cli_module.main(["doctor", "--home", str(tmp_path), "--pipeline", str(yaml_path), "--json"])
        payload = json.loads(capsys.readouterr().out)
        checks = {check["name"]: check for check in payload["checks"]}

        assert exit_code == 0
        assert checks["adapter tags"]["status"] == OK
        assert checks["adapter probe"]["status"] == OK

    def test_doctor_human_output_reports_failure_exit_code(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        import spl.daemon.cli as cli_module

        monkeypatch.setattr(
            cli_module,
            "Client",
            lambda url=None: FakeClient(None),
        )
        exit_code = cli_module.main(["doctor", "--home", str(tmp_path)])
        output = capsys.readouterr().out
        assert exit_code == 1
        assert "daemon" in output
        assert "failures" in output
