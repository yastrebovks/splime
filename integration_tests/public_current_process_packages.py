"""Run against installed wheels, from outside the source repositories.

python public_current_process_packages.py produce /tmp/evidence
python public_current_process_packages.py consume /tmp/evidence

The producer uses a disposable local store and development signing key. It
never deploys or contacts a registry or package index. The consumer starts in a
fresh interpreter so worker import and NumPy state checks are meaningful.
"""

from __future__ import annotations

import hashlib
from importlib import metadata
import json
import logging
import os
from pathlib import Path
import sys


def produce(root: Path) -> None:
    from daemon_server.store import ServerStore
    import daemon_server
    import spl

    assert "site-packages" in str(spl.__file__), spl.__file__
    assert "site-packages" in str(daemon_server.__file__), daemon_server.__file__
    assert metadata.version("splime") == metadata.version("spl-server") == "0.4.11"
    root.mkdir(parents=True, exist_ok=True)
    store = ServerStore(root / "producer")
    store.create_user(user_id="author", display_name="Disposable publication fixture")
    store.claim_user_handle("author", "fixture")
    source = """- !DFunction
  name: compute
  inputs:
  - {name: start_df, type: null, default: null}
  - {name: copy_result, type: bool, default: 'False'}
  outputs:
  - {name: default, type: null}
  body: |-
    import os
    import numpy as np
    import pandas as pd
    assert isinstance(start_df, pd.DataFrame)
    if copy_result:
        start_df = start_df.copy(deep=True)
    start_df["sum"] = int(np.arange(5).sum())
    start_df.attrs["pid"] = os.getpid()
    start_df.attrs["numpy_version"] = np.__version__
    return start_df
- !DDistribution
  package: numpy
  version: '2.3.5'
  modules: [numpy]
- !DDistribution
  package: pandas
  version: '2.2.3'
  modules: [pandas]
"""
    try:
        store.publish_object(
            owner_id="author",
            name="compute",
            entrypoint="compute",
            yaml_text=source,
            description="Disposable in-process interoperability fixture",
            kind="function",
            metadata={"kind": "function", "entrypoint": "compute"},
            distributions=[
                {"package": "numpy", "version": "2.3.5", "modules": ["numpy"]},
                {"package": "pandas", "version": "2.2.3", "modules": ["pandas"]},
            ],
            runtime_config={"mode": "venv"},
            library="default",
            content_hash=hashlib.sha256(source.encode()).hexdigest(),
        )

        # Any resolver call is a test failure, including for a binary dependency.
        def forbidden(*args, **kwargs):
            raise AssertionError("user-managed publication attempted wheel resolution")

        store.publication_service._resolved_runtime_locks = forbidden
        result = store.publication_service.materialize_object(
            owner_id="author", name_or_id="compute", execution_profile="current-process-v1"
        )
        signature = store.publication_service._sign(result)
        key = signature.pop("public_key")
        envelope = {k: result[k] for k in ("manifest", "manifest_hash", "bundle_hash")}
        envelope["signature"] = signature
        (root / "manifest.json").write_text(json.dumps(envelope))
        (root / "bundle.zip").write_bytes(result["bundle_bytes"])
        (root / "key.json").write_text(json.dumps({signature["key_id"]: key}))
        print(
            json.dumps(
                {
                    "producer": str(daemon_server.__file__),
                    "framework": str(spl.__file__),
                    "manifest": result["manifest_hash"],
                    "profile": "current-process-v1",
                }
            )
        )
    finally:
        store.close()


def consume(root: Path) -> None:
    import numpy as np
    import pandas as pd
    from spl import SPLClient
    from spl.public import parse_public_ref
    import spl
    import subprocess
    from spl.daemon_client import Client

    assert "site-packages" in str(spl.__file__), spl.__file__
    assert "spl.daemon.worker" not in sys.modules
    assert metadata.version("splime") == metadata.version("spl-server") == "0.4.11"
    envelope = json.loads((root / "manifest.json").read_text())
    registry = "https://fixture.example.test"
    client = SPLClient.embedded(
        registry,
        root / "consumer",
        trusted_keys={registry: json.loads((root / "key.json").read_text())},
        run_receipts=False,
    )
    backend = client._embedded_backend
    backend._install_bundle(envelope, (root / "bundle.zip").read_bytes())
    ref = "splime://@fixture/default/compute@1"
    path = backend._resolution_path(parse_public_ref(ref))
    path.parent.mkdir(parents=True, exist_ok=True)
    manifest = envelope["manifest"]
    path.write_text(
        json.dumps(
            {
                "reference": ref,
                "registry_url": registry,
                "release": {
                    "id": envelope["manifest_hash"],
                    "manifest_hash": envelope["manifest_hash"],
                    "bundle_hash": envelope["bundle_hash"],
                    "version": manifest["version"],
                    "version_id": manifest["version_id"],
                },
            }
        )
    )
    events = []
    backend._emit_run_receipt = lambda *a, **k: events.append(k["state"])

    def forbidden(*args, **kwargs):
        raise AssertionError("attempted worker/daemon/installer/environment operation")

    subprocess.Popen = forbidden
    Client.__init__ = forbidden
    for name in ("_execute", "_locked_environments", "_ensure_environment", "_materialize_wheels"):
        setattr(backend, name, forbidden)
    messages = []

    class Capture(logging.Handler):
        def emit(self, record):
            messages.append(record.getMessage())

    handler = Capture()
    logger = logging.getLogger("spl.public_dependencies")
    logger.addHandler(handler)
    before = (
        np,
        np.__version__,
        np.__file__,
        pd,
        pd.__version__,
        pd.__file__,
        list(sys.path),
        os.getcwd(),
        dict(os.environ),
        sys.stdout,
        sys.stderr,
    )
    index = pd.MultiIndex.from_tuples([("a", 1), ("a", 2), ("b", 1)], names=["group", "row"])
    frame = pd.DataFrame(
        {
            "nullable": pd.array([1, pd.NA, 3], dtype="Int64"),
            "category": pd.Categorical(["private-cell-marker", "other", None]),
            "when": pd.to_datetime(["2026-01-01", None, "2026-03-01"], utc=True),
        },
        index=index,
    )
    expected = frame.copy(deep=True)
    expected["sum"] = 10
    try:
        result = client.call(ref, kwargs={"start_df": frame}, trust=True)
        copied_result = client.call(ref, kwargs={"start_df": frame, "copy_result": True})
    finally:
        logger.removeHandler(handler)
    assert result.value is frame
    assert frame["sum"].tolist() == [10, 10, 10]
    assert frame.attrs == {"pid": os.getpid(), "numpy_version": np.__version__}
    expected.attrs = frame.attrs.copy()
    pd.testing.assert_frame_equal(frame, expected)
    pd.testing.assert_frame_equal(result.value.head(2), expected.head(2))
    assert copied_result.value is not frame
    pd.testing.assert_frame_equal(copied_result.value, frame)
    assert result.payload["result"] == {
        "kind": "native-in-memory",
        "type": f"{type(frame).__module__}.{type(frame).__qualname__}",
        "reconstructable": False,
        "shape": [3, 4],
    }
    json.dumps(result.payload)
    assert "private-cell-marker" not in json.dumps(result.payload)
    assert "private-cell-marker" not in repr(result) + result._repr_html_()
    assert (
        np,
        np.__version__,
        np.__file__,
        pd,
        pd.__version__,
        pd.__file__,
        sys.path,
        os.getcwd(),
        dict(os.environ),
        sys.stdout,
        sys.stderr,
    ) == before
    assert "spl.daemon.worker" not in sys.modules
    assert events == ["started", "success", "started", "success"]
    assert np.__version__ == "2.1.3" and pd.__version__ == "2.2.3"
    assert any("numpy was recorded as 2.3.5" in msg and np.__version__ in msg for msg in messages)
    assert not any("pandas was recorded" in msg for msg in messages)
    print(
        json.dumps(
            {
                "caller_pid": os.getpid(),
                "result_type": type(result.value).__name__,
                "result_shape": list(result.value.shape),
                "result_identity_preserved": result.value is frame,
                "warnings": messages,
                "worker_imported": False,
                "numpy_unchanged": True,
                "pandas_unchanged": True,
                "events": events,
            }
        )
    )


if __name__ == "__main__":
    {"produce": produce, "consume": consume}[sys.argv[1]](Path(sys.argv[2]).resolve())
