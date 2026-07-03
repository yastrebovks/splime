import pytest


@pytest.mark.smoke
def test_core() -> None:
    import spl.core  # noqa: F401
    from spl import SPLClient as TopLevelSPLClient
    from spl._client import SPLClient

    assert SPLClient(daemon_port=8765)._daemon.base_url == "http://127.0.0.1:8765"
    assert TopLevelSPLClient is SPLClient
