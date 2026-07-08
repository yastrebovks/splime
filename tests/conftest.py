from __future__ import annotations

from collections.abc import Iterator

import pytest

from spl.core.adapter_compat import _reset_adapter_compatibility_warnings


@pytest.fixture(autouse=True)
def reset_adapter_compatibility_warnings() -> Iterator[None]:
    """Keep process-global adapter warning dedupe isolated between tests."""

    _reset_adapter_compatibility_warnings()
    yield
    _reset_adapter_compatibility_warnings()
