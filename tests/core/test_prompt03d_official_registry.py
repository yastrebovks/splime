from __future__ import annotations

import pytest

from spl.embedded import EmbeddedBackend
from spl.official_registry import (
    OFFICIAL_REGISTRY_KEY_ID,
    OFFICIAL_REGISTRY_ORIGIN,
    OFFICIAL_REGISTRY_PUBLIC_KEY_B64,
    official_registry_trusted_keys,
)


def test_official_registry_uses_the_packaged_owner_pin_without_injection(tmp_path) -> None:
    backend = EmbeddedBackend(OFFICIAL_REGISTRY_ORIGIN, tmp_path, run_receipts=False)

    assert backend.trusted_keys == {
        "https://splime.io": {
            "ed25519-sha256:635b0f38b59fa4a513723bc8e347d807dae535996173ffebfba2fcca73cbe91e": (
                "DDWqFt/l2ZaRFMgvdCdyoCcQ15ixQyMjx4nqtmvqTz4="
            )
        }
    }
    assert OFFICIAL_REGISTRY_KEY_ID in backend.trusted_keys[OFFICIAL_REGISTRY_ORIGIN]
    assert OFFICIAL_REGISTRY_PUBLIC_KEY_B64 == backend.trusted_keys[OFFICIAL_REGISTRY_ORIGIN][OFFICIAL_REGISTRY_KEY_ID]


def test_official_pin_is_exact_origin_only_and_custom_origins_remain_explicit(tmp_path) -> None:
    assert official_registry_trusted_keys("https://splime.io") is not None
    for lookalike in (
        "https://splime.io:443",
        "https://api.splime.io",
        "https://splime.io.example.test",
        "https://example.test",
    ):
        assert official_registry_trusted_keys(lookalike) is None
        assert (
            EmbeddedBackend(lookalike, tmp_path / lookalike.replace("/", "_"), run_receipts=False).trusted_keys is None
        )
    assert official_registry_trusted_keys("http://splime.io") is None
    with pytest.raises(ValueError, match="must use HTTPS"):
        EmbeddedBackend("http://splime.io", tmp_path / "http", run_receipts=False)
