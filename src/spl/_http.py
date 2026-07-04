"""HTTP helpers shared by central-server clients."""

from __future__ import annotations

import ssl
from functools import lru_cache
from typing import Any
from urllib.request import Request, urlopen


@lru_cache(maxsize=1)
def verified_https_context() -> ssl.SSLContext:
    """Return a certificate-verifying HTTPS context backed by certifi."""

    import certifi

    return ssl.create_default_context(cafile=certifi.where())


def urlopen_verified(request: Request) -> Any:
    """Open a URL request with the bundled certifi CA bundle."""

    return urlopen(request, context=verified_https_context())
