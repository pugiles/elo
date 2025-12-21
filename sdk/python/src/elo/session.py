from __future__ import annotations

from typing import Optional

from .client import EloClient

_default_client: Optional[EloClient] = None


def setup(
    base_url: str = "http://127.0.0.1:3000",
    api_key: Optional[str] = None,
    timeout: float = 10.0,
    client: Optional[EloClient] = None,
) -> EloClient:
    global _default_client
    _default_client = client or EloClient(
        base_url=base_url,
        api_key=api_key,
        timeout=timeout,
    )
    return _default_client


def get_client() -> EloClient:
    if _default_client is None:
        raise RuntimeError("elo.setup(...) must be called before using models")
    return _default_client
