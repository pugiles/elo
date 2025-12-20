import os
import subprocess
import time
import urllib.request

import pytest

from elo.client import EloClient
from elo.models import EdgeListResult


def _wait_ready(base_url: str, api_key: str) -> None:
    for _ in range(50):
        try:
            req = urllib.request.Request(
                f"{base_url}/nodes",
                headers={"x-api-key": api_key, "content-type": "application/json"},
            )
            with urllib.request.urlopen(req) as resp:
                if resp.status == 200:
                    return
        except Exception:
            time.sleep(0.1)
    raise RuntimeError("server not ready")


@pytest.mark.integration
def test_sdk_list_edges_smoke(tmp_path: str) -> None:
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
    api_key = "seu_token"
    base_url = "http://127.0.0.1:3006"
    env = os.environ.copy()
    env.update(
        {
            "ELO_API_KEY": api_key,
            "ELO_HOST": "127.0.0.1",
            "ELO_PORT": "3006",
            "ELO_DB_PATH": str(tmp_path / "elo_py_smoke.redb"),
        }
    )

    proc = subprocess.Popen(
        ["cargo", "run", "--release", "--bin", "elo"],
        cwd=base_dir,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    try:
        _wait_ready(base_url, api_key)
        client = EloClient(base_url=base_url, api_key=api_key)
        client.create_node("user:123")
        client.create_node("team:42")
        client.create_edge("user:123", "team:42")
        client.set_edge_data("user:123", "team:42", "type", "owner")
        edges = client.list_edges(edge_type="owner")

        assert edges
        assert isinstance(edges[0], EdgeListResult)
        assert edges[0].from_ == "user:123"
        assert edges[0].to == "team:42"
    finally:
        proc.terminate()
        proc.wait(timeout=5)
