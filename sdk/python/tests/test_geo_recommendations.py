import os
import subprocess
import time
import urllib.request

import pytest

from elo.client import EloClient


def _wait_ready(base_url: str, api_key: str) -> None:
    max_attempts = 200
    for _ in range(max_attempts):
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
def test_recommendations_filter_by_geo_radius(tmp_path: str) -> None:
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
    api_key = "seu_token"
    base_url = "http://127.0.0.1:3008"
    env = os.environ.copy()
    env.update(
        {
            "ELO_API_KEY": api_key,
            "ELO_HOST": "127.0.0.1",
            "ELO_PORT": "3008",
            "ELO_DB_PATH": str(tmp_path / "elo_py_geo.redb"),
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

        client.create_node(
            "user:me", data={"type": "User", "location": "-23.5505,-46.6333"}
        )
        client.create_node("user:friend", data={"type": "User"})
        client.create_node(
            "team:near", data={"type": "Team", "location": "-23.5510,-46.6340"}
        )
        client.create_node(
            "team:far", data={"type": "Team", "location": "-22.9068,-43.1729"}
        )

        client.create_edge("user:me", "user:friend")
        client.create_edge("user:friend", "team:near")
        client.create_edge("user:friend", "team:far")

        recs = client.recommendations(
            start="user:me", node_type="Team", radius_km=10
        )
        assert [rec.id for rec in recs] == ["team:near"]
    finally:
        proc.terminate()
        proc.wait(timeout=5)
