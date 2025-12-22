import os
import subprocess
import time
import urllib.request

import pytest

import elo
from elo import GeoPoint
from elo.orm import Node


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


class Gym(Node):
    class Meta:
        node_type = "Gym"


@pytest.mark.integration
def test_find_near_uses_geo_hash_prefix(tmp_path: str) -> None:
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
    api_key = "seu_token"
    base_url = "http://127.0.0.1:3009"
    env = os.environ.copy()
    env.update(
        {
            "ELO_API_KEY": api_key,
            "ELO_HOST": "127.0.0.1",
            "ELO_PORT": "3009",
            "ELO_DB_PATH": str(tmp_path / "elo_py_nearby.redb"),
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
        elo.setup(base_url, api_key)

        Gym(id="gym:near", location=GeoPoint(-23.5505, -46.6333)).save()
        Gym(id="gym:far", location=GeoPoint(40.7128, -74.0060)).save()

        nearby = Gym.find_near(-23.5505, -46.6333, radius_km=10)
        assert [gym.id for gym in nearby] == ["gym:near"]
    finally:
        proc.terminate()
        proc.wait(timeout=5)
