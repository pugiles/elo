import os
import subprocess
import time
import urllib.request

import pytest

from elo.client import EloClient


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
def test_lifecycle_simulation_phases(tmp_path: str) -> None:
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
    api_key = "seu_token"
    base_url = "http://127.0.0.1:3007"
    env = os.environ.copy()
    env.update(
        {
            "ELO_API_KEY": api_key,
            "ELO_HOST": "127.0.0.1",
            "ELO_PORT": "3007",
            "ELO_DB_PATH": str(tmp_path / "elo_py_lifecycle.redb"),
        }
    )

    proc = subprocess.Popen(
        ["cargo", "run", "--release", "--bin", "elo"],
        cwd=base_dir,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    phases = [(1, 1, 0), (3, 2, 2), (5, 3, 4), (8, 4, 8), (13, 6, 13)]
    total_users = 0
    total_teams = 0
    total_posts = 0
    total_memberships = 0
    total_authors = 0

    try:
        _wait_ready(base_url, api_key)
        client = EloClient(base_url=base_url, api_key=api_key)

        for phase_index, (users, teams, posts) in enumerate(phases, start=1):
            for i in range(teams):
                team_id = f"team:{phase_index}:{i}"
                client.create_node(
                    team_id, data={"type": "Team", "phase": str(phase_index)}
                )

            for i in range(users):
                user_id = f"user:{phase_index}:{i}"
                client.create_node(
                    user_id, data={"type": "User", "phase": str(phase_index)}
                )

            for i in range(posts):
                post_id = f"post:{phase_index}:{i}"
                client.create_node(
                    post_id, data={"type": "Post", "phase": str(phase_index)}
                )

            team_count = max(teams, 1)
            user_count = max(users, 1)

            for i in range(users):
                user_id = f"user:{phase_index}:{i}"
                team_id = f"team:{phase_index}:{i % team_count}"
                client.create_edge(user_id, team_id, data={"type": "member"})
                total_memberships += 1

            for i in range(posts):
                post_id = f"post:{phase_index}:{i}"
                user_id = f"user:{phase_index}:{i % user_count}"
                team_id = f"team:{phase_index}:{i % team_count}"
                client.create_edge(user_id, post_id, data={"type": "author"})
                client.create_edge(team_id, post_id, data={"type": "host"})
                total_authors += 1

            total_users += users
            total_teams += teams
            total_posts += posts

            assert len(client.list_nodes(node_type="User")) >= total_users
            assert len(client.list_nodes(node_type="Team")) >= total_teams
            assert len(client.list_nodes(node_type="Post")) >= total_posts
            assert len(client.list_edges(edge_type="member")) >= total_memberships
            assert len(client.list_edges(edge_type="author")) >= total_authors
    finally:
        proc.terminate()
        proc.wait(timeout=5)
