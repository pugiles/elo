import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SDK_PATH = ROOT / "sdk" / "python" / "src"
sys.path.insert(0, str(SDK_PATH))

from elo import EloClient  # noqa: E402


def main() -> None:
    api_key = os.getenv("ELO_API_KEY", "seu_token")
    host = os.getenv("ELO_HOST", "127.0.0.1")
    port = os.getenv("ELO_PORT", "3000")
    base_url = os.getenv("ELO_BASE_URL", f"http://{host}:{port}")

    client = EloClient(base_url=base_url, api_key=api_key)
    phases = [(1, 1, 0), (3, 2, 2), (5, 3, 4), (8, 4, 8), (13, 6, 13)]
    total_users = 0
    total_teams = 0
    total_posts = 0

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

        for i in range(posts):
            post_id = f"post:{phase_index}:{i}"
            user_id = f"user:{phase_index}:{i % user_count}"
            team_id = f"team:{phase_index}:{i % team_count}"
            client.create_edge(user_id, post_id, data={"type": "author"})
            client.create_edge(team_id, post_id, data={"type": "host"})

        total_users += users
        total_teams += teams
        total_posts += posts

        print(
            f"phase {phase_index}: users={total_users} teams={total_teams} posts={total_posts}"
        )


if __name__ == "__main__":
    main()
