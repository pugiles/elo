from typing import List

import elo
from elo import GeoPoint
from elo.orm import Node


class User(Node):
    class Meta:
        node_type = "User"

    def follow(self, team: "Team") -> None:
        self._client().post(
            "/edges",
            json={
                "from": self.id,
                "to": team.id,
                "data": {"since": "2025-01-01", "type": "Follows"},
            },
        )

    def get_suggested_teams(self, limit: int = 5) -> List["Team"]:
        resp = self._client().get(
            "/recommendations",
            params={"start": self.id, "type": "Team", "limit": limit},
        )
        data = resp.json()
        return [Team(id=item["id"], **item["data"]) for item in data]


class Team(Node):
    class Meta:
        node_type = "Team"


def main() -> None:
    elo.setup("http://127.0.0.1:3000", "seu_token")

    flamengo = Team(
        id="fla", city="Rio", sport="Futebol", location=GeoPoint(-22.9068, -43.1729)
    ).save()
    joao = User(id="joao", email="j@j.com", location=GeoPoint(-22.9065, -43.1720)).save()

    joao.follow(flamengo)
    joao.update(active=False)

    suggestions = joao.get_suggested_teams()
    print(suggestions)

    nearby = Team.find_near(-22.9068, -43.1729, radius_km=10)
    print(nearby)


if __name__ == "__main__":
    main()
