## Uso rapido

```python
from elo import EloClient

client = EloClient(base_url="http://127.0.0.1:3000", api_key="seu_token")

client.create_node("user:123", data={"type": "user"})
client.create_node("team:42", data={"type": "team", "rating": "520"})
client.create_edge("user:123", "team:42", data={"type": "owner"})

client.update_node("user:123", rating="850", status="active")
client.update_edge("user:123", "team:42", role="captain")

node = client.get_node("user:123")
print(node.id, node.data, node.edges)

teams = client.list_nodes(node_type="team")
edges = client.list_edges(edge_type="owner")

recs = client.recommendations(
    start="user:123",
    node_type="team",
    num_key="rating",
    min_value=300,
    max_value=900,
    limit=5,
)
print(recs)

# Filtrar por raio (usa GeoPoint no start ou lat/lon direto)
client.update_node("user:123", location="-23.5505,-46.6333")
nearby = client.recommendations(
    start="user:123",
    node_type="team",
    radius_km=10,
)
print(nearby)
```

## Uso com ORM (classes customizadas)

```python
import elo
from elo.orm import Node
from typing import List
from elo import GeoPoint

elo.setup("http://127.0.0.1:3000", "seu_token")

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


flamengo = Team(
    id="fla", city="Rio", sport="Futebol", location=GeoPoint(-22.9068, -43.1729)
).save()
joao = User(id="joao", email="j@j.com").save()
joao.follow(flamengo)
joao.update(active=False)

nearby = Team.find_near(-22.9068, -43.1729, radius_km=10)
print(nearby)
```

`find_near` usa geohash com prefixo para filtrar candidatos (aproximado).

Exemplo completo: `sdk/python/examples/orm_models.py`

## Rodando com uv

```sh
uv run python -c "from elo import EloClient; print(EloClient())"
```
