# Elo

Minimal graph API for nodes, edges, and recommendations.

## Quick start

```sh
ELO_API_KEY=your_token ELO_HOST=127.0.0.1 ELO_PORT=3000 cargo run
```

## Smoke test

```sh
ELO_API_KEY=your_token bash scripts/run_smoke.sh
```

## API examples

```sh
curl -X POST http://127.0.0.1:3000/nodes \
  -H "x-api-key: your_token" -H "content-type: application/json" \
  -d '{"id":"user:123"}'

curl -X POST http://127.0.0.1:3000/schema \
  -H "x-api-key: your_token" -H "content-type: application/json" \
  -d '{"entity":"node","fields":["type","rating","location","status"]}'

curl -X POST http://127.0.0.1:3000/schema \
  -H "x-api-key: your_token" -H "content-type: application/json" \
  -d '{"entity":"edge","fields":["type","weight","status"]}'

python - <<'PY'
from elo import EloClient
client = EloClient(base_url="http://127.0.0.1:3000", api_key="your_token")
client.upsert_schema("node", ["type", "rating", "location", "status"])
client.upsert_schema("edge", ["type", "weight", "status"])
PY

curl -X PUT http://127.0.0.1:3000/nodes/user:123/data \
  -H "x-api-key: your_token" -H "content-type: application/json" \
  -d '{"key":"type","value":"user"}'

curl -X PATCH http://127.0.0.1:3000/nodes/user:123 \
  -H "x-api-key: your_token" -H "content-type: application/json" \
  -d '{"data":{"rating":"850","status":"active"}}'

curl -X POST http://127.0.0.1:3000/edges \
  -H "x-api-key: your_token" -H "content-type: application/json" \
  -d '{"from":"user:123","to":"team:42"}'

curl -X PATCH http://127.0.0.1:3000/edges \
  -H "x-api-key: your_token" -H "content-type: application/json" \
  -d '{"from":"user:123","to":"team:42","data":{"role":"owner","since":"2025"}}'

curl -X POST http://127.0.0.1:3000/blocks \
  -H "x-api-key: your_token" -H "content-type: application/json" \
  -d '{"from":"user:123","to":"user:456"}'

curl -X DELETE "http://127.0.0.1:3000/blocks?from=user:123&to=user:456" \
  -H "x-api-key: your_token"

curl "http://127.0.0.1:3000/nodes?type=team&hydrate=false" \
  -H "x-api-key: your_token"

curl "http://127.0.0.1:3000/edges?type=owner&hydrate=false" \
  -H "x-api-key: your_token"

Notes:
- Edges with `type=block` are mirrored automatically (A->B also creates B->A). `type=mute` stays one-way.

curl "http://127.0.0.1:3000/recommendations?start=user:123&type=team&radius_km=10&exclude_edge_types=block,mute&hydrate=false" \
  -H "x-api-key: your_token"

curl "http://127.0.0.1:3000/nearby?type=Gym&geo_hash_prefix=6gkzwg" \
  -H "x-api-key: your_token"

curl "http://127.0.0.1:3000/nearby?type=Gym&geo_hash_prefix=6gkzwg&start=user:123&exclude_edge_types=block,mute" \
  -H "x-api-key: your_token"

curl "http://127.0.0.1:3000/nearby?type=Gym&lat=-22.9068&lon=-43.1729&radius_km=10" \
  -H "x-api-key: your_token"
```

## SDK (Python)

See `sdk/python/README.md` for installation and more examples.

```python
from elo import EloClient

client = EloClient(base_url="http://127.0.0.1:3000", api_key="seu_token")
client.upsert_schema("node", ["type", "rating", "location", "status"])
client.upsert_schema("edge", ["type", "weight", "status"])

client.update_node("Joao", email="joao@novo.com", status="active", level="pro")
client.update_edge("Joao", "Flamengo", since="2025", role="Captain")

node_full = client.get_node("Joao", hydrate=True)
node_light = client.get_node("Joao", hydrate=False)
```
