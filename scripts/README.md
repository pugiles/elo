# Load Testing

## Seed a large dataset

Seed directly into the RedB file:

```sh
ELO_DB_PATH=elo.redb SEED_USERS=100000 SEED_TEAMS=10000 SEED_USER_EDGES=5 SEED_TEAM_EDGES=5 \
SEED_RATING_MIN=300 SEED_RATING_MAX=900 SEED_BATCH=10000 SEED_RESET=true \
cargo run --release --bin seed
```

## Run the API server

```sh
ELO_API_KEY=seu_token ELO_DB_PATH=elo.redb ELO_HOST=127.0.0.1 ELO_PORT=3000 cargo run --release
```

## Smoke test

```sh
ELO_API_KEY=seu_token bash scripts/run_smoke.sh
```

## Load test (k6)

```sh
ELO_API_KEY=seu_token SEED_USERS=100000 K6_VUS=50 K6_DURATION=60s \
k6 run scripts/load_k6.js
```

## Load test (k6 mixed workload)

```sh
ELO_API_KEY=seu_token SEED_USERS=100000 SEED_TEAMS=10000 K6_VUS=50 K6_DURATION=60s \
k6 run scripts/load_k6_mixed.js
```

## Load test (k6 profile weights)

```sh
# Read-heavy
ELO_API_KEY=seu_token SEED_USERS=100000 SEED_TEAMS=10000 K6_VUS=50 K6_DURATION=60s \
REC_PCT=0.8 GET_PCT=0.1 LIST_PCT=0.08 WRITE_PCT=0.02 \
k6 run scripts/load_k6_profile.js

# Write-heavy
ELO_API_KEY=seu_token SEED_USERS=100000 SEED_TEAMS=10000 K6_VUS=50 K6_DURATION=60s \
REC_PCT=0.3 GET_PCT=0.1 LIST_PCT=0.1 WRITE_PCT=0.5 \
k6 run scripts/load_k6_profile.js
```

## Cold vs warm runs (k6)

```sh
ELO_API_KEY=seu_token SEED_USERS=100000 K6_VUS=50 K6_DURATION=60s K6_WARMUP=30s \
scripts/run_k6_warm_cold.sh
```

## Cold vs warm with restart (k6)

```sh
ELO_API_KEY=seu_token ELO_DB_PATH=elo.redb K6_VUS=50 K6_DURATION=60s K6_WARMUP=30s \
scripts/run_k6_restart.sh
```

## Load test (vegeta)

```sh
ELO_API_KEY=seu_token SEED_USERS=100000 VEGETA_RATE=500 VEGETA_DURATION=60s \
scripts/load_vegeta.sh
```
