#!/usr/bin/env sh
set -euo pipefail

API_KEY="${ELO_API_KEY:-seu_token}"
BASE_URL="${ELO_BASE_URL:-http://127.0.0.1:3000}"
USERS="${SEED_USERS:-100000}"
VUS="${K6_VUS:-50}"
DURATION="${K6_DURATION:-60s}"
WARMUP="${K6_WARMUP:-30s}"

echo "Warm-up: ${WARMUP}"
ELO_API_KEY="$API_KEY" ELO_BASE_URL="$BASE_URL" SEED_USERS="$USERS" K6_VUS="$VUS" K6_DURATION="$WARMUP" \
  k6 run scripts/load_k6.js

echo "Cold run: ${DURATION}"
ELO_API_KEY="$API_KEY" ELO_BASE_URL="$BASE_URL" SEED_USERS="$USERS" K6_VUS="$VUS" K6_DURATION="$DURATION" \
  k6 run scripts/load_k6.js

echo "Warm run: ${DURATION}"
ELO_API_KEY="$API_KEY" ELO_BASE_URL="$BASE_URL" SEED_USERS="$USERS" K6_VUS="$VUS" K6_DURATION="$DURATION" \
  k6 run scripts/load_k6.js
