#!/usr/bin/env sh
set -euo pipefail

BASE_URL="${ELO_BASE_URL:-http://127.0.0.1:3000}"
API_KEY="${ELO_API_KEY:-seu_token}"
USERS="${SEED_USERS:-100000}"
RATE="${VEGETA_RATE:-200}"
DURATION="${VEGETA_DURATION:-30s}"
REC_TYPE="${REC_TYPE:-team}"
NUM_KEY="${REC_NUM_KEY:-rating}"
MIN="${REC_MIN:-300}"
MAX="${REC_MAX:-900}"
LIMIT="${REC_LIMIT:-10}"
OUT="${VEGETA_OUT:-/tmp/elo_reco.bin}"

TARGETS="$(mktemp)"
trap 'rm -f "$TARGETS"' EXIT

i=0
while [ "$i" -lt "$USERS" ]; do
  printf "GET %s/recommendations?start=user:%s&type=%s&num_key=%s&min=%s&max=%s&limit=%s\n" \
    "$BASE_URL" "$i" "$REC_TYPE" "$NUM_KEY" "$MIN" "$MAX" "$LIMIT" >> "$TARGETS"
  i=$((i + 1))
done

cat "$TARGETS" | vegeta attack -rate="$RATE" -duration="$DURATION" -header="x-api-key: $API_KEY" -output="$OUT"
vegeta report "$OUT"
