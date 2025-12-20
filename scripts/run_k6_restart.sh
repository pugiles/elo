#!/usr/bin/env sh
set -euo pipefail

API_KEY="${ELO_API_KEY:-seu_token}"
BASE_URL="${ELO_BASE_URL:-http://127.0.0.1:3000}"
HOST="${ELO_HOST:-127.0.0.1}"
PORT="${ELO_PORT:-3000}"
DB_PATH="${ELO_DB_PATH:-elo.redb}"
VUS="${K6_VUS:-50}"
DURATION="${K6_DURATION:-60s}"
WARMUP="${K6_WARMUP:-30s}"

SERVER_CMD="${ELO_SERVER_CMD:-}"
if [ -z "$SERVER_CMD" ]; then
  if [ -x "target/release/elo" ]; then
    SERVER_CMD="target/release/elo"
  else
    SERVER_CMD="cargo run --release"
  fi
fi

PID_FILE="/tmp/elo_server.pid"

wait_ready() {
  retries=50
  while [ "$retries" -gt 0 ]; do
    if curl -s -o /dev/null -w "%{http_code}" "http://$HOST:$PORT/nodes" \
      -H "x-api-key: $API_KEY" -H "content-type: application/json" | grep -q 200; then
      return 0
    fi
    retries=$((retries - 1))
    sleep 0.1
  done
  echo "Server did not become ready in time" >&2
  return 1
}

start_server() {
  ELO_API_KEY="$API_KEY" ELO_HOST="$HOST" ELO_PORT="$PORT" ELO_DB_PATH="$DB_PATH" \
    sh -c "$SERVER_CMD" >/tmp/elo_server.log 2>&1 &
  echo $! >"$PID_FILE"
  wait_ready
}

stop_server() {
  if [ -f "$PID_FILE" ]; then
    kill "$(cat "$PID_FILE")" >/dev/null 2>&1 || true
    rm -f "$PID_FILE"
  fi
}

trap stop_server EXIT INT TERM

echo "Cold run (fresh server): ${DURATION}"
start_server
ELO_API_KEY="$API_KEY" ELO_BASE_URL="$BASE_URL" K6_VUS="$VUS" K6_DURATION="$DURATION" \
  k6 run scripts/load_k6.js
stop_server

echo "Warm run (restart + warmup ${WARMUP}): ${DURATION}"
start_server
ELO_API_KEY="$API_KEY" ELO_BASE_URL="$BASE_URL" K6_VUS="$VUS" K6_DURATION="$WARMUP" \
  k6 run scripts/load_k6.js
ELO_API_KEY="$API_KEY" ELO_BASE_URL="$BASE_URL" K6_VUS="$VUS" K6_DURATION="$DURATION" \
  k6 run scripts/load_k6.js
