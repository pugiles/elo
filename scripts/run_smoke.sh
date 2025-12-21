#!/usr/bin/env bash
set -euo pipefail

API_KEY="${ELO_API_KEY:-seu_token}"
HOST="${ELO_HOST:-127.0.0.1}"
PORT="${ELO_PORT:-3000}"

ELO_API_KEY="$API_KEY" cargo run --bin elo >/tmp/elo-server.log 2>&1 &
server_pid=$!

cleanup() {
  if kill -0 "$server_pid" 2>/dev/null; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
}

trap cleanup EXIT

for _ in {1..30}; do
  if curl -s -o /dev/null -w "%{http_code}" \
    -H "x-api-key: $API_KEY" \
    "http://$HOST:$PORT/nodes" | grep -qE "^(200|401)$"; then
    break
  fi
  sleep 0.2
done

ELO_API_KEY="$API_KEY" python3 scripts/test_api.py
