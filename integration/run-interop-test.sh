#!/usr/bin/env bash
# End-to-end interop test: Node producer -> Redis -> Go asynq consumer.
#
# Prerequisites:
#   - Redis running on 127.0.0.1:6379 (docker run -d -p 6379:6379 redis:7-alpine)
#   - Go toolchain on PATH (for the consumer)
#   - npm dependencies installed (npm install)
#
# Usage: ./integration/run-interop-test.sh
set -euo pipefail

cd "$(dirname "$0")/.."

REDIS_ADDR="${REDIS_ADDR:-127.0.0.1:6379}"
RESULT_FILE="${RESULT_FILE:-/tmp/asynq-interop-results.jsonl}"
CONSUMER_BIN=./integration/consumer
CONSUMER_LOG=/tmp/asynq-interop-consumer.log

echo "==> flushing redis"
docker exec asynq-test-redis redis-cli flushall >/dev/null 2>&1 || true
rm -f "$RESULT_FILE" "$CONSUMER_LOG"

echo "==> building Go consumer"
(cd integration && go build -o consumer consumer.go)

echo "==> starting Go asynq consumer"
REDIS_ADDR="$REDIS_ADDR" RESULT_FILE="$RESULT_FILE" "$CONSUMER_BIN" >"$CONSUMER_LOG" 2>&1 &
CONSUMER_PID=$!
trap 'kill "$CONSUMER_PID" 2>/dev/null || true' EXIT
sleep 2

echo "==> producing tasks from Node"
./node_modules/.bin/tsx integration/produce.mts

echo "==> waiting for Go consumer to process"
# Wait until the result file has 4 lines (one per task) or timeout.
for i in $(seq 1 30); do
  if [ -f "$RESULT_FILE" ] && [ "$(wc -l < "$RESULT_FILE")" -ge 4 ]; then
    break
  fi
  sleep 1
done

kill "$CONSUMER_PID" 2>/dev/null || true
sleep 1

echo "==> consumer log"
tail -20 "$CONSUMER_LOG"

echo "==> results"
cat "$RESULT_FILE"

LINES=$(wc -l < "$RESULT_FILE")
if [ "$LINES" -lt 4 ]; then
  echo "FAIL: expected 4 consumed tasks, got $LINES"
  exit 1
fi

echo "PASS: Go asynq server consumed all $LINES Node-produced tasks"
