#!/usr/bin/env bash
# demo.sh
# Simple live demo script for the Chord + KV + Gnutella system.

set -euo pipefail

BASE_PORT=5000
NUM_NODES=4

PIDS=()

start_nodes() {
  echo "=== Starting $NUM_NODES Chord nodes ==="
  for ((i=0; i<NUM_NODES; i++)); do
    PORT=$((BASE_PORT + i))
    if [[ $i -eq 0 ]]; then
      echo "Starting bootstrap node on port $PORT"
      python run_node.py --host 127.0.0.1 --port "$PORT" &
    else
      echo "Starting node on port $PORT (bootstraps to 127.0.0.1:${BASE_PORT})"
      python run_node.py --host 127.0.0.1 --port "$PORT" --bootstrap 127.0.0.1:${BASE_PORT} &
    fi
    PIDS+=($!)
    sleep 0.5
  done

  echo "Waiting 5 seconds for ring stabilization..."
  sleep 5
}

stop_nodes() {
  echo
  echo "=== Stopping all nodes ==="
  for pid in "${PIDS[@]:-}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
    fi
  done
}

trap stop_nodes EXIT

put_keys() {
  echo
  echo "=== PUT some keys into the cluster ==="
  for i in {0..4}; do
    KEY="key-$i"
    VALUE="value-$i"
    # send PUT to a different node each time just to show distribution
    PORT=$((BASE_PORT + (i % NUM_NODES)))
    echo "PUT $KEY -> $VALUE via node $PORT"
    curl -s -X POST "http://127.0.0.1:${PORT}/put" \
      -H "Content-Type: application/json" \
      -d "{\"key\":\"${KEY}\", \"value\":\"${VALUE}\", \"writer_id\":\"demo-client\"}" \
      >/dev/null
  done
}

get_keys() {
  echo
  echo "=== GET the same keys from node ${BASE_PORT} ==="
  for i in {0..4}; do
    KEY="key-$i"
    echo -n "GET $KEY -> "
    curl -s -X POST "http://127.0.0.1:${BASE_PORT}/get" \
      -H "Content-Type: application/json" \
      -d "{\"key\":\"${KEY}\"}"
    echo
  done
}

crash_one_node() {
  echo
  echo "=== Simulating a crash on node port 5002 ==="
  CRASH_INDEX=2          # index in PIDS array (0-based)
  CRASH_PORT=$((BASE_PORT + CRASH_INDEX))
  CRASH_PID=${PIDS[$CRASH_INDEX]}

  echo "Killing node on port ${CRASH_PORT} (PID ${CRASH_PID})"
  kill "$CRASH_PID" 2>/dev/null || true
  # Mark it as dead in the array
  PIDS[$CRASH_INDEX]=0

  echo "Waiting 5 seconds for Chord to detect failure and re-stabilize..."
  sleep 5
}

restart_node() {
  echo
  echo "=== Restarting crashed node on port 5002 ==="
  RESTART_INDEX=2
  PORT=$((BASE_PORT + RESTART_INDEX))

  echo "Restarting node on port ${PORT} with bootstrap 127.0.0.1:${BASE_PORT}"
  python run_node.py --host 127.0.0.1 --port "$PORT" --bootstrap 127.0.0.1:${BASE_PORT} &
  PIDS[$RESTART_INDEX]=$!

  echo "Waiting 5 seconds for node to rejoin and anti-entropy to run..."
  sleep 5
}

show_metrics() {
  echo
  echo "=== Metrics from node ${BASE_PORT} ==="
  echo "(If you have jq installed, you can pipe this through jq for pretty-printing.)"
  curl -s "http://127.0.0.1:${BASE_PORT}/metrics"
  echo
}

gnutella_demo() {
  echo
  echo "=== Gnutella flooding demo (optional) ==="
  echo "Starting a Gnutella query for key-1 with TTL=5 via node ${BASE_PORT}"
  curl -s -X POST "http://127.0.0.1:${BASE_PORT}/g_start_query" \
    -H "Content-Type: application/json" \
    -d '{"key":"key-1","ttl":5}'
  echo
}

########################
# Main demo flow
########################

start_nodes

put_keys
get_keys

crash_one_node

echo
echo "=== GET after crash (still via node ${BASE_PORT}) ==="
get_keys

restart_node

echo
echo "=== GET after node has rejoined ==="
get_keys

gnutella_demo

show_metrics

echo
echo "Demo complete. Press Ctrl+C to stop, or wait for automatic cleanup."
# Script will exit here; trap will stop nodes.
sleep 3