#!/bin/bash
set -e

PORTS=(8080 8081 8082 8083 8084)
MAX_RETRIES=30
SLEEP_SECS=2

echo "Waiting for services to become healthy..."

for port in "${PORTS[@]}"; do
  echo "Testing port $port..."
  count=0
  until curl -s http://localhost:$port/health | grep -q '"status":"ok"'; do
    count=$((count + 1))
    if [ $count -ge $MAX_RETRIES ]; then
      echo "Timeout waiting for service on port $port"
      exit 1
    fi
    sleep $SLEEP_SECS
  done
  echo "Service on port $port is healthy!"
done

echo "All services are healthy!"
