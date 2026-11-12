#!/bin/sh
# Creates the connector, or updates its config if it already exists. PUT on
# /connectors/<name>/config does both, so this is safe to run every start.
set -eu
CONNECT=${CONNECT_URL:-http://connect:8083}

until curl -sf "$CONNECT/connectors" >/dev/null; do
  echo "waiting for Kafka Connect"
  sleep 3
done

name=$(sed -n 's/.*"name": "\([^"]*\)".*/\1/p' /connect/source.json | head -n1)
# Only the "config" object goes to PUT.
config=$(tr -d '\n' < /connect/source.json | sed 's/.*"config": \({.*}\)[[:space:]]*}[[:space:]]*$/\1/')

curl -sf -X PUT -H 'Content-Type: application/json' --data "$config" "$CONNECT/connectors/$name/config"
echo
curl -sf "$CONNECT/connectors/$name/status"
echo
