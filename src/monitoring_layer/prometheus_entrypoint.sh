#!/bin/sh
# Render prometheus.yml template by replacing ${VAR} placeholders
# with actual environment variable values, then start Prometheus.

set -e

TEMPLATE="/etc/prometheus/prometheus.template.yml"
CONFIG="/tmp/prometheus.yml"

cp "$TEMPLATE" "$CONFIG"

# Replace placeholder strings with values from environment.
# Uses pipe delimiter to avoid conflicts with URLs containing slashes.
sed -i "s|\${SUPABASE_PROJECT_REF}|${SUPABASE_PROJECT_REF}|g" "$CONFIG"
sed -i "s|\${SUPABASE_METRICS_SECRET_KEY}|${SUPABASE_METRICS_SECRET_KEY}|g" "$CONFIG"

exec /bin/prometheus \
  --config.file="$CONFIG" \
  --storage.tsdb.path=/prometheus
