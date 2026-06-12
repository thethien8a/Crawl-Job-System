#!/usr/bin/env bash
# Bootstrap script for Lakehouse-Lite.
# Run this from the repo root on the HOST before starting Docker services.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "==> Creating temp_data directory for crawler bind-mount..."
mkdir -p "${SCRIPT_DIR}/src/crawl_layer/temp_data"

echo "==> Starting Orchestration layer (Airflow + Postgres)..."
docker compose --project-directory "${SCRIPT_DIR}" \
  -f src/orchestration_layer/docker-compose.yaml up -d

echo "==> Starting Monitoring layer (Prometheus + Grafana)..."
docker compose --project-directory "${SCRIPT_DIR}" \
  -f src/monitoring_layer/docker-compose.yaml up -d

echo "==> All services started."
echo "    Airflow UI: http://localhost:8080"
echo "    Grafana:    http://localhost:3000"
