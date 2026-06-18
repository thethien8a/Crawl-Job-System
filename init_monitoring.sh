#!/usr/bin/env bash
# Khoi tao / trien khai lop Giam sat (Monitoring) tren EC2-B.
#
# Stack: Caddy (Basic Auth + reverse proxy) + Prometheus + Grafana
#        + nginx phuc vu dashboard HTML Bronze/Silver tu thu muc EFS chia se.
#
# Dieu kien tien quyet (lam mot lan trc khi chay script):
#   1. git clone repo ve EC2-B.
#   2. cp .env.example .env va dien: MONITORING_* (domain + Basic Auth),
#      SUPABASE_PROJECT_REF, SUPABASE_METRICS_SECRET_KEY,
#      va AIRFLOW_STATSD_EXPORTER_TARGET=<EC2-A-private-ip>:9102.
#   3. mount EFS vao src/monitoring_layer/business/reports (cung EFS voi EC2-A).
#
# Cach dung (chay tu thu muc repo):
#   bash init_monitoring.sh
#
# Script idempotent: chay lai bat ky luc nao de redeploy sau khi `git pull`.

set -euo pipefail

# Repo root = thu muc chua script nay; compose dung duong dan tuong doi nen phai dung cwd nay.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

COMPOSE_FILE="src/monitoring_layer/docker-compose.yaml"

echo "==> [1/4] Kiem tra dieu kien tien quyet..."
command -v docker >/dev/null 2>&1 || { echo "LOI: chua cai docker."; exit 1; }
docker compose version >/dev/null 2>&1 || { echo "LOI: thieu Docker Compose v2 (docker compose)."; exit 1; }
docker info >/dev/null 2>&1 || { echo "LOI: khong truy cap duoc Docker daemon (them user vao group docker?)."; exit 1; }

if [[ ! -f "${SCRIPT_DIR}/.env" ]]; then
  echo "LOI: khong tim thay .env o repo root."
  echo "     Chay: cp .env.example .env  roi dien MONITORING_*, SUPABASE_PROJECT_REF, SUPABASE_METRICS_SECRET_KEY, AIRFLOW_STATSD_EXPORTER_TARGET."
  exit 1
fi

echo "==> [2/4] Dam bao thu muc dashboard (EFS mount target) ton tai..."
mkdir -p "${SCRIPT_DIR}/src/monitoring_layer/business/reports"

# Tren kien truc 2 may, Prometheus phai scrape EC2-A qua IP private, khong phai sidecar local.
SCRAPE_TARGET="$(grep -E '^AIRFLOW_STATSD_EXPORTER_TARGET=' .env | cut -d= -f2- | tr -d '\r' | xargs || true)"
if [[ -z "$SCRAPE_TARGET" || "$SCRAPE_TARGET" == statsd-exporter:* ]]; then
  echo "CANH BAO: AIRFLOW_STATSD_EXPORTER_TARGET='$SCRAPE_TARGET' dang tro local."
  echo "         Tren EC2-B phai dat = <EC2-A-private-ip>:9102 de Prometheus scrape duoc."
fi

echo "==> [3/4] Khoi dong stack giam sat (Caddy + Prometheus + Grafana + nginx)..."
docker compose --project-directory "${SCRIPT_DIR}" -f "${COMPOSE_FILE}" up -d

echo "==> [4/4] Trang thai container:"
docker compose --project-directory "${SCRIPT_DIR}" -f "${COMPOSE_FILE}" ps

echo
echo "==> Xong."
echo "    Dashboards: http(s)://<MONITORING_DOMAIN>/business/"
echo "    Grafana:    http(s)://<MONITORING_DOMAIN>/grafana/"
echo "    Mo port 80/443 trong SG cua EC2-B."
