#!/usr/bin/env bash
# Khoi tao / trien khai lop Dieu phoi (Orchestration) tren EC2-A.
#
# Stack: Airflow apiserver + scheduler + Postgres + statsd-exporter,
#        cong them cac container pipeline `lakehouse-crawler` chay qua DockerOperator.
#
# Dieu kien tien quyet (lam mot lan trc khi chay script):
#   1. git clone repo ve EC2-A.
#   2. cp .env.example .env  va dien: HOST_REPO_PATH, FERNET_KEY, WEBSERVER_SECRET_KEY,
#      AWS_*, Supabase, MotherDuck, ITVIEC_*, AIRFLOW_UID, _AIRFLOW_WWW_USER_*.
#   3. (tuy chon) mount EFS vao src/monitoring_layer/business/reports neu chay 2 may.
#
# Cach dung (chay tu thu muc repo):
#   bash init_orchestration.sh
#
# Script idempotent: chay lai bat ky luc nao de redeploy sau khi `git pull`.

set -euo pipefail

# Repo root = thu muc chua script nay; compose dung duong dan tuong doi nen phai dung cwd nay.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

COMPOSE_FILE="src/orchestration_layer/docker-compose.yaml"
PIPELINE_IMAGE="${PIPELINE_IMAGE:-lakehouse-crawler:latest}"

set_env_var() {
  local key="$1"
  local value="$2"

  if grep -qE "^${key}=" .env; then
    sed -i "s|^${key}=.*|${key}=${value}|" .env
  else
    printf '\n%s=%s\n' "$key" "$value" >> .env
  fi
}

echo "==> [1/5] Kiem tra dieu kien tien quyet..."
command -v docker >/dev/null 2>&1 || { echo "LOI: chua cai docker."; exit 1; }
docker compose version >/dev/null 2>&1 || { echo "LOI: thieu Docker Compose v2 (docker compose)."; exit 1; }
docker info >/dev/null 2>&1 || { echo "LOI: khong truy cap duoc Docker daemon (them user vao group docker?)."; exit 1; }

if [[ ! -f "${SCRIPT_DIR}/.env" ]]; then
  echo "LOI: khong tim thay .env o repo root."
  echo "     Chay: cp .env.example .env  roi dien cac bien can thiet."
  exit 1
fi

# DockerOperator bind-mounts the host Docker socket and host repo path, so keep
# these deployment-derived values in sync on every redeploy.
DOCKER_GID="$(stat -c '%g' /var/run/docker.sock)"
set_env_var "DOCKER_GID" "$DOCKER_GID"
set_env_var "HOST_REPO_PATH" "$SCRIPT_DIR"
echo "DOCKER_GID=${DOCKER_GID} (nhom cua /var/run/docker.sock)"
echo "HOST_REPO_PATH=${SCRIPT_DIR}"

echo "==> [2/5] Tao cac thu muc bind-mount..."
mkdir -p "${SCRIPT_DIR}/src/crawl_layer/temp_data"
mkdir -p "${SCRIPT_DIR}/src/orchestration_layer/logs"
mkdir -p "${SCRIPT_DIR}/src/monitoring_layer/business/reports"

echo "==> [3/5] Build image pipeline (${PIPELINE_IMAGE}) cho DockerOperator..."
docker build -t "${PIPELINE_IMAGE}" .

echo "==> [4/5] Khoi dong stack dieu phoi (Airflow + Postgres + statsd-exporter)..."
docker compose --project-directory "${SCRIPT_DIR}" -f "${COMPOSE_FILE}" up -d

echo "==> [5/5] Trang thai container:"
docker compose --project-directory "${SCRIPT_DIR}" -f "${COMPOSE_FILE}" ps

echo
echo "==> Xong. Airflow UI: http://<EC2-A-IP>:8080"
echo "    Mo port 9102 tu SG cua EC2-B de Prometheus scrape statsd-exporter."
