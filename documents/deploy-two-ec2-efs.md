# Triển khai Giám sát và Điều phối trên Hai Máy chủ EC2 (chia sẻ EFS)

Hướng dẫn này chia tách stack trên hai máy chủ EC2:

- **EC2-A — Điều phối**: Airflow (apiserver + scheduler), Postgres, `statsd-exporter`,
  và các container pipeline `lakehouse-crawler` được khởi chạy qua `DockerOperator`.
- **EC2-B — Giám sát**: Caddy (cổng xác thực), Prometheus, Grafana, và dịch vụ nginx
  phục vụ các dashboard Bronze/Silver.

Có chính xác hai liên kết xuyên máy chủ, cả hai đã được đấu nối sẵn trong code/config:

1. **Scrape metrics** — Prometheus (EC2-B) scrape `statsd-exporter` của Airflow trên EC2-A.
2. **File dashboard** — các DAG điều phối ghi dashboard HTML; nginx giám sát phục vụ nó.
   Chúng chia sẻ một thư mục duy nhất được hỗ trợ bởi **Amazon EFS** được mount trên cả hai máy chủ.

```diagram
             EC2-B  (Giám sát)                         EC2-A  (Điều phối)
  ╭──────────────────────────────────╮       ╭──────────────────────────────────────╮
  │ caddy  :80/:443  (xác thực cơ bản)│       │ airflow-apiserver  :8080              │
  │   ├─▶ grafana   :3000 (nội bộ)    │       │ airflow-scheduler                    │
  │   └─▶ nginx "dashboards"          │       │ postgres                             │
  │ prometheus :9090 (nội bộ) ────────┼──────▶│ statsd-exporter   :9102 (published)  │
  │                                   │ scrape │ DockerOperator ─▶ lakehouse-crawler  │
  │ nginx phục vụ ◀── reports/ ◀──────┼───────┼── ghi reports/ (dashboard HTML)    │
  ╰───────────────────┬──────────────╯  EFS   ╰───────────────┬──────────────────────╯
                      └──────────── Amazon EFS (shared) ───────┘
         mount trên CẢ HAI máy chủ tại:  <repo>/src/monitoring_layer/business/reports
```

> Pipeline (crawl → bronze → silver → supabase → gold) chạy **chỉ trên EC2-A**
> bên trong container `lakehouse-crawler` và giao tiếp với AWS S3 / Supabase / MotherDuck
> qua internet. Không có gì trong luồng đó cần EC2-B.

---

## 1. Điều kiện tiên quyết

- Hai máy chủ EC2 trong **cùng một VPC** (sử dụng IP private cho việc scrape xuyên máy chủ).
- Docker + Docker Compose v2 trên cả hai.
- Một **hệ thống file EFS** có thể truy cập từ cả hai máy chủ.
- Một bản sao repo trên mỗi máy chủ (mỗi máy chủ chỉ chạy file compose của riêng nó).

## 2. Tạo và mount EFS trên cả hai máy chủ

Thư mục chia sẻ là thư mục đầu ra dashboard. Mount EFS chính xác tại:

```
<repo>/src/monitoring_layer/business/reports
```

Trên **mỗi** máy chủ:

```bash
# amazon-efs-utils: `sudo yum install -y amazon-efs-utils`  (AL2)
#                   `sudo apt-get install -y amazon-efs-utils` (Ubuntu)
sudo mkdir -p /home/ec2-user/Lakehouse-Lite/src/monitoring_layer/business/reports
sudo mount -t efs -o tls fs-0123456789abcdef0:/ \
  /home/ec2-user/Lakehouse-Lite/src/monitoring_layer/business/reports
```

Duy trì qua các lần khởi động lại trong `/etc/fstab`:

```
fs-0123456789abcdef0:/ /home/ec2-user/Lakehouse-Lite/src/monitoring_layer/business/reports efs _netdev,tls 0 0
```

**Quyền**: container pipeline ghi HTML và nginx đọc nó. Tùy chọn đơn giản và ổn định nhất là một **EFS Access Point** với POSIX uid/gid `0` và quyền thư mục gốc `0777`, hoặc `sudo chmod 0777` thư mục `reports/` đã mount.
Các file không nhạy cảm (và nằm sau Caddy basic auth).

## 3. Security Groups

| From → To                     | Port      | Mục đích                                  |
|-------------------------------|-----------|------------------------------------------|
| EC2-B SG → EC2-A SG           | 9102/tcp  | Prometheus scrape Airflow statsd-exporter |
| EC2-A SG, EC2-B SG → EFS SG   | 2049/tcp  | NFS tới các mount target EFS             |
| Your IP → EC2-A SG            | 8080/tcp  | Airflow UI (giữ private)                |
| Internet / your IP → EC2-B SG | 80, 443   | Caddy (dashboards + Grafana)             |

## 4. File môi trường

Mỗi máy chủ sử dụng `.env` ở gốc repo (sao chép từ [`.env.example`](../.env.example)).
Điền các biến cần thiết cho từng máy chủ.

**EC2-A (điều phối)** — AWS creds, `FERNET_KEY`, `WEBSERVER_SECRET_KEY`,
`AIRFLOW_UID`, `_AIRFLOW_WWW_USER_*`, cùng các target pipeline (Supabase DB, MotherDuck,
Google Sheets) và cài đặt DockerOperator:

```ini
HOST_REPO_PATH=/home/ec2-user/Lakehouse-Lite     # đường dẫn tuyệt đối tới bản sao này
PIPELINE_IMAGE=lakehouse-crawler:latest
# Airflow → sidecar của chính nó qua mạng compose (không đổi):
AIRFLOW_STATSD_EXPORTER_TARGET=statsd-exporter:9102
```

**EC2-B (giám sát)** — `MONITORING_*` (Caddy auth/domain), `SUPABASE_PROJECT_REF`,
`SUPABASE_METRICS_SECRET_KEY`, và target scrape xuyên máy chủ:

```ini
# Chỉ Prometheus tới IP private của EC2-A thay vì tên sidecar local:
AIRFLOW_STATSD_EXPORTER_TARGET=10.0.1.23:9102
```

## 5. Triển khai

**EC2-A (điều phối):**

```bash
cd /home/ec2-user/Lakehouse-Lite
docker build -t lakehouse-crawler:latest .          # image pipeline được DockerOperator sử dụng
mkdir -p src/crawl_layer/temp_data                  # bind-mount crawler
docker compose --project-directory . \
  -f src/orchestration_layer/docker-compose.yaml up -d
```

**EC2-B (giám sát):**

```bash
cd /home/ec2-user/Lakehouse-Lite
docker compose --project-directory . \
  -f src/monitoring_layer/docker-compose.yaml up -d
```

## 6. Xác minh

- **Metrics**: trong Prometheus (`http://<EC2-B>/`, hoặc port-forward 9090) → *Status →
  Targets*, job `airflow` trỏ tới `<EC2-A-ip>:9102` phải ở trạng thái **UP**.
- **Dashboards**: kích hoạt các DAG `generate_bronze_dashboard` / `generate_silver_dashboard`
  trên EC2-A. Chúng ghi `bronze_dashboard.html` / `silver_dashboard.html` vào
  thư mục `reports/` trên EFS.
- **Phục vụ**: mở `https://<MONITORING_DOMAIN>/business/` trên EC2-B — trang đích
  liên kết tới cả hai dashboard (được phục vụ từ cùng các file EFS được EC2-A ghi).

---

### Tại sao lại có thư mục con `reports/`?

Các dashboard trước đây ghi HTML trực tiếp vào `business/`, nơi cũng chứa các module generator `.py`. Mount EFS lên `business/` sẽ che khuất các module đó bên trong container điều phối. Đầu ra hiện tại đi vào `business/reports/`, vì vậy EFS chỉ hỗ trợ HTML được tạo — bind mount trong
[`_dag_factory.py`](../src/orchestration_layer/dags/_dag_factory.py) và mount nginx trong
[`monitoring docker-compose.yaml`](../src/monitoring_layer/docker-compose.yaml) đều được
phạm vi hóa tới thư mục con đó. Điều này cũng đúng trên một máy chủ đơn (cả hai file compose đều phân giải tới cùng đường dẫn local `reports/`).
