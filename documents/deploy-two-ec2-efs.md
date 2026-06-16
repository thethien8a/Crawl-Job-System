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

## 2. Gợi ý cấu hình EC2 tiết kiệm nhưng vẫn ổn định

Mục tiêu của phần này là giữ chi phí thấp cho môi trường sinh viên/demo, nhưng vẫn đủ tài nguyên
để hệ thống chạy ổn khi crawl, làm sạch dữ liệu, tạo dashboard và xem monitoring.

### 2.1. Tóm tắt khuyến nghị

| Máy | Cấu hình khuyến nghị | Cấu hình tối thiểu nên dùng | Ổ đĩa gốc | Lý do |
|-----|----------------------|-----------------------------|-----------|-------|
| **EC2-A — Điều phối** | 2 vCPU / 4 GiB RAM | 2 vCPU / 2 GiB RAM + swap | 40–60 GiB gp3 | Đây là máy nặng nhất: chạy Airflow, Postgres, statsd-exporter và các container pipeline có Chrome/Xvfb. |
| **EC2-B — Giám sát** | 2 vCPU / 4 GiB RAM | 1–2 vCPU / 2 GiB RAM | 25–30 GiB gp3 | Chỉ chạy Caddy, Prometheus, Grafana và nginx; nhẹ hơn EC2-A nhiều. |
| **EFS** | Standard Regional, dung lượng nhỏ | Chỉ lưu HTML dashboard | Không dùng cho Docker/Postgres | EFS chỉ nên chứa `reports/`; không lưu log, database hay dữ liệu crawl thô. |

Nếu tài khoản AWS của bạn còn credit Free Tier mới và giao diện cho phép chọn `c7i-flex.large`,
cấu hình này dùng được cho **EC2-B**. Tuy nhiên không nên để chạy liên tục nếu không cần demo,
vì khi hết credit/Free Tier thì chi phí on-demand sẽ cao. Với EC2-B, 60 GiB gp3 là dư; nên giảm
xuống 25–30 GiB để tránh tốn tiền EBS không cần thiết.

### 2.2. Vì sao EC2-A cần nhiều tài nguyên hơn EC2-B?

EC2-A chạy stack điều phối trong [`src/orchestration_layer/docker-compose.yaml`](../src/orchestration_layer/docker-compose.yaml):

- Airflow apiserver và scheduler dùng `LocalExecutor`.
- Postgres là metadata database của Airflow.
- statsd-exporter xuất metrics cho Prometheus scrape từ EC2-B.
- Các DAG không chạy logic trực tiếp trong Airflow; chúng khởi chạy container pipeline qua
  [`DockerOperator`](../src/orchestration_layer/dags/_dag_factory.py).

Các workload trong DAG factory gồm:

| Workload | Lịch chạy hiện tại | Mức nặng |
|----------|--------------------|----------|
| Crawl từng site | 3 giờ/lần | Nặng nhất với ITviec/VietnamWorks vì dùng browser. |
| Validate + Bronze upload | Sau crawl | Nhẹ đến trung bình. |
| Silver cleaning | 8 giờ/lần | Trung bình đến nặng nếu date range lớn. |
| Supabase load + MotherDuck Gold | 6 giờ/lần | Chủ yếu network/IO. |
| Bronze/Silver dashboard generation | Hằng ngày | Nhẹ đến trung bình. |

Đặc biệt, image pipeline trong [`Dockerfile`](../Dockerfile) cài Chrome và Xvfb. Trong
[`_dag_factory.py`](../src/orchestration_layer/dags/_dag_factory.py), container pipeline còn được cấu hình
`shm_size=2GB` để Chrome chạy ổn hơn. Vì vậy EC2-A không nên quá nhỏ nếu muốn chạy browser crawler.

### 2.3. Vì sao EC2-B có thể nhỏ hơn?

EC2-B chỉ chạy monitoring stack trong [`src/monitoring_layer/docker-compose.yaml`](../src/monitoring_layer/docker-compose.yaml):

- Caddy mở cổng 80/443, xác thực Basic Auth và reverse proxy.
- Prometheus scrape chính nó, Airflow statsd-exporter và Supabase metrics.
- Grafana đọc Prometheus để hiển thị dashboard.
- nginx phục vụ HTML dashboard từ EFS.

Trong [`prometheus.yml`](../src/monitoring_layer/prometheus/prometheus.yml), scrape interval mặc định là 15s
cho Prometheus/Airflow và 60s cho Supabase, số target ít. Vì vậy EC2-B không cần CPU mạnh, nhưng nên có
2–4 GiB RAM để Grafana và Prometheus không bị swap khi demo.

### 2.4. Cấu hình nên chọn trên AWS Console

**EC2-A — Điều phối:**

- Instance type: ưu tiên `c7i-flex.large` hoặc `t3.small` nếu còn credit/Free Tier phù hợp.
- Nếu bắt buộc tiết kiệm hơn: `t3.micro` chỉ nên dùng để test rất nhỏ, thêm 2–4 GiB swap và chỉ chạy
  `--max-pages 1`.
- Root EBS: 40–60 GiB gp3 vì máy này cần chứa Docker image pipeline, Airflow logs, Postgres volume
  và dữ liệu temp trước khi Bronze upload.
- Security Group: chỉ mở 8080 cho IP cá nhân nếu cần Airflow UI; mở 9102 từ EC2-B SG để Prometheus scrape.

**EC2-B — Giám sát:**

- Instance type: `t3.small` là cân bằng tốt; `c7i-flex.large` dùng được nếu credit đang cover và bạn chấp nhận
  tốn thêm khi hết credit; `t3.micro` chỉ phù hợp demo nhẹ.
- Root EBS: 25–30 GiB gp3. Không cần 60 GiB nếu Prometheus retention ngắn.
- Security Group: mở 80/443 từ Internet hoặc IP cá nhân; SSH 22 chỉ mở từ IP cá nhân, không để `0.0.0.0/0`.
- Prometheus retention: nên giữ 3–7 ngày cho demo/sinh viên để tiết kiệm disk.

### 2.5. Quy tắc vận hành để không đội chi phí

- Không chạy nhiều browser crawler cùng lúc. Với cấu hình tiết kiệm, chỉ nên có 1 task nặng chạy tại một thời điểm.
- Khi test, giữ `max_pages` thấp trước: 1–2 pages; tăng dần nếu thấy CPU/RAM còn dư.
- Không để EC2-A và EC2-B chạy 24/7 nếu không cần. Stop instance sau khi demo/test xong.
- EFS chỉ mount vào thư mục `reports/`; không đưa Postgres, Docker volumes, Airflow logs hoặc dữ liệu crawl thô lên EFS.
- Giảm dung lượng EBS của EC2-B xuống 25–30 GiB; chỉ EC2-A mới nên dùng 40–60 GiB.

Khuyến nghị thực tế cho đồ án/demo: **EC2-A 2 vCPU / 4 GiB RAM / 50 GiB gp3** và
**EC2-B 2 vCPU / 4 GiB RAM / 30 GiB gp3**. Cấu hình này không phải rẻ nhất tuyệt đối,
nhưng là mức hợp lý để hệ thống ít lỗi hơn khi chạy browser crawler và monitoring.

## 3. Tạo và mount EFS trên cả hai máy chủ

Thư mục chia sẻ là thư mục đầu ra dashboard. Mount EFS chính xác tại:

```
<repo>/src/monitoring_layer/business/reports
```

> Lưu ý: lệnh `mount` bên dưới **không tự tạo EFS**. Bạn phải tạo EFS một lần
> trong AWS trước, lấy được File system ID dạng `fs-...`, rồi mới chạy lệnh mount
> trên cả EC2-A và EC2-B.

### 3.1. Tạo EFS trên AWS Console

Thực hiện một lần trong AWS Console:

1. Vào **Amazon EFS → Create file system**.
2. Chọn đúng **VPC** đang chứa cả EC2-A và EC2-B.
3. Với môi trường sinh viên/demo, giữ mặc định đơn giản:
   - **File system type**: Regional.
   - **Throughput mode**: Elastic hoặc Bursting mặc định đều được cho tải nhỏ.
   - **Storage class**: Standard. Nếu tài khoản còn Free Tier EFS, chỉ lưu dashboard HTML
     để giữ dung lượng rất nhỏ.
4. Tạo hoặc kiểm tra **mount target** trong subnet mà hai EC2 có thể truy cập.
   Cách dễ nhất là đặt EC2-A, EC2-B và EFS mount target trong cùng VPC, cùng AZ/subnet
   khi làm demo.
5. Gán Security Group cho EFS và mở inbound:

| From                 | To     | Port     | Mục đích             |
|----------------------|--------|----------|----------------------|
| EC2-A Security Group | EFS SG | 2049/tcp | Mount NFS từ EC2-A   |
| EC2-B Security Group | EFS SG | 2049/tcp | Mount NFS từ EC2-B   |

Sau khi tạo xong, copy **File system ID** của EFS, ví dụ `fs-0123456789abcdef0`,
rồi thay vào các lệnh mount bên dưới.

### 3.2. Mount EFS lên cả hai EC2

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

## 4. Security Groups

| From → To                     | Port      | Mục đích                                  |
|-------------------------------|-----------|------------------------------------------|
| EC2-B SG → EC2-A SG           | 9102/tcp  | Prometheus scrape Airflow statsd-exporter |
| EC2-A SG, EC2-B SG → EFS SG   | 2049/tcp  | NFS tới các mount target EFS             |
| Your IP → EC2-A SG            | 8080/tcp  | Airflow UI (giữ private)                |
| Internet / your IP → EC2-B SG | 80, 443   | Caddy (dashboards + Grafana)             |

## 5. File môi trường

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

## 6. Triển khai

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

## 7. Xác minh

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
