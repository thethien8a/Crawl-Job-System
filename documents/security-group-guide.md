# Hướng dẫn cấu hình Security Group trên AWS Console

## Thông tin EC2-A đã lấy được

| Thông tin | Giá trị |
|-----------|---------|
| Instance ID | `i-045e1605d85060dee` |
| Security Group | `launch-wizard-1` (`sg-0f977c79629039d28`) |
| VPC | `vpc-00edcbd0600bcdf81` |
| AZ | `ap-southeast-1b` |
| EFS File System | `fs-04c7c873495026a30` |

---

## Việc 1: Cho phép EC2-B scrape metrics port 9102 từ EC2-A

> Mục đích: Prometheus trên EC2-B cần truy cập port `9102` trên EC2-A để lấy Airflow metrics từ `statsd-exporter`.

### Bước 1 — Mở Security Group của EC2-A

1. Vào **AWS Console** → Tìm **EC2** trong thanh search → Click **EC2**
2. Menu bên trái → Click **Security Groups** (nằm trong mục **Network & Security**)
3. Tìm Security Group tên **`launch-wizard-1`** (ID: `sg-0f977c79629039d28`) → Click vào nó

### Bước 2 — Thêm Inbound Rule cho port 9102

1. Click tab **Inbound rules** ở phía dưới
2. Click nút **Edit inbound rules**
3. Click **Add rule** và điền:

| Field | Giá trị |
|-------|---------|
| **Type** | Custom TCP |
| **Port range** | `9102` |
| **Source** | Chọn **Custom**, rồi nhập **Security Group ID của EC2-B** (ví dụ `sg-xxxxxx`) |
| **Description** | `Prometheus scrape statsd-exporter from EC2-B` |

> **Cách tìm SG của EC2-B**: Vào **EC2 → Instances** → Click vào instance EC2-B → Tab **Security** → Ghi lại Security Group ID.
>
> **Nếu không nhớ SG của EC2-B**, bạn có thể tạm thời chọn Source là **Private IP của EC2-B** kèm `/32` (ví dụ `172.31.xx.xx/32`). Cách này cũng hoạt động nhưng kém linh hoạt hơn khi IP thay đổi.

4. Click **Save rules**

---

## Việc 2: Cấu hình Security Group cho EFS (port 2049)

> Mục đích: Cả EC2-A và EC2-B đều cần mount EFS qua NFS (port 2049) để truy cập thư mục `reports/`.

### Bước 1 — Tìm Security Group của EFS

1. Vào **AWS Console** → Tìm **EFS** trong thanh search → Click **EFS**
2. Click vào File system **`fs-04c7c873495026a30`**
3. Click tab **Network**
4. Bạn sẽ thấy **Mount targets** — mỗi mount target có một **Security Group** gắn kèm
5. Ghi lại **Security Group ID** của mount target (ví dụ `sg-yyyyyy`)

> **Lưu ý quan trọng**: Nếu chưa có mount target nào trong AZ `ap-southeast-1b` (AZ của EC2-A), bạn cần tạo:
> - Click **Manage** ở tab Network
> - Click **Add mount target**
> - Chọn AZ: `ap-southeast-1b`, Subnet: `subnet-0660f02351a88eed4`
> - Chọn hoặc tạo Security Group cho EFS
> - Click **Save**
>
> Tương tự cho AZ của EC2-B nếu khác AZ.

### Bước 2 — Thêm Inbound Rule cho EFS Security Group

1. Quay lại **EC2 → Security Groups**
2. Tìm **Security Group của EFS** (ID bạn ghi ở Bước 1)
3. Click tab **Inbound rules** → **Edit inbound rules**
4. Thêm **2 rules**:

**Rule 1 — Cho EC2-A mount:**

| Field | Giá trị |
|-------|---------|
| **Type** | NFS |
| **Port range** | `2049` (tự điền) |
| **Source** | Custom → `sg-0f977c79629039d28` (SG của EC2-A) |
| **Description** | `NFS mount from EC2-A` |

**Rule 2 — Cho EC2-B mount:**

| Field | Giá trị |
|-------|---------|
| **Type** | NFS |
| **Port range** | `2049` (tự điền) |
| **Source** | Custom → Security Group ID của EC2-B |
| **Description** | `NFS mount from EC2-B` |

5. Click **Save rules**

---

## Việc 3: Sau khi config SG xong — Mount EFS

Khi đã config xong cả 2 việc trên, chạy lệnh mount EFS trên EC2-A:

```bash
sudo mkdir -p /home/ubuntu/Lakehouse-Lite/src/monitoring_layer/business/reports

sudo mount -t nfs4 \
  -o nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2,noresvport \
  fs-04c7c873495026a30.efs.ap-southeast-1.amazonaws.com:/ \
  /home/ubuntu/Lakehouse-Lite/src/monitoring_layer/business/reports
```

Rồi thêm vào `/etc/fstab` để persist qua reboot:

```
fs-04c7c873495026a30.efs.ap-southeast-1.amazonaws.com:/ /home/ubuntu/Lakehouse-Lite/src/monitoring_layer/business/reports nfs4 nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2,noresvport,_netdev 0 0
```

---

## Tóm tắt các Security Group rules cần thêm

| SG cần sửa | Rule | Source | Port |
|------------|------|--------|------|
| **EC2-A SG** (`sg-0f977c79629039d28`) | Inbound | EC2-B SG | 9102/tcp |
| **EFS SG** | Inbound | EC2-A SG (`sg-0f977c79629039d28`) | 2049 (NFS) |
| **EFS SG** | Inbound | EC2-B SG | 2049 (NFS) |
