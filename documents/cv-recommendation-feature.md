# Tài liệu triển khai: Gợi ý việc làm theo CV (CV-based Job Recommendation)

> Trạng thái: Bản thiết kế chuẩn bị triển khai (pre-implementation design).
> Phạm vi: 2 repo — `Lakehouse-Lite` (sản xuất dữ liệu + index) và `JobSearchWeb` (phục vụ + parse CV).
> Mục đích: đồ án tốt nghiệp, ưu tiên độ liên quan của gợi ý, real-time, ẩn danh.

---

## 1. Mục tiêu & nguyên tắc

- **Mục tiêu nghiệp vụ:** người dùng upload CV (PDF/DOCX) → nhận **Top 10** việc làm phù hợp nhất, kèm **giải thích lý do** (skill trùng, % phù hợp).
- **Tiêu chí thành công:** độ liên quan của kết quả (relevance), giải thích được.
- **Nguyên tắc kiến trúc:**
  - Tái sử dụng tối đa pipeline có sẵn — đặc biệt là bộ trích xuất kỹ năng taxonomy (`HybridKeywordExtractor`).
  - Tách bạch: sản xuất dữ liệu (Lakehouse-Lite) vs phục vụ (JobSearchWeb).
  - Không phá vỡ đường đọc hiện tại của web (bảng `public.ready_jobs` giữ nguyên).

---

## 2. Các quyết định đã chốt (BA sign-off)

| Mã | Hạng mục | Quyết định |
|----|----------|-----------|
| A1 | Bối cảnh | Đồ án tốt nghiệp |
| A2 | Thành công | Độ liên quan của gợi ý |
| B1 | Auth | Không cần đăng nhập (ẩn danh) |
| B2 | Luồng | Real-time |
| B3 | Phiên | Mỗi lần upload là 1 phiên độc lập (không lưu lịch sử, không job-alert) |
| C1 | Định dạng CV | PDF + DOCX (không OCR) |
| C2 | Ngôn ngữ | Cả tiếng Anh và tiếng Việt |
| C3 | Giới hạn | <= 5MB, <= 5 trang |
| D1 | Thuật toán | Hybrid (vector + skill-overlap taxonomy) |
| D2 | Filter cứng | Location + Experience |
| D3 | Kết quả | Top 10, có giải thích |
| E1/J3 | Embedding | API trả phí → chốt **Google `gemini-embedding-001`** (free tier, đa ngôn ngữ tốt) |
| E2 | PII gửi bên thứ 3 | Không ràng buộc |
| F1/J2 | Vector DB | **Qdrant Cloud free tier** (managed) |
| F2 | Quy mô | Hàng nghìn job |
| F3 | Tần suất index | Hằng ngày |
| G1 | Nguồn dữ liệu | Silver parquet trên S3 |
| G2 | Text embed job | `clean_job_title` + `requirements_cleaned` + `job_description_cleaned` |
| G3/M2 | Payload Qdrant | job_url, title, company, location, salary, exp, source_site, require_* |
| H1 | Frontend | React 18 + Vite + Tailwind + TanStack Query + Axios (repo JobSearchWeb) |
| H2 | API | Thêm endpoint vào FastAPI backend sẵn có (KHÔNG dựng service mới) |
| K2 | Taxonomy | Load từ **Google Sheets** (đã đẩy sẵn) + fallback CSV |
| K3 | Scoring | `final = 0.6*cosine + 0.4*skill_overlap` |
| L1 | Location/Exp | **Người dùng chọn trên UI** (không parse từ CV) |
| M1 | Index op | Upsert hằng ngày theo `job_url`; xóa job hết hạn |
| N1 | Lưu CV | S3, lifecycle tự xóa sau 30 ngày |
| N2 | Rate-limit | Có, chặt hơn endpoint `/jobs` |

---

## 3. Kiến trúc tổng thể

```diagram
 LAKEHOUSE-LITE (sản xuất + index)              JOBSEARCHWEB (phục vụ)
╭──────────────────────────────╮            ╭───────────────────────────────────╮
│ Silver parquet (S3)          │            │ React UI                          │
│  ├─ require_* skills          │            │  - upload CV (PDF/DOCX)           │
│  └─ text cleaned              │            │  - chọn location + số năm KN      │
│            │                  │            │            │ POST /recommend      │
│  Airflow DAG: index_qdrant    │            │            ▼                      │
│  (DockerOperator, hằng ngày)  │            │ FastAPI backend                   │
│   1. đọc Silver (reader.py)   │            │  1. parse CV (pdfplumber/docx)    │
│   2. build text               │            │  2. clean text                    │
│   3. embed (Gemini, batch) ───┼──┐         │  3. extract skills (taxonomy GSheet)│
│   4. upsert + xóa hết hạn      │  │         │  4. embed CV (Gemini, query)      │
╰────────────┬──────────────────╯  │         │  5. query Qdrant (filter+vector)  │
             │                      │         │  6. re-rank hybrid → Top 10       │
             ▼                      └────────▶│  7. lưu CV → S3 (30d)             │
   ╭─────────────────────────╮  Qdrant Cloud  ╰─────────────┬─────────────────────╯
   │ Qdrant collection        │◀───── query ────────────────╯
   │  vector(768) + payload    │
   ╰─────────────────────────╯
```

**Điểm mấu chốt:** mọi trường cần để lọc/giải thích đều nằm trong **payload của Qdrant** (được nạp khi index từ Silver). Nhờ đó backend serving không phải đổi schema `ready_jobs`, không phải đọc thêm Supabase khi gợi ý.

---

## 4. Hợp đồng dùng chung giữa 2 repo (SHARED CONTRACT)

> 3 hằng số này **phải khớp tuyệt đối** giữa Lakehouse-Lite (index) và JobSearchWeb (query). Sai một cái là kết quả vô nghĩa.

| Hằng số | Giá trị |
|---------|---------|
| Embedding model | `gemini-embedding-001` |
| Vector dimension | `768` (cắt bằng MRL `output_dimensionality=768`, **L2-normalize** sau khi cắt) |
| Distance metric | `Cosine` |
| Qdrant collection name | `jobs_v1` |
| Task type khi embed JOB | `RETRIEVAL_DOCUMENT` |
| Task type khi embed CV | `RETRIEVAL_QUERY` |

Nên đặt 3 hằng số đầu vào biến môi trường dùng chung (`EMBEDDING_MODEL`, `EMBEDDING_DIM`, `QDRANT_COLLECTION`) để cả 2 repo đọc cùng một nguồn.

---

## 5. Thiết kế Qdrant collection

```text
Collection: jobs_v1
  vectors:
    size: 768
    distance: Cosine
  point id: UUID5(namespace=DNS, name=job_url)   # idempotent upsert theo job_url
  payload:
    job_url            : keyword
    job_title          : text
    company_name       : keyword
    clean_location     : keyword         # cho filter location
    is_vietnam         : bool
    min_exp_level      : float           # cho filter experience
    max_exp_level      : float
    min_monthly_salary : float
    max_monthly_salary : float
    source_site        : keyword
    deadline_ts        : integer         # epoch giây; lọc job hết hạn ở query-time
    require_programming_languages : keyword[]
    require_frameworks            : keyword[]
    require_tools                 : keyword[]
    require_cloud_skills          : keyword[]
    require_knowledge             : keyword[]
    require_domain_knowledge      : keyword[]
    require_foreign_languages     : keyword[]
```

Tạo **payload index** cho `clean_location`, `min_exp_level`, `deadline_ts` để filter nhanh.

---

## 6. PHẦN A — Indexing pipeline (repo Lakehouse-Lite)

### 6.1. Module mới (theo đúng convention `storage_layer`, import tuyệt đối `src.*`, chạy bằng `python -m`)

```text
src/storage_layer/Qdrant/
  config.py                       # QDRANT_URL/API_KEY, COLLECTION, DIM, GEMINI_API_KEY, model name
  client.py                       # get_qdrant_client() -> QdrantClient (single factory)
  schema/data_class.py            # QdrantJobPayload dataclass = single source of truth của payload
  scripts/embed.py                # embed_documents(texts) -> vectors (Gemini, batch + backoff)
  scripts/index_silver_to_qdrant.py   # main: đọc Silver -> embed -> upsert -> xóa hết hạn
```

### 6.2. Luồng `index_silver_to_qdrant.py`

1. Tạo collection nếu chưa có (idempotent), tạo payload index.
2. Lặp theo `SITES` (giống `load_silver_to_supabase`), đọc Silver bằng `get_jobs_silver_by_site(site, SILVER_ENTITY_NAME, from_date, to_date)` (mặc định: ngày hôm nay; có thể nhận `--from_date/--to_date`).
3. Với mỗi job, ghép text:
   `text = f"{clean_job_title}\n{requirements_cleaned}\n{job_description_cleaned}"`.
4. **Embed theo batch** (Gemini cho phép nhiều input/lần). Dùng `task_type=RETRIEVAL_DOCUMENT`, `output_dimensionality=768`, sau đó **L2-normalize**.
5. Dựng `PointStruct(id=uuid5(job_url), vector=v, payload=QdrantJobPayload(...))`.
6. `client.upsert(collection, points)` theo lô (vd 100 điểm/lần).
7. **Xóa job hết hạn:** `client.delete(collection, filter=deadline_ts < now)` (hoặc lọc ở query-time — xem 7.4).

### 6.3. Xử lý rate-limit free tier (quan trọng)

- Gom batch lớn nhất có thể; thêm retry + exponential backoff khi gặp `429`.
- Vài nghìn job/ngày nằm trong free tier nếu chạy tuần tự có nghỉ. Nếu vượt: bật paid tier (chi phí < $1/tháng ở quy mô này).

### 6.4. Airflow DAG

- File mới `src/orchestration_layer/dags/index_qdrant.py`, dùng **DockerOperator** chạy `python -m src.storage_layer.Qdrant.scripts.index_silver_to_qdrant` trong container `lakehouse-crawler` (đúng pattern hiện tại — DAG chỉ orchestrate).
- Lịch: **hằng ngày**, đặt sau khi Silver hoàn tất (nối downstream của DAG silver hoặc lịch trễ hơn).
- Params override qua UI: `{"from_date": "...", "to_date": "..."}`.

### 6.5. Dependencies thêm (requirements Lakehouse-Lite)

`qdrant-client`, `google-genai` (SDK Gemini).

### 6.6. Biến môi trường thêm (`.env` Lakehouse-Lite)

```
QDRANT_URL=https://<cluster>.qdrant.io:6333
QDRANT_API_KEY=...
QDRANT_COLLECTION=jobs_v1
GEMINI_API_KEY=...
EMBEDDING_MODEL=gemini-embedding-001
EMBEDDING_DIM=768
```

---

## 7. PHẦN B — Serving (repo JobSearchWeb, backend FastAPI)

### 7.1. Module mới

```text
backend/app/services/
  cv_parser.py     # đọc PDF (pdfplumber) / DOCX (python-docx) -> text thô
  taxonomy.py      # load taxonomy từ Google Sheets + fallback CSV; build HybridKeywordExtractor
  embedder.py      # embed_query(text) -> vector (Gemini, task_type=RETRIEVAL_QUERY, dim 768, normalize)
  recommender.py   # query Qdrant + re-rank hybrid + dựng giải thích
  cv_storage.py    # upload CV gốc lên S3
backend/app/routers/recommend.py   # POST /api/v1/recommend
```

> `taxonomy.py` **port lại** `HybridKeywordExtractor` + `build_extractor()` từ
> `Lakehouse-Lite/src/storage_layer/MinIO_S3/layer/silver/utils/flashtext_extractor.py`,
> và đọc bảng taxonomy từ **Google Sheets** (tái dùng cách của `google_sheets.py`).
> Phải dùng đúng cột `canonical_en` / `keywords` để CV và job ra **cùng nhãn canonical**.

### 7.2. Endpoint `POST /api/v1/recommend`

- Nhận: `multipart/form-data`
  - `file`: CV (PDF/DOCX, <= 5MB).
  - `locations`: danh sách location user chọn (optional).
  - `years_experience`: số năm KN user nhập (optional).
- Trả: `200` JSON
  ```json
  {
    "results": [
      {
        "job_url": "...", "job_title": "...", "company_name": "...",
        "location": "...", "source_site": "...",
        "score": 0.87,
        "matched_skills": {
          "require_programming_languages": ["python", "sql"],
          "require_frameworks": ["spark"]
        }
      }
    ]
  }
  ```

### 7.3. Luồng xử lý request

1. Validate file (đuôi, size <= 5MB, số trang <= 5).
2. `cv_parser`: trích text thô.
3. Clean text (port tiện ích từ `silver/utils/clean_text.py`).
4. `taxonomy`: trích **CV skills** (8 nhóm `require_*`).
5. `embedder`: embed text CV (1 vector, `RETRIEVAL_QUERY`).
6. `recommender`: query Qdrant (xem 7.4) lấy top N (vd N=50).
7. Re-rank hybrid (xem 7.5) → cắt Top 10.
8. Dựng `matched_skills` = giao giữa CV skills và payload job skills.
9. `cv_storage`: upload CV gốc lên S3 (async/nền, không chặn response).
10. Trả Top 10.

### 7.4. Query Qdrant (filter cứng + vector)

```text
client.search(
  collection = jobs_v1,
  query_vector = cv_vector,
  limit = 50,
  query_filter = Filter(must = [
     # job chưa hết hạn
     FieldCondition(key="deadline_ts", range=Range(gte=now_ts)),
     # location user chọn (nếu có)
     FieldCondition(key="clean_location", match=MatchAny(any=locations)),
     # experience: job yêu cầu tối thiểu <= số năm của ứng viên
     FieldCondition(key="min_exp_level", range=Range(lte=years_experience)),
  ]),
  with_payload = True,
)
```

> Filter location/experience chỉ thêm vào `must` khi user có nhập (giữ recall khi bỏ trống).

### 7.5. Công thức re-rank hybrid

```text
cosine        = điểm similarity Qdrant trả về (đã ở thang [0,1] với Cosine + vector normalized)

skill_overlap = Σ_c  w_c * |CV_c ∩ JOB_c| / max(|JOB_c|, 1)
                ───────────────────────────────────────────
                       Σ_c  w_c  (chỉ tính nhóm c mà JOB có skill)

  trọng số nhóm (w_c) đề xuất:
    programming_languages = 0.30
    frameworks            = 0.25
    tools                 = 0.15
    cloud_skills          = 0.15
    knowledge             = 0.10
    domain_knowledge      = 0.05
    (foreign_languages dùng để hiển thị, không tính điểm)

final = 0.6 * cosine + 0.4 * skill_overlap     # K3
```

Sắp xếp giảm dần theo `final`, lấy Top 10.

### 7.6. Bảo mật & vận hành

- **Rate-limit** riêng cho `/recommend` (slowapi), chặt hơn `/jobs` (vd 5 req/phút/IP) vì có upload + gọi embedding.
- **CORS:** hiện chỉ cho `GET` (`main.py`) → cần bổ sung `POST` cho riêng route recommend.
- **CV PII:** lưu S3 bucket riêng, bật **lifecycle rule tự xóa sau 30 ngày**; không log nội dung CV.
- **Lưu ý free tier Gemini:** dữ liệu có thể được dùng để cải thiện sản phẩm — chấp nhận được theo quyết định E2; ghi rõ trong điều khoản nếu demo công khai.

### 7.7. Dependencies thêm (requirements backend)

`qdrant-client`, `google-genai`, `pdfplumber`, `python-docx`, `boto3`, `flashtext`, `polars` (nếu port taxonomy dùng polars) hoặc thay bằng `pandas`/`csv` cho nhẹ.

### 7.8. Biến môi trường thêm (backend)

```
QDRANT_URL=...
QDRANT_API_KEY=...
QDRANT_COLLECTION=jobs_v1
GEMINI_API_KEY=...
EMBEDDING_MODEL=gemini-embedding-001
EMBEDDING_DIM=768
CV_S3_BUCKET=...
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
GOOGLE_SHEETS_CREDENTIALS_FILE=...
GOOGLE_SHEETS_SPREADSHEET_ID=...
```

---

## 8. PHẦN C — Frontend (React)

- **Component upload:** drag-drop, chấp nhận `.pdf/.docx`, chặn >5MB ở client.
- **Bộ lọc:** dropdown location (tái dùng endpoint locations sẵn có) + ô nhập số năm kinh nghiệm.
- **Gọi API:** TanStack Query `useMutation` → `POST /api/v1/recommend` (Axios, `multipart/form-data`).
- **Hiển thị kết quả:** danh sách Top 10 + **badge % phù hợp** (`score`) + chip các **skill trùng** (`matched_skills`) để giải thích.
- **Trạng thái:** loading (đang phân tích CV), empty (không có kết quả), error (file lỗi/parse fail).

---

## 9. Các bước triển khai (deployment runbook)

1. **Qdrant Cloud:** tạo cluster free tier → lấy `QDRANT_URL` + `QDRANT_API_KEY`.
2. **Gemini:** tạo API key ở Google AI Studio → `GEMINI_API_KEY`.
3. **S3:** tạo bucket lưu CV + đặt lifecycle rule xóa sau 30 ngày.
4. **Lakehouse-Lite:**
   - Thêm deps, tạo module `storage_layer/Qdrant/*`, cập nhật `.env`.
   - Chạy **backfill** lần đầu: `python -m src.storage_layer.Qdrant.scripts.index_silver_to_qdrant --from_date <đầu> --to_date <cuối>`.
   - Bật DAG `index_qdrant` (hằng ngày).
5. **JobSearchWeb backend:**
   - Thêm deps + các service + router, cập nhật `.env` và CORS (cho POST `/recommend`).
   - Cập nhật `docker-compose.yml` (env mới).
6. **Frontend:** thêm trang/Component upload + kết quả; build lại image.
7. **Deploy:** `docker-compose up -d --build` (theo runbook hiện có của JobSearchWeb).

---

## 10. Kiểm thử & nghiệm thu

- **Index:** sau backfill, kiểm tra `collection count` ≈ số job Silver còn hạn.
- **Idempotent:** chạy index 2 lần → số điểm không tăng trùng (nhờ UUID5 theo job_url).
- **API smoke test:** upload 1 CV mẫu Data Engineer → top kết quả phải là job DE/DA hợp lý; CV tiếng Việt và tiếng Anh đều ra kết quả.
- **Filter:** chọn location + số năm KN → kết quả tôn trọng ràng buộc.
- **Latency:** đo end-to-end (mục tiêu nhanh, chấp nhận <= 10s — I2).
- **Giải thích:** mỗi kết quả có `matched_skills` không rỗng (trừ khi thuần semantic).

---

## 11. Rủi ro & giảm thiểu

| Rủi ro | Ảnh hưởng | Giảm thiểu |
|--------|-----------|-----------|
| Sai lệch dimension/model giữa 2 repo | Kết quả vô nghĩa | Khóa SHARED CONTRACT (mục 4) qua env dùng chung |
| Rate-limit Gemini free tier | Index/định kỳ chậm/lỗi | Batch + backoff; fallback paid (<$1/tháng) |
| Parse CV thất bại (PDF scan/ảnh) | Không trích được text | Validate sớm, báo lỗi rõ ràng; ngoài phạm vi OCR (C1) |
| Taxonomy phủ thiếu kỹ năng trong CV | skill_overlap thấp | Vector (cosine) bù lại; bổ sung seed taxonomy dần |
| Cold start (ít job trong Qdrant) | Gợi ý nghèo | Backfill đủ dải ngày trước khi demo |
| CV chứa PII | Rủi ro bảo mật | S3 riêng + lifecycle 30 ngày + không log nội dung |
| `min_exp_level` null ở job | Lọt/loại sai khi filter exp | Coi null là "không yêu cầu" → không loại |

---

## 12. Lộ trình (milestones)

1. **M1 — Hạ tầng:** tạo Qdrant Cloud, Gemini key, S3 bucket + lifecycle, chốt SHARED CONTRACT.
2. **M2 — Index:** module Qdrant + backfill + DAG hằng ngày (Lakehouse-Lite).
3. **M3 — Serving:** endpoint `/recommend` + parse CV + taxonomy + embed + re-rank (JobSearchWeb backend).
4. **M4 — Frontend:** trang upload + bộ lọc + hiển thị kết quả/giải thích.
5. **M5 — Nghiệm thu:** kiểm thử relevance, latency, bảo mật; tinh chỉnh trọng số `w_c`.
