# Tài liệu thiết kế hệ thống — Lakehouse-Lite

> Phạm vi tài liệu: mô tả phương án thiết kế hệ thống Lakehouse-Lite trong giai đoạn trước triển khai để đưa vào báo cáo đồ án tốt nghiệp. Tài liệu trình bày kiến trúc logic, thiết kế dữ liệu, thiết kế module, thiết kế triển khai, cơ chế vận hành, bảo mật, giám sát và khả năng mở rộng.

---

## 1. Mục tiêu thiết kế

Thiết kế của Lakehouse-Lite hướng tới việc xây dựng một pipeline dữ liệu tuyển dụng có tính module hóa, có thể mở rộng nguồn dữ liệu, lưu trữ được cả dữ liệu thô và dữ liệu đã xử lý, đồng thời phục vụ được nhiều mục đích downstream như tìm kiếm việc làm, phân tích BI và giám sát chất lượng dữ liệu.

Các mục tiêu thiết kế chính:

1. Tách rõ các tầng thu thập, lưu trữ, xử lý, phục vụ, điều phối và giám sát.
2. Bảo toàn dữ liệu thô để có thể xử lý lại khi logic cleaning thay đổi.
3. Chuẩn hóa dữ liệu tuyển dụng từ nhiều website về một schema thống nhất.
4. Tối ưu dữ liệu đã làm sạch cho cả workload OLTP và OLAP.
5. Đảm bảo pipeline có thể chạy tự động, có thể rerun và hạn chế tạo dữ liệu trùng.
6. Giảm coupling giữa Airflow và logic xử lý dữ liệu.
7. Dễ bổ sung source, field, taxonomy hoặc downstream target mới.
8. Phục vụ được gợi ý việc làm cá nhân hóa theo CV dựa trên vector search và kỹ năng taxonomy.

---

## 2. Nguyên tắc thiết kế

| Nguyên tắc | Áp dụng trong hệ thống |
|------------|------------------------|
| Separation of Concerns | Crawl, storage, cleaning, serving, orchestration và monitoring nằm ở các thư mục/tầng riêng |
| Medallion Architecture | Dữ liệu đi qua Bronze → Silver → Gold theo mức độ xử lý tăng dần |
| Single Source of Truth cho schema | Silver schema sinh từ SilverJobItem, Supabase schema sinh từ JobData, Gold fields sinh từ GoldJobItem |
| Idempotent Load | Supabase UPSERT, Gold full refresh, Silver reader chọn latest object theo ngày |
| Config qua environment | Credential và thông tin deploy lấy từ .env |
| Orchestration không chứa business logic | Airflow chỉ gọi command trong DockerOperator |
| Progressive enrichment | Bronze giữ raw, Silver làm sạch/enrich, Gold reshape cho BI |
| Source-specific + common pipeline | Mỗi website có cleaning riêng nhưng tái sử dụng common cleaners |
| Retrieval-Augmented Serving | Silver được index sang Qdrant để backend gợi ý CV truy vấn theo vector và metadata filter |

---

## 3. Kiến trúc tổng thể

### 3.1. Kiến trúc logic

```text
┌──────────────────────────────────────────────────────────┐
│                    External Job Sources                  │
│          TopCV | ITviec | VietnamWorks                   │
└─────────────────────────────┬────────────────────────────┘
                              │
┌─────────────────────────────▼────────────────────────────┐
│                       Crawl Layer                        │
│  HTTP crawler for TopCV | Browser crawlers for ITviec/VW │
│  Parser per source | Dedup URL | Save to local JSONL     │
└─────────────────────────────┬────────────────────────────┘
                              │
┌─────────────────────────────▼────────────────────────────┐
│                  Local Temp + Validation                 │
│       JSONL staging | Great Expectations validation      │
└─────────────────────────────┬────────────────────────────┘
                              │
┌─────────────────────────────▼────────────────────────────┐
│                     Bronze Layer on S3                   │
│      Raw JSONL.GZ partitioned by source/year/month/day   │
└─────────────────────────────┬────────────────────────────┘
                              │
┌─────────────────────────────▼────────────────────────────┐
│                     Silver Processing                    │
│ Polars cleaning | taxonomy extraction | schema enforcing │
└─────────────────────────────┬────────────────────────────┘
                              │
┌─────────────────────────────▼────────────────────────────┐
│                     Silver Layer on S3                   │
│     Clean Parquet partitioned by source_site/date        │
└───────────────┬─────────────────────┬────────────────┬────┘
                │                     │                │
┌───────────────▼──────────────┐ ┌────▼────────────┐ ┌─▼────────────────┐
│ Supabase Serving PostgreSQL  │ │ Qdrant Vector DB │ │ MotherDuck Gold  │
│ ready_jobs for frontend      │ │ jobs_v1          │ │ star schema BI   │
└──────────────────────────────┘ └────┬────────────┘ └────────┬─────────┘
                                      │                       │
                         ┌────────────▼────────────┐ ┌────────▼─────────┐
                         │ FastAPI CV Recommend    │ │ Power BI Reports │
                         │ Gemini + hybrid rerank  │ └──────────────────┘
                         └────────────┬────────────┘
                                      │
                         ┌────────────▼────────────┐
                         │ React upload CV UI      │
                         └─────────────────────────┘

Airflow + DockerOperator điều phối toàn bộ pipeline.
Prometheus + Grafana + Caddy + Nginx giám sát vận hành và dashboard nghiệp vụ.
```

### 3.2. Kiến trúc vật lý dự kiến triển khai

| Nhóm container/dịch vụ | Thành phần | Vai trò |
|------------------------|------------|--------|
| Pipeline container | lakehouse-crawler/lakehouse-pipeline image | Chạy crawler, validation, Bronze, Silver, Supabase, Gold command |
| Airflow stack | Airflow webserver, scheduler, Postgres, StatsD Exporter | Lập lịch, quản lý DAG, lưu metadata Airflow, phát metrics |
| Monitoring stack | Prometheus, Grafana, Caddy, Nginx dashboards | Scrape metrics, hiển thị dashboard, reverse proxy/auth |
| Recommendation stack | Qdrant Cloud, Gemini Embedding, FastAPI, React | Index vector job, nhận CV, tính gợi ý và hiển thị Top 10 |
| Cloud services | AWS S3, Supabase, MotherDuck, Qdrant Cloud, Google Gemini | Lưu Bronze/Silver/CV, phục vụ OLTP, phục vụ OLAP, phục vụ vector search/embedding |

Phương án triển khai vật lý được lựa chọn là tách hạ tầng thành hai EC2 và một EFS dùng chung:

| Máy chủ | Thành phần chạy chính | Vai trò |
|--------|------------------------|--------|
| EC2-A — Điều phối | Airflow apiserver/scheduler, Postgres metadata, StatsD Exporter, DockerOperator/lakehouse-crawler | Chạy toàn bộ pipeline crawl → Bronze → Silver → Supabase → Qdrant → Gold và sinh dashboard HTML |
| EC2-B — Giám sát | Caddy, Prometheus, Grafana, Nginx dashboards | Scrape metrics từ EC2-A, hiển thị dashboard vận hành và phục vụ dashboard HTML nghiệp vụ |
| Amazon EFS | Mount tại src/monitoring_layer/business/reports trên cả hai EC2 | Chia sẻ file report HTML do EC2-A sinh để EC2-B phục vụ qua Nginx/Caddy |

---

## 4. Thiết kế tầng Crawl

### 4.1. Cấu trúc module

Tầng Crawl nằm trong [crawl_layer](../src/crawl_layer). Mỗi website có module riêng:

| Source | Module | Chiến lược crawl |
|--------|--------|------------------|
| TopCV | [topcv](../src/crawl_layer/crawler/topcv) | HTTP async, fetch search pages và detail pages |
| ITviec | [itviec](../src/crawl_layer/crawler/itviec) | Browser automation bằng nodriver, login, parse side panel |
| VietnamWorks | [vietnamworks](../src/crawl_layer/crawler/vietnamworks) | Browser automation bằng nodriver, render JS, lấy job URL/detail |

Mỗi source được chia thành các thành phần:

| Thành phần | Vai trò |
|------------|--------|
| __main__.py | CLI entrypoint, nhận keyword/max-pages/headless |
| crawler.py | Điều phối crawl cấp cao, pagination, dedup, flush batch |
| parser.py | Parse HTML thành JobItem tương ứng |
| browser.py | Quản lý browser session cho nguồn cần trình duyệt |
| http_client.py | Quản lý HTTP session/retry/backoff cho TopCV |
| config.py | Hằng số source, URL, entity |
| utils.py | Hàm encode keyword/slug và tiện ích nhỏ |

### 4.2. Thiết kế dữ liệu đầu ra của Crawl

Dữ liệu crawl được biểu diễn bằng dataclass trong [data_class.py](../src/crawl_layer/data_model/data_class.py):

| Dataclass | Vai trò |
|----------|--------|
| JobItem | Trường chung cho mọi source |
| TopCVJobItem | Mở rộng JobItem với company_size, job_type, experience_level, education_level, job_position, job_deadline |
| ITViecJobItem | Mở rộng JobItem với company_size |
| VietnamWorksJobItem | Mở rộng JobItem với job_type, experience_level, education_level, job_position, job_deadline |

### 4.3. Thiết kế lưu tạm

Crawler không upload trực tiếp lên S3 mà ghi vào local temp thông qua save_to_temp trong [loader.py](../src/crawl_layer/utils/loader.py). Thiết kế này có các ưu điểm:

- Tách crawl khỏi storage upload.
- Có điểm kiểm tra dữ liệu trước khi đưa vào Bronze.
- Nếu crawler lỗi giữa chừng, các page đã flush vẫn còn trong temp file.
- Cho phép Airflow tách DAG crawl và DAG validate/bronze.

Quy ước file temp:

```text
src/crawl_layer/temp_data/<source>_jobs_YYYYMMDD.jsonl
```

Ví dụ:

```text
src/crawl_layer/temp_data/topcv_jobs_20260512.jsonl
src/crawl_layer/temp_data/itviec_jobs_20260512.jsonl
src/crawl_layer/temp_data/vietnamworks_jobs_20260512.jsonl
```

---

## 5. Thiết kế tầng Validation và Bronze

### 5.1. Validation local temp

Validation nằm trong [validation](../src/storage_layer/MinIO_S3/layer/local_temp/validation). Mỗi source có một module validation riêng:

| Source | Module validation | Vai trò |
|--------|-------------------|--------|
| TopCV | [topcv_validate.py](../src/storage_layer/MinIO_S3/layer/local_temp/validation/topcv_validate.py) | Kiểm tra file topcv_jobs*.jsonl |
| ITviec | [itviec_validate.py](../src/storage_layer/MinIO_S3/layer/local_temp/validation/itviec_validate.py) | Kiểm tra file itviec_jobs*.jsonl |
| VietnamWorks | [vnworks_validate.py](../src/storage_layer/MinIO_S3/layer/local_temp/validation/vnworks_validate.py) | Kiểm tra file vietnamworks_jobs*.jsonl |

Công cụ validation chính là Great Expectations ở chế độ ephemeral context. Validation đọc JSONL bằng pandas, tạo expectation suite và trả exit code để Airflow quyết định có tiếp tục upload Bronze hay không.

Thiết kế rule:

- Required columns cần đạt tỷ lệ non-null tối thiểu.
- Optional columns có threshold mềm hơn.
- Một số field đặc thù site có rule ngoại lệ.

### 5.2. Bronze upload

Bronze entrypoint nằm tại [main.py](../src/storage_layer/MinIO_S3/layer/bronze/main.py). Module này đọc bucket name từ [bucket.yml](../src/storage_layer/MinIO_S3/config/bucket.yml), sau đó gọi load_to_bronze trong [loader.py](../src/crawl_layer/utils/loader.py).

Quy trình Bronze:

1. Scan file JSONL trong temp_data.
2. Nếu có source_filter thì chỉ xử lý file của source đó.
3. Gzip file JSONL thành JSONL.GZ.
4. Upload lên S3 Bronze theo path chuẩn.
5. Xóa file gzip local.
6. Dọn file JSONL local của source đã upload.

Quy ước S3 key Bronze:

```text
<source>/<entity>/year=YYYY/month=MM/day=DD/<source>_<entity>_YYYYMMDD_HHMMSS.jsonl.gz
```

Ví dụ:

```text
topcv/jobs/year=2026/month=05/day=12/topcv_jobs_20260512_170702.jsonl.gz
```

### 5.3. Thiết kế path abstraction

Đường dẫn S3 được đóng gói trong [path.py](../src/storage_layer/MinIO_S3/config/path.py):

| Class | Vai trò |
|-------|--------|
| BronzeBucketPaths | Sinh prefix và S3 path cho Bronze |
| SilverBucketPaths | Sinh prefix, S3 path và file key cho Silver |

DEFAULT_ENTITY_NAME được đặt là jobs để thống nhất entity trên Bronze, Silver, Gold và Supabase.

---

## 6. Thiết kế tầng Silver

### 6.1. Mục tiêu của Silver

Silver layer chịu trách nhiệm biến dữ liệu thô thành dữ liệu sạch, có cấu trúc và sẵn sàng cho downstream. Đây là tầng quan trọng nhất về mặt chất lượng dữ liệu.

Nhiệm vụ chính:

- Đọc Bronze JSONL.GZ theo source và khoảng ngày.
- Loại các dòng không đủ thông tin tối thiểu.
- Áp dụng cleaning pipeline theo từng site.
- Chuẩn hóa schema cuối cùng về SilverJobItem.
- Ghi Parquet lên S3 Silver.

### 6.2. Shared Silver runner

Pipeline chung nằm trong [pipeline.py](../src/storage_layer/MinIO_S3/layer/silver/cleaning/common/pipeline.py). Thiết kế gồm các hàm chính:

| Thành phần | Vai trò |
|------------|--------|
| build_argument_parser | Chuẩn hóa CLI --from_date, --to_date, --entity_name, --no_save, --export_parquet |
| run_pipeline | Lặp từng ngày, đọc Bronze, filter essential rows, gọi clean_fn, enforce schema, upload Silver |
| filter_essential_rows | Loại dòng thiếu job_title hoặc company_name |
| enforce_silver_schema | Cast/add/drop/reorder columns theo Silver schema |
| main_for_site | Entry-point wrapper dùng chung cho ba site |

Thiết kế này giúp mỗi site chỉ cần định nghĩa hàm clean_<site>_jobs, còn logic đọc/ghi/CLI/schema dùng chung.

### 6.3. Schema Silver

Schema Silver được định nghĩa bằng SilverJobItem trong [data_class.py](../src/storage_layer/MinIO_S3/layer/silver/data_model/data_class.py). Hàm silver_schema_to_polars sinh mapping từ Python type sang Polars type.

Lợi ích:

- Thêm hoặc xóa field tại một nơi.
- Pipeline tự động ép DataFrame về đúng schema.
- Tránh lỗi downstream do thiếu cột hoặc sai thứ tự cột.

Nhóm field chính:

| Nhóm | Ví dụ field |
|------|-------------|
| Metadata | job_url, search_keyword, job_deadline |
| Title | job_title, clean_job_title, job_title_special_keywords |
| Company | clean_company_name, company_size, min_company_size, max_company_size |
| Location | location, clean_location, is_vietnam |
| Industry | job_industry_clean, job_industry_unmapped |
| Details | job_type, job_position |
| Experience/Education | experience_level, min_exp_level, max_exp_level, education_level |
| Salary/Benefits | salary, min_monthly_salary, max_monthly_salary, benefits_categories_vi |
| Text | job_description_cleaned, requirements_cleaned |
| Skills | require_programming_languages, require_frameworks, require_tools, require_cloud_skills, require_knowledge, require_domain_knowledge, require_foreign_languages, require_domain_university |

### 6.4. Site-specific cleaning pipelines

#### 6.4.1. TopCV

TopCV Silver pipeline nằm tại [main_process.py](../src/storage_layer/MinIO_S3/layer/silver/cleaning/clean_topcv/main_process.py). Thứ tự xử lý:

1. Drop cột không cần thiết.
2. Đọc industry taxonomy.
3. Clean job_url.
4. Clean company name.
5. Process job title và special keywords.
6. Clean location.
7. Clean industry với separator dấu phẩy.
8. Clean description.
9. Clean salary.
10. Clean benefits.
11. Clean requirements và extract taxonomy skills.
12. Clean company size, job type, experience, education, position, deadline.

#### 6.4.2. ITviec

ITviec Silver pipeline nằm tại [main_process.py](../src/storage_layer/MinIO_S3/layer/silver/cleaning/clean_itviec/main_process.py). ITviec thiếu một số field so với TopCV/VietnamWorks nên pipeline bỏ qua các cleaner liên quan job_type, experience_level, education_level, job_position và job_deadline nếu cột không tồn tại.

Thứ tự chính:

1. Drop cột không cần thiết.
2. Đọc industry taxonomy.
3. Clean company name và job_url.
4. Process job title.
5. Clean location.
6. Clean industry với separator None.
7. Clean description, salary, benefits, requirements.
8. Clean company size.

#### 6.4.3. VietnamWorks

VietnamWorks Silver pipeline nằm tại [main_process.py](../src/storage_layer/MinIO_S3/layer/silver/cleaning/clean_vnworks/main_process.py). Pipeline tương tự TopCV nhưng không có company_size. Một số cột có thể tồn tại nhưng null nhiều.

Thứ tự chính:

1. Drop cột không cần thiết.
2. Đọc industry taxonomy.
3. Clean job_url và company name.
4. Process job title.
5. Clean location và industry.
6. Clean description, salary, benefits, requirements.
7. Clean job_type, experience, education, position, deadline.

### 6.5. Taxonomy seed design

Taxonomy seed nằm trong [seeds](../src/storage_layer/MinIO_S3/layer/silver/seeds). Hàm read_seeds trong [config_loader.py](../src/storage_layer/MinIO_S3/layer/silver/utils/config_loader.py) ưu tiên đọc từ Google Sheets nếu cấu hình tồn tại, sau đó fallback về CSV local.

Các seed chính:

| File | Vai trò |
|------|--------|
| program_lang_taxonomy.csv | Phân loại ngôn ngữ lập trình |
| framework_taxonomy.csv | Phân loại framework |
| tools_taxonomy.csv | Phân loại công cụ |
| cloud_skill_taxonomy.csv | Phân loại kỹ năng cloud |
| knowledge_taxonomy.csv | Phân loại kiến thức chuyên môn |
| domain_taxonomy.csv | Phân loại domain nghiệp vụ |
| language_taxonomy.csv | Phân loại ngoại ngữ |
| domain_university_taxonomy.csv | Mapping domain đại học |
| industry_taxonomy.csv | Chuẩn hóa ngành nghề |
| benefit_taxonomy.csv | Phân loại phúc lợi |
| location_mapping.csv | Chuẩn hóa địa điểm |
| company_mapping.csv | Chuẩn hóa tên công ty |

### 6.6. Silver storage design

Silver ghi Parquet thông qua [loader.py](../src/storage_layer/MinIO_S3/layer/silver/utils/loader.py).

Quy ước S3 key Silver:

```text
jobs/source_site=<site>/year=YYYY/month=MM/day=DD/clean_bronze_YYYYMMDD_HHMMSS.parquet
```

Ví dụ:

```text
jobs/source_site=topcv/year=2026/month=05/day=12/clean_bronze_20260512_170702.parquet
```

Silver reader trong [reader.py](../src/storage_layer/MinIO_S3/layer/silver/utils/reader.py) có thiết kế quan trọng: khi một ngày có nhiều file Parquet do rerun, reader chỉ chọn object mới nhất theo LastModified. Điều này giúp downstream không đọc lẫn snapshot cũ.

---

## 7. Thiết kế Supabase Serving Layer

### 7.1. Mục tiêu

Supabase phục vụ workload dạng OLTP/serving cho ứng dụng tìm kiếm việc làm. Thay vì đưa toàn bộ schema Silver vào frontend, hệ thống chỉ chọn tập cột cần thiết.

### 7.2. Schema

Schema target được định nghĩa bằng JobData trong [data_class.py](../src/storage_layer/Supabase/schema/data_class.py):

| Cột | Kiểu logic | Mô tả |
|-----|------------|-------|
| job_url | text | Khóa định danh duy nhất |
| job_title | text | Tiêu đề việc làm đã làm sạch |
| company_name | text | Tên công ty đã chuẩn hóa |
| location | text | Địa điểm đã chuẩn hóa |
| job_deadline | text | Hạn nộp |
| job_title_special_keywords | text array | Keyword/kỹ năng trong title |
| source_site | text | Nguồn dữ liệu |

### 7.3. Thiết kế load

Loader nằm tại [load_silver_to_supabase.py](../src/storage_layer/Supabase/scripts/load_silver_to_supabase.py), cấu hình SQL nằm tại [config.py](../src/storage_layer/Supabase/scripts/config.py).

Quy trình:

1. Tạo bảng ready_jobs nếu chưa tồn tại.
2. Lặp qua các site: topcv, itviec, vietnamworks.
3. Đọc Silver latest Parquet theo khoảng ngày.
4. Map clean_job_title → job_title, clean_location → location, clean_company_name → company_name.
5. Select đúng các cột trong JobData.
6. unique theo job_url.
7. Bulk UPSERT theo batch size 100.
8. Commit theo từng site; nếu site lỗi thì rollback site đó và tiếp tục site khác.

### 7.4. Thiết kế idempotency

Bảng ready_jobs có UNIQUE constraint trên job_url. UPSERT dùng ON CONFLICT(job_url) DO UPDATE, vì vậy chạy lại cùng một khoảng ngày sẽ cập nhật dữ liệu mới thay vì tạo bản ghi trùng.

---

## 8. Thiết kế Gold Layer trên MotherDuck

### 8.1. Mục tiêu

Gold layer phục vụ phân tích BI/OLAP. Dữ liệu Silver có nhiều list fields không tối ưu cho BI, vì vậy Gold reshape dữ liệu thành star schema gồm fact table, date dimension và các bridge table.

### 8.2. Cấu hình Gold

Gold config nằm trong [config.py](../src/storage_layer/MotherDuck/config.py). Các thành phần chính:

| Cấu hình | Ý nghĩa |
|----------|--------|
| MOTHERDUCK_DATABASE | Database target, mặc định lakehouse-lite |
| GOLD_SCHEMA | Schema gold |
| SILVER_PARQUET_GLOB | Glob đọc toàn bộ Silver Parquet từ S3 |
| GOLD_JOBS_TABLE | Fact table jobs |
| GOLD_INDUSTRIES_TABLE | Bridge table job_industries |
| GOLD_BENEFITS_TABLE | Bridge table job_benefits |
| GOLD_REQUIREMENTS_TABLE | Bridge table job_requirements |
| GOLD_DIM_DATE_TABLE | Dimension dim_date |

### 8.3. Schema Gold

GoldJobItem trong [data_class.py](../src/storage_layer/MotherDuck/schema/data_class.py) định nghĩa các field phục vụ fact jobs và list fields cần unnest.

Các field scalar được giữ ở gold.jobs, các field list được đưa sang child/bridge table:

| List field | Bảng đích | Cách biểu diễn |
|------------|----------|----------------|
| job_industry_clean | gold.job_industries | job_id, industry |
| benefits_categories_vi | gold.job_benefits | job_id, benefit |
| job_title_special_keywords | gold.job_requirements | job_id, requirement_type=special_keyword, value |
| require_programming_languages | gold.job_requirements | requirement_type=programming_language |
| require_frameworks | gold.job_requirements | requirement_type=framework |
| require_tools | gold.job_requirements | requirement_type=tool |
| require_cloud_skills | gold.job_requirements | requirement_type=cloud_skill |
| require_knowledge | gold.job_requirements | requirement_type=knowledge |
| require_domain_knowledge | gold.job_requirements | requirement_type=domain_knowledge |
| require_foreign_languages | gold.job_requirements | requirement_type=foreign_language |
| require_domain_university | gold.job_requirements | requirement_type=domain_university |

### 8.4. Thiết kế SQL build

Gold SQL builder nằm tại [gold_sql_builder.py](../src/storage_layer/MotherDuck/scripts/gold_sql_builder.py). Quy trình build:

1. Tạo staging table bằng cách đọc Silver Parquet trực tiếp từ S3.
2. Deduplicate theo job_url, chọn bản mới nhất theo year/month/day và filename.
3. Gán job_id bằng ROW_NUMBER theo job_url.
4. Tạo dim_date bằng generate_series.
5. Tạo fact table gold.jobs và date_key.
6. Unnest job_industry_clean sang gold.job_industries.
7. Unnest benefits_categories_vi sang gold.job_benefits.
8. Unnest các require_* sang gold.job_requirements.
9. Drop staging table.

### 8.5. Full refresh design

Các bảng Gold được tạo bằng CREATE OR REPLACE TABLE. Đây là thiết kế phù hợp vì:

- Quy mô dữ liệu đồ án không quá lớn.
- Logic rebuild đơn giản và dễ kiểm soát.
- Không cần xử lý incremental phức tạp.
- Đảm bảo Gold phản ánh snapshot mới nhất từ Silver.

---

## 9. Thiết kế CV Recommendation Layer

### 9.1. Mục tiêu

CV Recommendation Layer phục vụ người dùng cuối upload CV và nhận Top 10 việc làm phù hợp nhất. Thiết kế tận dụng dữ liệu Silver đã chuẩn hóa, taxonomy kỹ năng hiện có và vector database để kết hợp semantic matching với rule-based skill overlap.

### 9.2. Luồng index job từ Silver sang Qdrant

```text
Silver S3 Parquet
      ↓
index_silver_to_qdrant
      ↓
Build job text = clean_job_title + requirements_cleaned + job_description_cleaned
      ↓
Gemini embedding, task_type=RETRIEVAL_DOCUMENT, 768 dimensions
      ↓
Qdrant upsert collection jobs_v1, id=UUID5(job_url), payload=job metadata + require_*
```

Job text được sinh từ các trường đã làm sạch để giảm nhiễu HTML/bullet và tăng chất lượng embedding. Point id dựa trên job_url nên backfill hoặc index lại theo ngày vẫn idempotent.

### 9.3. Thiết kế Qdrant collection

| Thuộc tính | Thiết kế |
|------------|----------|
| Collection | jobs_v1 |
| Vector size | 768 |
| Distance | Cosine |
| Embedding model | gemini-embedding-001 |
| Job task type | RETRIEVAL_DOCUMENT |
| CV task type | RETRIEVAL_QUERY |
| Point id | UUID5 theo job_url |

Payload chính:

| Nhóm | Trường |
|------|--------|
| Định danh/hiển thị | job_url, job_title, company_name, source_site |
| Filter | clean_location, is_vietnam, min_exp_level, max_exp_level, deadline_ts |
| Lương | min_monthly_salary, max_monthly_salary |
| Kỹ năng | require_programming_languages, require_frameworks, require_tools, require_cloud_skills, require_knowledge, require_domain_knowledge, require_foreign_languages |

Các trường clean_location, min_exp_level và deadline_ts được tạo payload index để truy vấn filter nhanh.

### 9.4. Luồng xử lý request gợi ý CV

```text
React upload CV UI
      ↓ multipart/form-data
FastAPI POST /api/v1/recommend
      ↓
Validate file PDF/DOCX, <= 5MB, <= 5 trang
      ↓
Parse text bằng pdfplumber/python-docx
      ↓
Clean text + trích kỹ năng bằng taxonomy Google Sheets/CSV
      ↓
Gemini embedding, task_type=RETRIEVAL_QUERY
      ↓
Qdrant search với vector + filter location/experience/deadline
      ↓
Hybrid re-rank
      ↓
Top 10 job + score + matched_skills
```

Endpoint không yêu cầu đăng nhập. Mỗi lần upload là một phiên độc lập, không lưu lịch sử gợi ý của người dùng.

### 9.5. Công thức xếp hạng hybrid

Hệ thống dùng hai tín hiệu:

1. Cosine similarity từ Qdrant để đo mức gần ngữ nghĩa giữa CV và job.
2. Skill overlap theo các nhóm kỹ năng đã chuẩn hóa bằng taxonomy.

Công thức cuối:

```text
final = 0.6 * cosine + 0.4 * skill_overlap
```

Skill overlap ưu tiên programming languages, frameworks, tools, cloud skills, knowledge và domain knowledge. Kết quả trả về matched_skills để giải thích lý do job phù hợp với CV.

### 9.6. Thiết kế bảo mật và vận hành cho CV

| Khu vực | Thiết kế |
|---------|----------|
| Rate-limit | Endpoint /recommend giới hạn chặt hơn /jobs vì có upload file và gọi embedding |
| CV storage | CV gốc lưu ở S3 bucket riêng, lifecycle tự xóa sau 30 ngày |
| Log | Không log nội dung CV, chỉ log metadata kỹ thuật cần thiết |
| CORS | Backend cho phép POST /api/v1/recommend từ frontend hợp lệ |
| Cost control | Batch embedding khi index, retry/backoff khi gặp rate-limit Gemini/Qdrant |

---

## 10. Thiết kế Airflow Orchestration

### 10.1. Nguyên tắc orchestration

Factory DAG nằm trong [_dag_factory.py](../src/orchestration_layer/dags/_dag_factory.py). Airflow chỉ chịu trách nhiệm điều phối, không import trực tiếp crawler/storage modules trong process Airflow. Mọi tác vụ business chạy trong container pipeline thông qua DockerOperator.

Lợi ích:

- Airflow environment nhẹ hơn.
- Dependency crawler/browser không làm bẩn Airflow image.
- Logic pipeline có thể chạy độc lập bằng python -m ngoài Airflow.
- Dễ debug command tương ứng trên máy local/container.

### 10.2. SITE_CONFIGS

SITE_CONFIGS ánh xạ mỗi site với module crawl, module validation, bronze source và silver module. Đây là điểm mở rộng chính khi thêm website mới.

| Site | crawl_module | validate_module | bronze_source | silver_module |
|------|--------------|-----------------|---------------|---------------|
| topcv | topcv | topcv_validate | topcv | clean_topcv |
| vietnamworks | vietnamworks | vnworks_validate | vietnamworks | clean_vnworks |
| itviec | itviec | itviec_validate | itviec | clean_itviec |

### 10.3. Lịch chạy

| Nhóm DAG | Lịch | Thiết kế |
|----------|------|----------|
| Crawl | Mỗi 3 giờ, lệch phút theo site | Giảm tải đồng thời và tránh browser chạy cùng lúc quá nhiều |
| Validate/Bronze | Triggered bởi crawl | Chỉ upload Bronze sau khi validation pass |
| Silver | Mỗi 8 giờ | Làm sạch dữ liệu đã có trong Bronze |
| Supabase | Mỗi 6 giờ | Cập nhật serving table thường xuyên |
| Qdrant index | Hằng ngày | Đồng bộ job vector/payload cho CV Recommendation |
| Gold | Mỗi 6 giờ | Đồng bộ dữ liệu phân tích với Silver |
| Dashboard | 0 giờ hằng ngày | Sinh báo cáo tĩnh theo ngày |

### 10.4. DockerOperator design

DockerOperator dùng các mount chung:

| Mount | Vai trò |
|-------|--------|
| temp_data | Chia sẻ file JSONL giữa crawler và validate/bronze |
| .env | Cung cấp credential và cấu hình cho container |
| business/reports | Lưu dashboard HTML sinh ra từ container |
| Docker socket | Cho phép Airflow gọi pipeline container bằng DockerOperator |

shm_size được đặt 2GB để Chrome/nodriver hoạt động ổn định trong container.

---

## 11. Thiết kế Monitoring Layer

### 11.1. Thành phần monitoring

Monitoring stack được cấu hình trong [docker-compose.yaml](../src/monitoring_layer/docker-compose.yaml).

| Service | Image | Vai trò |
|---------|-------|--------|
| caddy | caddy:alpine | Reverse proxy, HTTPS/auth qua Caddyfile |
| prometheus | prom/prometheus | Thu thập metrics |
| grafana | grafana/grafana | Hiển thị operational dashboards |
| dashboards | nginx:alpine | Serve Bronze/Silver HTML dashboards |

### 11.2. Metrics flow

```text
Airflow → StatsD Exporter → Prometheus → Grafana
```

Airflow phát metrics sang statsd-exporter trong orchestration stack trên EC2-A. Prometheus trong monitoring stack trên EC2-B scrape endpoint statsd-exporter qua private IP/security group nội bộ. Grafana được provision datasource Prometheus và dashboard JSON.

### 11.3. Business dashboard flow

```text
Bronze/Silver S3 → dashboard generator Python → reports/*.html → Nginx dashboards → Caddy
```

Dashboard Bronze/Silver được sinh bởi:

- [bronze_dashboard.py](../src/monitoring_layer/business/bronze_dashboard.py)
- [silver_dashboard.py](../src/monitoring_layer/business/silver_dashboard.py)

Nginx serve file HTML trong reports, landing page nằm tại [index.html](../src/monitoring_layer/nginx/index.html). Ở môi trường triển khai hai EC2, reports là mount EFS chung tại src/monitoring_layer/business/reports: EC2-A ghi file HTML, EC2-B đọc và phục vụ qua Nginx/Caddy.

---

## 12. Thiết kế triển khai

### 12.1. Container hóa pipeline

Dockerfile ở [Dockerfile](../Dockerfile) xây image Python 3.11 slim, cài dependency, Chrome/Chromium và xvfb để chạy browser-based crawler trong môi trường không có màn hình.

Pipeline image được Airflow gọi qua DockerOperator. Khi chạy trên Linux server/EC2, browser crawler chạy dưới xvfb-run.

### 12.2. Orchestration stack

Orchestration stack ở [docker-compose.yaml](../src/orchestration_layer/docker-compose.yaml) gồm:

- Airflow webserver.
- Airflow scheduler.
- Postgres metadata DB.
- StatsD Exporter.

Airflow dùng LocalExecutor, DAGS_ARE_PAUSED_AT_CREATION=true và load_examples=false.

### 12.3. Monitoring stack

Monitoring stack gồm Prometheus, Grafana, Caddy và Nginx dashboards. Theo phương án triển khai hai EC2, stack này chạy trên EC2-B và dùng EFS để đọc reports do EC2-A sinh ra, theo tài liệu [deploy-two-ec2-efs.md](deploy-two-ec2-efs.md).

### 12.4. Phương án triển khai hai EC2 và EFS

```text
EC2-A — Orchestration/Pipeline
  Airflow + Postgres + statsd-exporter
  DockerOperator → lakehouse-crawler
  Ghi dashboard HTML vào EFS reports/

Amazon EFS
  Mount trên cả EC2-A và EC2-B tại src/monitoring_layer/business/reports

EC2-B — Monitoring
  Caddy + Prometheus + Grafana + Nginx dashboards
  Prometheus scrape statsd-exporter EC2-A
  Nginx serve reports/ từ EFS
```

Pipeline crawl → Bronze → Silver → Supabase → Qdrant → Gold được bố trí chạy trên EC2-A. EC2-B không xử lý dữ liệu pipeline mà tập trung vào quan sát vận hành, dashboard và reverse proxy.

### 12.5. Cloud services

| Service | Vai trò | Credential/config |
|---------|--------|-------------------|
| AWS S3 | Bronze/Silver object storage | AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION |
| Supabase | PostgreSQL serving DB | SUPABASE_HOST, SUPABASE_PORT, SUPABASE_DATABASE, SUPABASE_USER, SUPABASE_PASSWORD |
| MotherDuck | Gold OLAP warehouse | MOTHERDUCK_TOKEN |
| Google Sheets | Editable taxonomy source | GOOGLE_SHEETS_CREDENTIALS_FILE, GOOGLE_SHEETS_SPREADSHEET_ID |
| Qdrant Cloud | Vector database cho jobs_v1 | QDRANT_URL, QDRANT_API_KEY, QDRANT_COLLECTION |
| Google Gemini | Embedding job/CV | GEMINI_API_KEY, EMBEDDING_MODEL, EMBEDDING_DIM |
| Amazon EFS | Shared reports giữa EC2-A và EC2-B | File system ID, mount target, security group NFS 2049 |

---

## 13. Thiết kế cấu hình và bảo mật

### 13.1. Cấu hình môi trường

File [.env.example](../.env.example) cung cấp template biến môi trường. File .env thật không được commit.

Nhóm biến chính:

| Nhóm | Biến tiêu biểu |
|------|----------------|
| ITviec login | ITVIEC_USERNAME, ITVIEC_PASSWORD |
| AWS | AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION |
| Supabase | SUPABASE_HOST, SUPABASE_PORT, SUPABASE_DATABASE, SUPABASE_USER, SUPABASE_PASSWORD |
| MotherDuck | MOTHERDUCK_TOKEN |
| Google Sheets | GOOGLE_SHEETS_CREDENTIALS_FILE, GOOGLE_SHEETS_SPREADSHEET_ID |
| Qdrant/Gemini | QDRANT_URL, QDRANT_API_KEY, QDRANT_COLLECTION, GEMINI_API_KEY, EMBEDDING_MODEL, EMBEDDING_DIM |
| CV storage | CV_S3_BUCKET và AWS credential tương ứng |
| Airflow | FERNET_KEY, WEBSERVER_SECRET_KEY, HOST_REPO_PATH, PIPELINE_IMAGE, AIRFLOW_UID |
| Monitoring | MONITORING_DOMAIN, MONITORING_AUTH_USER, MONITORING_AUTH_PASSWORD_HASH |

### 13.2. Bảo mật dữ liệu và truy cập

| Khu vực | Thiết kế bảo mật |
|---------|------------------|
| Credential | Lưu trong .env, không commit |
| AWS S3 | IAM cần quyền tối thiểu list/get/put/delete trên Bronze và Silver buckets |
| Supabase | Dùng connection config từ env; có thể bổ sung RLS ở tầng ứng dụng |
| Monitoring | Caddy reverse proxy, auth qua user/password hash |
| Dashboard HTML | Không commit reports sinh từ dữ liệu thật nếu có thông tin nhạy cảm |
| Browser login | ITviec credential lấy từ env, không hardcode |
| CV upload | Không log nội dung CV, lưu S3 bucket riêng và lifecycle xóa sau 30 ngày |
| Recommendation API | Rate-limit endpoint /recommend và chỉ cho phép CORS từ frontend hợp lệ |
| EFS | Chỉ mount reports, không đưa Postgres/Docker volumes/log pipeline lên EFS |

---

## 14. Thiết kế độ tin cậy và xử lý lỗi

| Khu vực | Cơ chế thiết kế |
|---------|----------------|
| Crawler | Flush theo page để giảm mất dữ liệu khi lỗi giữa chừng |
| URL duplicate | Mỗi crawler duy trì _seen_urls trong phiên chạy |
| HTTP TopCV | Có retry, delay, bounded concurrency |
| Browser crawler | Dùng một browser session cho site cần login/clearance |
| Validation | Nếu Great Expectations fail thì DAG validate/bronze dừng trước upload |
| Bronze | Upload xong mới dọn temp source |
| Silver | Nếu ngày không có Bronze thì skip ngày đó |
| Silver schema | enforce_silver_schema thêm cột thiếu, cast kiểu và bỏ cột ngoài schema |
| Supabase | Commit theo site, lỗi site này rollback và tiếp tục site khác |
| Qdrant index | Upsert theo UUID5(job_url), chạy lại không tạo vector trùng |
| Gemini/Qdrant rate-limit | Batch embedding, retry/backoff và lịch index hằng ngày |
| CV recommendation | Validate file sớm, xử lý lỗi parse rõ ràng, re-rank sau khi search để ổn định kết quả |
| Gold | Full refresh bằng CREATE OR REPLACE, log row counts sau khi chạy |
| Airflow | retries=1, retry_delay=5 phút, max_active_runs=1 |
| EFS reports | Chỉ dùng làm shared output dashboard, giảm rủi ro ảnh hưởng database/log khi EFS lỗi |

---

## 15. Thiết kế hiệu năng và mở rộng

### 15.1. Thiết kế hiệu năng

| Thành phần | Thiết kế hiệu năng |
|------------|-------------------|
| TopCV crawler | Async HTTP, batch detail fetch, concurrency có giới hạn |
| Browser crawler | Chạy tuần tự theo session để ổn định với anti-bot |
| Bronze | JSONL.GZ giảm dung lượng lưu trữ raw |
| Silver | Polars xử lý DataFrame nhanh, Parquet tối ưu đọc cột |
| Silver reader | Pre-check S3 prefix trước khi scan để tránh lỗi path rỗng |
| Supabase | Bulk UPSERT bằng execute_values và batch size 100 |
| Qdrant | Vector search trên collection jobs_v1, filter payload cho location/experience/deadline |
| Gemini embedding | Batch embedding khi index job, chỉ embed 1 CV ở request time |
| Recommendation API | Search top N rồi re-rank in-memory để trả Top 10 có giải thích |
| Gold | DuckDB/MotherDuck đọc Parquet trực tiếp từ S3 và xử lý bằng SQL vectorized |

### 15.2. Mở rộng source mới

Để thêm website tuyển dụng mới, thiết kế đề xuất yêu cầu:

1. Tạo module crawler mới trong crawl_layer/crawler/<source>.
2. Tạo dataclass hoặc mở rộng JobItem nếu có trường mới.
3. Tạo validation module trong local_temp/validation.
4. Tạo cleaning module trong silver/cleaning/clean_<source>.
5. Bổ sung mapping trong SITE_CONFIGS của Airflow.
6. Bổ sung source vào Supabase SITES nếu cần phục vụ frontend.
7. Kiểm tra Gold nếu source_site mới được đọc tự động qua Hive partitioning.

### 15.3. Mở rộng field mới

Để thêm field mới vào Silver:

1. Thêm field vào SilverJobItem.
2. Bổ sung cleaner tạo field đó trong pipeline site/common tương ứng.
3. enforce_silver_schema tự động đưa field vào schema Polars.
4. Nếu field cần lên Supabase thì thêm vào JobData và config tự sinh DDL/UPSERT.
5. Nếu field cần lên Gold thì thêm vào GoldJobItem hoặc LIST_FIELD_TO_CHILD nếu là list field.
6. Nếu field cần dùng cho gợi ý CV thì thêm vào Qdrant payload hoặc job text embedding.

---

## 16. Thiết kế giao diện vận hành

### 16.1. CLI commands chính

| Nhóm | Command mẫu |
|------|-------------|
| Crawl TopCV | python -m src.crawl_layer.crawler.topcv --keyword "data" --max-pages 2 |
| Crawl ITviec | python -m src.crawl_layer.crawler.itviec --keyword "data" --max-pages 2 --headless |
| Crawl VietnamWorks | python -m src.crawl_layer.crawler.vietnamworks --keyword "data" --max-pages 2 --headless |
| Bronze upload | python -m src.storage_layer.MinIO_S3.layer.bronze.main --source topcv |
| Silver TopCV | python -m src.storage_layer.MinIO_S3.layer.silver.cleaning.clean_topcv.main_process --from_date YYYY-MM-DD --to_date YYYY-MM-DD |
| Supabase load | python -m src.storage_layer.Supabase.scripts.load_silver_to_supabase --from_date YYYY-MM-DD --to_date YYYY-MM-DD |
| Qdrant index | python -m src.storage_layer.Qdrant.scripts.index_silver_to_qdrant --from_date YYYY-MM-DD --to_date YYYY-MM-DD |
| Gold load | python -m src.storage_layer.MotherDuck.scripts.load_silver_to_gold |
| Dashboard Bronze | python -m src.monitoring_layer.business.bronze_dashboard |
| Dashboard Silver | python -m src.monitoring_layer.business.silver_dashboard |

### 16.2. Airflow params

Các DAG hỗ trợ override tham số qua Airflow UI:

| DAG | Params |
|-----|--------|
| crawl_<site> | keyword, max_pages |
| silver_<site> | from_date, to_date |
| supabase_load_all | from_date, to_date |
| index_qdrant | from_date, to_date |
| cluster_company_name | from_date, to_date |

---

## 17. Thiết kế kiểm thử và nghiệm thu

Do phạm vi đồ án không đặt trọng tâm vào hệ thống CI/CD đầy đủ, kiểm thử/nghiệm thu vận hành được thiết kế dựa trên các bước sau:

| Mục tiêu kiểm tra | Cách kiểm tra |
|-------------------|--------------|
| Crawler hoạt động | Chạy max-pages nhỏ, kiểm tra temp JSONL có record |
| Validation pass/fail đúng | Chạy validation module tương ứng, xem log Great Expectations |
| Bronze upload đúng | Kiểm tra object S3 theo prefix source/jobs/year/month/day |
| Silver cleaning không ghi dữ liệu | Chạy Silver với --no_save |
| Silver output kiểm tra thủ công | Chạy Silver với --export_parquet |
| Supabase load | Kiểm tra row count và unique job_url trong ready_jobs |
| Qdrant index | Kiểm tra collection count xấp xỉ số job Silver còn hạn và chạy index lại không tăng trùng |
| Recommendation API | Upload CV mẫu Data Engineer, kiểm tra Top 10 có score và matched_skills hợp lý |
| Recommendation filter | Chọn location/số năm kinh nghiệm và kiểm tra kết quả tôn trọng filter |
| Gold build | Kiểm tra log row counts của gold.jobs và bridge tables |
| Dashboard | Generate HTML và mở qua Nginx/Caddy |
| Two-EC2/EFS deployment | EC2-A ghi reports vào EFS, EC2-B đọc reports qua Nginx và Prometheus scrape StatsD Exporter EC2-A |
| Airflow DAG | Trigger DAG với config nhỏ, kiểm tra task logs |

Hạng mục kiểm thử có thể bổ sung sau:

- Unit test cho cleaner quan trọng như salary, location, requirements.
- Contract test cho Silver schema.
- Integration test đọc Bronze sample và xuất Silver sample.
- Smoke test cho DAG command.
- Data quality report định kỳ ở Silver.

---

## 18. Thiết kế mở rộng tương lai

### 18.1. Web frontend

Supabase ready_jobs được thiết kế như một projection nhẹ cho frontend. Frontend truy vấn theo job_title, company_name, location, source_site và deadline mà không cần đọc trực tiếp Silver/Gold. Ngoài luồng tìm kiếm truyền thống, frontend còn được thiết kế cung cấp giao diện upload CV để gọi API gợi ý việc làm.

### 18.2. Nâng cấp CV Recommendation

Tính năng gợi ý việc làm theo CV được thiết kế theo tài liệu [cv-recommendation-feature.md](cv-recommendation-feature.md). Các hướng nâng cấp sau giai đoạn triển khai ban đầu gồm đánh giá relevance bằng bộ CV/job mẫu, tinh chỉnh trọng số skill_overlap theo ngành nghề, bổ sung filter lương và mở rộng giải thích kết quả bằng nhóm kỹ năng thiếu/cần cải thiện.

### 18.3. Data quality nâng cao

Có thể mở rộng validation từ local temp sang Silver bằng các rule như:

- job_url không null và có format URL hợp lệ.
- salary min <= max.
- min_exp_level <= max_exp_level.
- clean_location thuộc danh sách tỉnh/thành chuẩn.
- job_industry_clean không rỗng với tỷ lệ tối thiểu.

### 18.4. Incremental Gold

Trong phạm vi thiết kế ban đầu, Gold full refresh phù hợp quy mô đồ án. Khi dữ liệu lớn hơn, có thể chuyển sang incremental strategy:

- Partition Gold theo date_key.
- Merge/upsert fact jobs theo job_url.
- Refresh bridge tables theo tập job_id thay đổi.
- Dùng watermark theo LastModified hoặc ingestion timestamp.

---

## 19. Kết luận thiết kế

Thiết kế đề xuất của Lakehouse-Lite phù hợp với mục tiêu của một hệ thống dữ liệu phục vụ đồ án tốt nghiệp nhưng vẫn bám sát thực hành hiện đại trong data engineering. Hệ thống có kiến trúc phân tầng rõ ràng, sử dụng Medallion Architecture, tách biệt business logic khỏi orchestration, tận dụng object storage cho data lake, dùng Parquet cho dữ liệu đã xử lý, dùng Supabase cho serving, Qdrant/Gemini cho gợi ý CV và MotherDuck cho phân tích OLAP.

Điểm nổi bật của thiết kế là khả năng tái xử lý dữ liệu từ Bronze, schema Silver tập trung, cơ chế UPSERT/vector upsert/full refresh idempotent, cùng khả năng giám sát vận hành bằng Airflow/Prometheus/Grafana trên mô hình hai EC2 chia sẻ EFS. Thiết kế này tạo nền tảng tốt để mở rộng thêm nguồn tuyển dụng mới, data quality nâng cao và các thuật toán recommendation phức tạp hơn trong tương lai.
