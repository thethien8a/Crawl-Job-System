# Tài liệu phân tích hệ thống — Lakehouse-Lite

> Phạm vi tài liệu: phân tích yêu cầu, bối cảnh, phạm vi và ràng buộc của hệ thống Lakehouse-Lite trong giai đoạn trước triển khai để sử dụng trong báo cáo đồ án tốt nghiệp. Tài liệu mô tả các thành phần được lựa chọn cho phạm vi xây dựng, gồm thu thập dữ liệu, lưu trữ theo kiến trúc Medallion, làm sạch dữ liệu, phục vụ dữ liệu, gợi ý việc làm theo CV, phân tích BI, điều phối và giám sát.

---

## 1. Tổng quan hệ thống

Lakehouse-Lite là hệ thống pipeline dữ liệu phục vụ thu thập, chuẩn hóa, lưu trữ và phân tích dữ liệu tin tuyển dụng từ các nền tảng tuyển dụng tại Việt Nam. Hệ thống tập trung vào nhóm việc làm liên quan đến dữ liệu và trí tuệ nhân tạo như Data Engineer, Data Analyst, Data Scientist, AI/ML Engineer, Business Intelligence và Machine Learning Engineer.

Hệ thống được định hướng tổ chức theo kiến trúc Lakehouse dạng Medallion gồm ba lớp dữ liệu chính:

| Lớp dữ liệu | Vai trò chính | Định dạng/lưu trữ |
|------------|---------------|-------------------|
| Bronze | Lưu dữ liệu thô đã crawl, gần với dữ liệu nguồn nhất | JSONL nén gzip trên AWS S3 |
| Silver | Lưu dữ liệu đã làm sạch, chuẩn hóa và enrich | Parquet trên AWS S3 |
| Gold | Lưu dữ liệu phân tích theo mô hình sao phục vụ BI | MotherDuck/DuckDB |

Ngoài ba lớp dữ liệu, phạm vi hệ thống còn bao gồm:

- Supabase/PostgreSQL để phục vụ dữ liệu tuyển dụng đã làm sạch cho tầng ứng dụng.
- Airflow để lập lịch và điều phối pipeline.
- Prometheus, Grafana, Caddy, Nginx và dashboard HTML tĩnh để giám sát.
- Power BI để phân tích dữ liệu Gold.

Các tài liệu và mã nguồn tham chiếu chính:

| Nội dung | Tham chiếu |
|----------|------------|
| Tổng quan kiến trúc và pipeline | [README.md](../README.md) |
| Tài liệu công nghệ | [technology-stack-documentation.md](technology-stack-documentation.md) |
| Sơ đồ kiến trúc HTML | [architecture.html](architecture.html) |
| Factory tạo DAG Airflow | [_dag_factory.py](../src/orchestration_layer/dags/_dag_factory.py) |
| Schema Silver | [data_class.py](../src/storage_layer/MinIO_S3/layer/silver/data_model/data_class.py) |
| Schema Supabase | [data_class.py](../src/storage_layer/Supabase/schema/data_class.py) |
| Schema Gold | [data_class.py](../src/storage_layer/MotherDuck/schema/data_class.py) |

---

## 2. Bối cảnh và vấn đề cần giải quyết

### 2.1. Bối cảnh nghiệp vụ

Thị trường tuyển dụng ngành dữ liệu tại Việt Nam phân tán trên nhiều nền tảng như TopCV, ITviec và VietnamWorks. Mỗi nền tảng có cấu trúc HTML, cách đặt tên trường dữ liệu, cách biểu diễn mức lương, địa điểm, yêu cầu kỹ năng và ngành nghề khác nhau. Nếu chỉ thu thập dữ liệu thô, dữ liệu khó sử dụng cho tìm kiếm, phân tích hoặc báo cáo.

Lakehouse-Lite được đề xuất xây dựng để giải quyết vấn đề đó bằng cách tạo một pipeline dữ liệu tập trung, tự động và có khả năng mở rộng. Dữ liệu từ nhiều nguồn sẽ được thu thập định kỳ, lưu trữ thô, kiểm tra chất lượng, làm sạch, chuẩn hóa và đưa vào các kho phục vụ khác nhau tùy mục đích sử dụng.

### 2.2. Các vấn đề chính

| Vấn đề | Biểu hiện | Định hướng xử lý của hệ thống |
|--------|-----------|---------------------|
| Dữ liệu phân tán | Tin tuyển dụng nằm ở nhiều website, mỗi website có cấu trúc khác nhau | Xây dựng crawler riêng cho từng nguồn |
| Dữ liệu không đồng nhất | Tên công ty, địa điểm, kỹ năng, ngành nghề có nhiều cách viết | Chuẩn hóa ở Silver layer bằng taxonomy và mapping |
| Website có cơ chế chống bot | ITviec và VietnamWorks cần trình duyệt thật hoặc JavaScript rendering | Dùng nodriver/headless Chrome cho crawler browser-based |
| Cần lưu lại dữ liệu thô | Dữ liệu crawl có thể cần xử lý lại khi logic làm sạch thay đổi | Lưu Bronze dưới dạng JSONL nén trên S3 |
| Cần dữ liệu tối ưu cho phân tích | Dữ liệu list/multi-label khó phân tích trực tiếp | Xây Gold star schema trên MotherDuck |
| Cần phục vụ ứng dụng tìm kiếm | Frontend không cần toàn bộ schema Silver | Đưa projection nhẹ vào Supabase |
| Cần vận hành tự động | Chạy thủ công nhiều bước dễ lỗi | Lập lịch bằng Airflow và DockerOperator |

---

## 3. Mục tiêu hệ thống

### 3.1. Mục tiêu tổng quát

Xây dựng một hệ thống Lakehouse dữ liệu tuyển dụng có khả năng thu thập dữ liệu tự động từ nhiều nguồn, lưu trữ theo nhiều cấp độ xử lý, chuẩn hóa dữ liệu để phân tích và phục vụ dữ liệu cho các ứng dụng phía trên.

### 3.2. Mục tiêu cụ thể

1. Thu thập dữ liệu tin tuyển dụng từ TopCV, ITviec và VietnamWorks.
2. Lưu dữ liệu thô theo ngày, nguồn và loại thực thể để đảm bảo có thể truy vết.
3. Kiểm tra chất lượng dữ liệu trước khi đưa vào tầng Bronze.
4. Làm sạch và chuẩn hóa các trường quan trọng như tiêu đề việc làm, tên công ty, địa điểm, ngành nghề, kỹ năng, lương, kinh nghiệm và phúc lợi.
5. Lưu dữ liệu Silver dưới định dạng Parquet để tối ưu đọc phân tích.
6. Tải dữ liệu đã làm sạch vào Supabase để phục vụ tìm kiếm việc làm.
7. Index dữ liệu đã làm sạch sang Qdrant/Gemini để phục vụ gợi ý việc làm theo CV.
8. Xây dựng Gold layer dạng star schema trong MotherDuck để phục vụ Power BI.
9. Điều phối toàn bộ pipeline bằng Airflow.
10. Giám sát trạng thái pipeline và dữ liệu thông qua dashboard vận hành và dashboard nghiệp vụ.

---

## 4. Phạm vi hệ thống mục tiêu

### 4.1. Thành phần thuộc phạm vi triển khai

| Thành phần | Phạm vi | Mô tả |
|------------|---------|------|
| Crawl TopCV | Trong phạm vi triển khai | Crawler bất đồng bộ dựa trên HTTP/curl_cffi/aiohttp |
| Crawl ITviec | Trong phạm vi triển khai | Crawler dùng nodriver, có đăng nhập và xử lý Cloudflare/browser session |
| Crawl VietnamWorks | Trong phạm vi triển khai | Crawler dùng nodriver để xử lý nội dung render bằng JavaScript |
| Local temp staging | Trong phạm vi triển khai | Ghi dữ liệu crawl vào JSONL theo ngày trong thư mục temp_data |
| Validation | Trong phạm vi triển khai | Dùng Great Expectations kiểm tra null ratio và schema cơ bản |
| Bronze layer | Trong phạm vi triển khai | Nén JSONL thành JSONL.GZ và upload lên AWS S3 |
| Silver layer | Trong phạm vi triển khai | Làm sạch bằng Polars, taxonomy CSV/Google Sheets, xuất Parquet |
| Supabase serving | Trong phạm vi triển khai | UPSERT projection nhẹ vào bảng ready_jobs |
| CV Recommendation Layer | Trong phạm vi triển khai | Gợi ý Top 10 việc làm theo CV bằng Gemini embedding, Qdrant và hybrid scoring |
| Qdrant/Gemini indexing | Trong phạm vi triển khai | Index dữ liệu Silver sang Qdrant collection jobs_v1 bằng vector 768 chiều |
| Gold layer | Trong phạm vi triển khai | Xây star schema trong MotherDuck từ Silver Parquet |
| Airflow orchestration | Trong phạm vi triển khai | DAG theo từng site và các DAG load Supabase/Gold/dashboard |
| Monitoring | Trong phạm vi triển khai | Prometheus, Grafana, Caddy, Nginx, dashboard Bronze/Silver HTML |
| Triển khai hai EC2 + EFS | Trong phạm vi triển khai | EC2-A chạy orchestration/pipeline, EC2-B chạy monitoring, EFS chia sẻ reports |
| BI report | Trong phạm vi triển khai | File Power BI PBIX trong bi_report_layer |

### 4.2. Thành phần tích hợp dự kiến ngoài repository chính

| Thành phần | Phạm vi | Ghi chú |
|------------|---------|--------|
| Web frontend/JobSearchWeb | Thiết kế tích hợp ở repo phục vụ riêng | Đọc Supabase ready_jobs và gọi endpoint gợi ý CV |
| CV Recommendation serving | Thiết kế trong backend JobSearchWeb | FastAPI nhận CV PDF/DOCX tại POST /api/v1/recommend, parse CV, embed và trả Top 10 job phù hợp |
| CV Recommendation UI | Thiết kế trong frontend JobSearchWeb | React/Vite cung cấp màn hình upload CV, chọn location/số năm kinh nghiệm và hiển thị điểm phù hợp |

---

## 5. Tác nhân và bên liên quan

| Tác nhân | Vai trò | Tương tác với hệ thống |
|----------|---------|------------------------|
| Người vận hành pipeline | Cấu hình, chạy, theo dõi pipeline | Kích hoạt DAG, xem log Airflow, kiểm tra dashboard |
| Airflow Scheduler | Tự động lập lịch công việc | Chạy crawler, validation, Bronze, Silver, Supabase, Gold |
| Website tuyển dụng | Nguồn dữ liệu bên ngoài | Cung cấp HTML/trang job detail cho crawler |
| Data Engineer | Phát triển và bảo trì pipeline | Thêm nguồn crawl, chỉnh cleaning, cập nhật schema |
| Data Analyst/BI user | Phân tích dữ liệu thị trường việc làm | Truy cập Power BI/MotherDuck/Gold tables |
| Ứng dụng frontend | Người tiêu thụ dữ liệu phục vụ | Đọc bảng ready_jobs từ Supabase và gọi API gợi ý CV |
| Ứng viên/người tìm việc | Người dùng cuối | Upload CV, chọn bộ lọc và nhận danh sách việc làm phù hợp |
| Quản trị hệ thống | Đảm bảo hạ tầng hoạt động ổn định | Theo dõi Prometheus/Grafana/Caddy/Nginx |

---

## 6. Yêu cầu chức năng

### 6.1. Nhóm chức năng thu thập dữ liệu

| Mã | Yêu cầu | Mô tả |
|----|---------|-------|
| FR-C01 | Thu thập dữ liệu TopCV | Hệ thống thu thập danh sách job và trang chi tiết từ TopCV theo keyword và số trang cấu hình |
| FR-C02 | Thu thập dữ liệu ITviec | Hệ thống đăng nhập ITviec, mở trang tìm kiếm, click từng job card và parse dữ liệu |
| FR-C03 | Thu thập dữ liệu VietnamWorks | Hệ thống mở trang tìm kiếm bằng trình duyệt, lấy URL job và parse chi tiết |
| FR-C04 | Chống trùng URL trong một phiên crawl | Mỗi crawler duy trì tập URL đã thấy để tránh ghi trùng trong cùng lần chạy |
| FR-C05 | Lưu tạm theo batch/trang | Dữ liệu được flush theo từng page vào local temp để giảm mất dữ liệu nếu crawler lỗi giữa chừng |

### 6.2. Nhóm chức năng kiểm tra và lưu trữ Bronze

| Mã | Yêu cầu | Mô tả |
|----|---------|-------|
| FR-B01 | Lưu dữ liệu crawl vào JSONL | Mỗi record là một dòng JSON, file đặt theo source, entity và ngày crawl |
| FR-B02 | Validate dữ liệu local temp | Kiểm tra tỷ lệ null và các trường bắt buộc bằng Great Expectations |
| FR-B03 | Nén dữ liệu trước khi upload | File JSONL được gzip thành JSONL.GZ |
| FR-B04 | Upload lên S3 Bronze | S3 key phân vùng theo source/entity/year/month/day |
| FR-B05 | Dọn local temp sau upload | Sau khi upload thành công, file local của source được xóa để tránh upload lại |

### 6.3. Nhóm chức năng xử lý Silver

| Mã | Yêu cầu | Mô tả |
|----|---------|-------|
| FR-S01 | Đọc Bronze theo khoảng ngày | Pipeline Silver đọc từng ngày trong khoảng from_date/to_date |
| FR-S02 | Làm sạch dữ liệu theo từng site | TopCV, ITviec và VietnamWorks có pipeline làm sạch riêng |
| FR-S03 | Chuẩn hóa schema Silver | DataFrame sau cleaning được ép về schema SilverJobItem |
| FR-S04 | Chuẩn hóa trường nghiệp vụ | Chuẩn hóa job title, công ty, địa điểm, ngành nghề, lương, kinh nghiệm, phúc lợi, yêu cầu kỹ năng |
| FR-S05 | Phân loại kỹ năng bằng taxonomy | Trích xuất list kỹ năng từ requirements dựa trên seed taxonomy |
| FR-S06 | Lưu Parquet lên S3 Silver | Dữ liệu sạch được ghi thành Parquet theo source_site/year/month/day |

### 6.4. Nhóm chức năng phục vụ dữ liệu

| Mã | Yêu cầu | Mô tả |
|----|---------|-------|
| FR-P01 | Tải dữ liệu Silver vào Supabase | Đọc Silver theo site và khoảng ngày, chọn các cột phục vụ frontend |
| FR-P02 | UPSERT theo job_url | Nếu job_url đã tồn tại thì cập nhật, không tạo bản ghi trùng |
| FR-P03 | Map cleaned columns cho frontend | clean_job_title, clean_location, clean_company_name được alias về job_title, location, company_name |
| FR-P04 | Xây Gold layer | MotherDuck đọc Silver Parquet trực tiếp từ S3 và tạo fact/dimension/bridge tables |
| FR-P05 | Load taxonomy vào Gold | Các CSV taxonomy có thể được nạp thành dimension table trong Gold |

### 6.5. Nhóm chức năng gợi ý việc làm theo CV

| Mã | Yêu cầu | Mô tả |
|----|---------|-------|
| FR-R01 | Index job sang vector database | Hệ thống đọc Silver Parquet, ghép clean_job_title, requirements_cleaned và job_description_cleaned để tạo embedding job |
| FR-R02 | Lưu vector và payload job | Vector 768 chiều được upsert vào Qdrant collection jobs_v1, payload giữ job_url, title, company, location, salary, experience, source_site và require_* |
| FR-R03 | Nhận CV người dùng | Frontend cho phép upload PDF/DOCX, giới hạn dung lượng và số trang |
| FR-R04 | Parse và embed CV | Backend trích xuất text CV, làm sạch, trích kỹ năng taxonomy và tạo embedding truy vấn bằng Gemini |
| FR-R05 | Lọc ứng viên/job | Query Qdrant kết hợp vector similarity với filter location, kinh nghiệm và deadline |
| FR-R06 | Xếp hạng hybrid | Kết quả được re-rank bằng công thức final = 0.6 * cosine + 0.4 * skill_overlap |
| FR-R07 | Giải thích kết quả | API trả Top 10 job kèm score và matched_skills để người dùng hiểu vì sao phù hợp |
| FR-R08 | Quản lý CV upload | CV được lưu ở S3 riêng, áp dụng lifecycle tự xóa sau 30 ngày và không log nội dung CV |

### 6.6. Nhóm chức năng điều phối và giám sát

| Mã | Yêu cầu | Mô tả |
|----|---------|-------|
| FR-O01 | Lập lịch crawl theo site | TopCV, ITviec, VietnamWorks chạy lệch phút mỗi 3 giờ |
| FR-O02 | Tách DAG validate/bronze | Crawl DAG trigger DAG validate_bronze tương ứng nhưng không đợi kết quả |
| FR-O03 | Lập lịch Silver | Silver cleaning chạy mỗi 8 giờ |
| FR-O04 | Lập lịch Supabase và Gold | Load Supabase và Gold chạy mỗi 6 giờ |
| FR-O05 | Sinh dashboard nghiệp vụ | Dashboard Bronze/Silver được generate thành HTML hằng ngày |
| FR-O06 | Giám sát vận hành | Airflow phát metrics sang StatsD Exporter, Prometheus scrape và Grafana hiển thị |
| FR-O07 | Đồng bộ index Qdrant | DAG index_qdrant cập nhật vector job từ Silver theo lịch hằng ngày |

---

## 7. Yêu cầu phi chức năng

| Nhóm yêu cầu | Mô tả | Định hướng đáp ứng |
|--------------|-------|---------------------|
| Khả năng mở rộng dữ liệu | Dữ liệu tăng theo thời gian, cần lưu trữ linh hoạt | Dùng AWS S3 cho Bronze/Silver, phân vùng theo ngày và nguồn |
| Hiệu năng đọc phân tích | BI và Gold cần đọc nhanh | Silver dùng Parquet, MotherDuck đọc trực tiếp từ S3 |
| Tính idempotent | Chạy lại pipeline không làm sai dữ liệu | Supabase UPSERT theo job_url; Gold CREATE OR REPLACE; Silver chọn latest Parquet theo LastModified |
| Truy vết dữ liệu | Cần biết dữ liệu đến từ nguồn nào và ngày nào | S3 key chứa source/source_site/year/month/day; record có source_site/search_keyword |
| Khả năng bảo trì | Dễ thêm trường và cập nhật schema | Silver schema derive từ dataclass SilverJobItem |
| Độ tin cậy | Lỗi một site không làm hỏng toàn hệ thống | Per-site DAG và per-site commit Supabase |
| Bảo mật thông tin cấu hình | Không commit credential | Dùng .env và file example; .env bị ignore |
| Quan sát vận hành | Cần xem trạng thái job và metrics | Airflow UI, Prometheus, Grafana, dashboard HTML |
| Gợi ý thời gian thực | Người dùng cần nhận gợi ý ngay sau khi upload CV | Backend FastAPI parse CV, embed query và truy vấn Qdrant theo thời gian thực |
| Bảo vệ dữ liệu cá nhân | CV chứa thông tin cá nhân cần được xử lý hạn chế | Lưu S3 bucket riêng, lifecycle 30 ngày, không log nội dung CV |
| Chi phí hợp lý | Phù hợp đồ án tốt nghiệp | Dùng S3, Supabase, MotherDuck, Docker Compose; hạn chế hạ tầng tự quản lý |

---

## 8. Luồng dữ liệu nghiệp vụ

### 8.1. Luồng tổng quát

```text
TopCV / ITviec / VietnamWorks
        ↓
Crawl Layer
        ↓
Local temp JSONL
        ↓
Validation
        ↓
Bronze S3 JSONL.GZ
        ↓
Silver Cleaning + Taxonomy Enrichment
        ↓
Silver S3 Parquet
        ├──→ Supabase ready_jobs phục vụ ứng dụng tìm kiếm
        ├──→ Gemini Embedding → Qdrant jobs_v1 phục vụ gợi ý CV
        └──→ MotherDuck Gold star schema phục vụ BI/Power BI
```

### 8.2. Mô tả từng bước

| Bước | Đầu vào | Xử lý | Đầu ra |
|------|---------|-------|--------|
| Crawl | HTML/job pages từ website | Parse job list và job detail | JSON records |
| Save temp | JSON records | Append từng record vào file JSONL theo ngày | temp_data/source_jobs_YYYYMMDD.jsonl |
| Validate | JSONL local | Kiểm tra null ratio, required/optional columns | Pass/fail validation |
| Bronze upload | JSONL local | Gzip, upload S3, xóa temp | JSONL.GZ trong Bronze bucket |
| Silver cleaning | Bronze JSONL.GZ | Làm sạch, chuẩn hóa, enrich taxonomy | Parquet trong Silver bucket |
| Supabase load | Silver Parquet | Chọn cột, đổi tên cleaned fields, dedup, UPSERT | Bảng ready_jobs |
| Qdrant indexing | Silver Parquet | Ghép text job, embed bằng Gemini, upsert vector/payload | Collection jobs_v1 |
| CV recommendation | CV PDF/DOCX + filter người dùng | Parse CV, embed query, search Qdrant, hybrid re-rank | Top 10 job phù hợp kèm matched_skills |
| Gold build | Silver Parquet | Dedup latest job_url, unnest list fields, tạo star schema | Tables trong schema gold |
| Monitoring/BI | Bronze/Silver/Gold/metrics | Tổng hợp, trực quan hóa | HTML dashboard, Grafana, Power BI |

---

## 9. Phân tích nguồn dữ liệu

### 9.1. TopCV

TopCV được thiết kế thu thập bằng HTTP crawler bất đồng bộ. Crawler lấy trang kết quả tìm kiếm, trích xuất URL job detail, loại trùng URL, sau đó fetch trang chi tiết theo batch. Cách tiếp cận này nhẹ hơn headless browser và phù hợp vì TopCV không yêu cầu browser rendering phức tạp như hai nguồn còn lại.

Đặc điểm chính:

- Dùng concurrency có giới hạn để tránh quá tải.
- Có request delay và retry/backoff để giảm lỗi 429/5xx.
- Flush dữ liệu theo từng trang vào local temp.
- Có nhiều trường bổ sung như company_size, job_type, experience_level, education_level, job_position và job_deadline.

### 9.2. ITviec

ITviec được thiết kế thu thập bằng trình duyệt headless thông qua nodriver. Website có yêu cầu đăng nhập và cơ chế bảo vệ khiến HTTP request đơn giản không đủ. Crawler dùng một phiên browser duy nhất, đăng nhập, mở trang tìm kiếm, click từng job card, lấy HTML panel và parse bằng parser.

Đặc điểm chính:

- Cần ITVIEC_USERNAME và ITVIEC_PASSWORD trong biến môi trường.
- Không chạy concurrency mạnh vì session đăng nhập và clearance gắn với browser/IP/cookie.
- Một số trường nghiệp vụ không tồn tại đầy đủ như TopCV/VietnamWorks.
- Dữ liệu được flush theo từng trang để tránh mất tiến trình.

### 9.3. VietnamWorks

VietnamWorks cũng được thiết kế dùng nodriver do nội dung danh sách việc làm phụ thuộc JavaScript rendering. Crawler quét URL trên từng trang, loại trùng, sau đó truy cập job detail để parse.

Đặc điểm chính:

- Cần browser automation.
- Có các trường job_type, experience_level, education_level, job_position và job_deadline nhưng có thể null nhiều.
- Bronze source identifier là vietnamworks, đồng bộ với tên file temp và S3 path.

---

## 10. Phân tích mô hình dữ liệu

### 10.1. Mô hình dữ liệu thô ở Crawl/Bronze

Dữ liệu gốc dùng base dataclass JobItem và các dataclass mở rộng theo từng site. Nhóm trường chung gồm:

| Nhóm | Trường tiêu biểu | Ý nghĩa |
|------|------------------|--------|
| Thông tin job | job_title, job_url, source_site, search_keyword, scraped_at | Nhận diện và truy vết job |
| Công ty | company_name, company_size | Thông tin nhà tuyển dụng |
| Địa điểm | location | Địa điểm thô |
| Nội dung | job_description, requirements, benefits | Văn bản mô tả tuyển dụng |
| Phân loại | job_industry | Ngành nghề thô |
| Lương | salary | Thông tin lương thô |
| Thuộc tính bổ sung | job_type, experience_level, education_level, job_position, job_deadline | Tùy site |

### 10.2. Mô hình dữ liệu Silver

SilverJobItem là schema trung tâm của tầng Silver. Các trường được tổ chức theo nhóm:

| Nhóm | Trường tiêu biểu | Ý nghĩa |
|------|------------------|--------|
| Metadata | job_url, search_keyword, job_deadline | Truy vết và thời hạn |
| Job title | job_title, clean_job_title, job_title_special_keywords | Tiêu đề gốc, tiêu đề sạch, keyword đặc biệt |
| Company | clean_company_name, company_size, min_company_size, max_company_size | Chuẩn hóa công ty và quy mô |
| Location | location, clean_location, is_vietnam | Địa điểm gốc và địa điểm chuẩn hóa |
| Industry | job_industry_clean, job_industry_unmapped | Ngành nghề đã map và phần chưa map |
| Job details | job_type, job_position | Loại công việc và vị trí |
| Experience/Education | experience_level, min_exp_level, max_exp_level, education_level | Kinh nghiệm và học vấn |
| Salary/Benefits | salary, min_monthly_salary, max_monthly_salary, benefits_categories_vi | Lương và phúc lợi |
| Description | job_description, job_description_cleaned | Mô tả gốc và sạch |
| Requirements/Skills | requirements_cleaned, require_* | Kỹ năng và yêu cầu đã trích xuất theo taxonomy |

Các trường dạng list như require_programming_languages, require_frameworks, require_tools, require_cloud_skills, require_knowledge, require_domain_knowledge, require_foreign_languages và require_domain_university giúp hệ thống phân tích kỹ năng theo nhóm.

### 10.3. Mô hình dữ liệu Supabase

Supabase chỉ lưu projection nhẹ phục vụ tìm kiếm việc làm:

| Trường | Ý nghĩa |
|--------|--------|
| job_url | Khóa duy nhất để UPSERT |
| job_title | Tiêu đề việc làm đã dùng cleaned title |
| company_name | Tên công ty đã chuẩn hóa |
| location | Địa điểm đã chuẩn hóa |
| job_deadline | Hạn nộp hồ sơ |
| job_title_special_keywords | Kỹ năng/keyword nổi bật trong tiêu đề |
| source_site | Nguồn dữ liệu |

### 10.4. Mô hình dữ liệu Gold

Gold layer dùng mô hình sao phục vụ BI:

| Bảng | Vai trò |
|------|--------|
| gold.jobs | Fact table chính, mỗi job_url giữ bản mới nhất |
| gold.dim_date | Dimension thời gian, khóa date_key dạng YYYYMMDD |
| gold.job_industries | Bridge table từ list job_industry_clean |
| gold.job_benefits | Bridge table từ list benefits_categories_vi |
| gold.job_requirements | Bridge table từ nhiều nhóm require_* và job_title_special_keywords |
| gold.dim_*_taxonomy | Dimension taxonomy nạp từ seed CSV |

### 10.5. Mô hình dữ liệu Recommendation/Qdrant

Tầng gợi ý CV sử dụng Qdrant collection jobs_v1 làm chỉ mục vector cho các tin tuyển dụng còn hiệu lực. Mỗi điểm vector đại diện cho một job_url và được tạo từ nội dung đã làm sạch ở Silver.

| Nhóm payload | Trường tiêu biểu | Vai trò |
|--------------|------------------|--------|
| Định danh job | job_url, source_site | Liên kết kết quả gợi ý về tin tuyển dụng gốc |
| Hiển thị | job_title, company_name, clean_location | Trả thông tin cần hiển thị trực tiếp cho frontend |
| Filter | clean_location, min_exp_level, max_exp_level, deadline_ts | Lọc cứng theo lựa chọn người dùng và loại job hết hạn |
| Lương | min_monthly_salary, max_monthly_salary | Cho phép mở rộng filter/ranking theo lương |
| Kỹ năng | require_programming_languages, require_frameworks, require_tools, require_cloud_skills, require_knowledge, require_domain_knowledge, require_foreign_languages | Tính skill_overlap và matched_skills để giải thích kết quả |

Vector job dùng mô hình gemini-embedding-001, dimension 768 và distance metric Cosine. Point id được sinh ổn định theo job_url để việc upsert hằng ngày không tạo trùng.

---

## 11. Quy tắc nghiệp vụ và xử lý dữ liệu quan trọng

| Quy tắc | Mô tả |
|--------|------|
| Append temp theo ngày | Crawler ghi nối vào cùng file source_jobs_YYYYMMDD.jsonl trong ngày |
| Bronze upload có tính destructive local | Sau upload Bronze thành công, file local của source bị xóa để tránh upload lặp |
| Bronze giữ raw data | Bronze không làm sạch sâu, chỉ nén và phân vùng dữ liệu thô |
| Silver đọc theo ngày | Silver pipeline xử lý từng ngày trong khoảng from_date/to_date |
| Dòng thiếu title/company bị loại | Các dòng thiếu job_title hoặc company_name không có giá trị downstream |
| Silver schema là contract | Mọi DataFrame sau cleaning phải khớp SilverJobItem |
| Taxonomy ưu tiên Google Sheets | Nếu cấu hình Google Sheets tồn tại thì đọc từ Sheets, lỗi thì fallback CSV local |
| Supabase UPSERT theo job_url | Đảm bảo chạy lại không tạo record trùng |
| Gold full refresh | Gold tables được CREATE OR REPLACE mỗi lần chạy |
| Gold chọn bản mới nhất | Nếu job_url xuất hiện nhiều lần, Gold lấy snapshot mới nhất theo ngày/file |
| Qdrant idempotent upsert | Point id dựa trên job_url nên index lại không nhân bản vector |
| Recommendation hybrid scoring | Kết quả CV recommendation kết hợp semantic cosine và overlap kỹ năng taxonomy |
| CV retention | CV upload được lưu ở S3 riêng và tự xóa sau 30 ngày theo lifecycle |

---

## 12. Phân tích chất lượng dữ liệu

### 12.1. Kiểm tra trước Bronze

Validation local temp dùng Great Expectations để kiểm tra dữ liệu JSONL mới nhất theo từng source. Các rule chính:

- Kiểm tra file JSONL có đọc được bằng pandas.
- Kiểm tra các cột required đạt tỷ lệ non-null tối thiểu.
- Cho phép optional fields có tỷ lệ null cao hơn vì không phải site nào cũng cung cấp đầy đủ.
- Một số trường có rule riêng theo đặc thù site, ví dụ company_size của TopCV có thể null với brand URL.

### 12.2. Làm sạch ở Silver

Silver layer xử lý chất lượng ở mức sâu hơn:

- Chuẩn hóa URL.
- Chuẩn hóa tên công ty và mapping canonical company name.
- Chuẩn hóa địa điểm và xác định Việt Nam/không Việt Nam.
- Chuẩn hóa salary thành min_monthly_salary và max_monthly_salary.
- Chuẩn hóa kinh nghiệm thành min_exp_level và max_exp_level.
- Làm sạch HTML/bullet trong mô tả, yêu cầu và phúc lợi.
- Trích xuất kỹ năng bằng FlashText/taxonomy.
- Chuẩn hóa ngành nghề thành multi-label list.
- Ép schema cuối cùng để đảm bảo downstream ổn định.

---

## 13. Phân tích điều phối pipeline

Airflow không import trực tiếp logic business mà chỉ gọi các command trong container lakehouse-crawler thông qua DockerOperator. Cách thiết kế này giúp tách orchestration khỏi logic xử lý dữ liệu.

| DAG | Lịch chạy | Mục đích |
|-----|-----------|----------|
| crawl_topcv | 0 */3 * * * | Crawl TopCV mỗi 3 giờ |
| crawl_itviec | 15 */3 * * * | Crawl ITviec mỗi 3 giờ, lệch 15 phút |
| crawl_vietnamworks | 30 */3 * * * | Crawl VietnamWorks mỗi 3 giờ, lệch 30 phút |
| validate_bronze_<site> | Triggered | Validate local temp và upload Bronze cho từng site |
| silver_<site> | 0 */8 * * * | Làm sạch Bronze thành Silver cho từng site |
| supabase_load_all | 0 */6 * * * | Load Silver vào Supabase cho tất cả site |
| load_silver_to_gold | 0 */6 * * * | Xây Gold layer trong MotherDuck |
| load_taxonomy_to_gold | Manual | Nạp taxonomy CSV vào Gold dimension tables |
| index_qdrant | Hằng ngày | Index Silver job text và payload sang Qdrant phục vụ CV recommendation |
| generate_bronze_dashboard | 0 0 * * * | Sinh dashboard Bronze hằng ngày |
| generate_silver_dashboard | 0 0 * * * | Sinh dashboard Silver hằng ngày |
| cluster_company_name | Manual | Cluster tên công ty bằng fuzzy matching |

---

## 14. Phân tích giám sát và báo cáo

### 14.1. Monitoring vận hành

Hệ thống monitoring gồm:

- Airflow phát metrics qua StatsD.
- StatsD Exporter chuyển metrics sang Prometheus format.
- Prometheus scrape metrics.
- Grafana hiển thị dashboard vận hành Airflow và Supabase.
- Caddy reverse proxy và bảo vệ truy cập monitoring.

Trong phương án triển khai được lựa chọn, orchestration/pipeline và monitoring được tách trên hai EC2. EC2-A chạy Airflow, Postgres metadata, StatsD Exporter và các container lakehouse-crawler. EC2-B chạy Prometheus, Grafana, Caddy và Nginx dashboards. Prometheus trên EC2-B scrape endpoint StatsD Exporter publish từ EC2-A qua private IP/security group nội bộ.

### 14.2. Dashboard nghiệp vụ dữ liệu

Bronze và Silver dashboard là HTML tĩnh được sinh từ dữ liệu S3. Dashboard giúp kiểm tra:

- Dung lượng object theo source.
- Tình trạng dữ liệu theo ngày.
- Freshness của dữ liệu.
- Số lượng file hoặc bản ghi theo tầng.

Các file HTML dashboard được ghi vào thư mục src/monitoring_layer/business/reports. Thư mục này được thiết kế mount bằng Amazon EFS trên cả EC2-A và EC2-B để container pipeline trên EC2-A có thể ghi báo cáo, trong khi Nginx dashboards trên EC2-B phục vụ cùng nội dung cho người vận hành.

### 14.3. BI report

Power BI đọc dữ liệu Gold hoặc dữ liệu phân tích đã được chuẩn bị để xây báo cáo về thị trường tuyển dụng, ví dụ phân bố job theo nguồn, địa điểm, kỹ năng, mức lương, ngành nghề và thời gian.

---

## 15. Ràng buộc và giả định

### 15.1. Ràng buộc kỹ thuật

| Ràng buộc | Mô tả |
|----------|------|
| Import tuyệt đối từ src | Repository không có __init__.py ở src, phải chạy command từ repo root |
| Cần Chrome/headless browser | ITviec và VietnamWorks cần nodriver/Chrome, container cần shm_size đủ lớn |
| Bucket S3 cấu hình qua `.env` | `S3_BRONZE_BUCKET` và `S3_SILVER_BUCKET` phải là tên bucket unique trong AWS account |
| .env không commit | Credential bắt buộc phải cấu hình ngoài Git |
| Không có test/lint formal | Verification chủ yếu bằng dry-run, max-pages nhỏ và dashboard/log |
| Phụ thuộc dịch vụ AI/vector | Recommendation cần Gemini API và Qdrant Cloud hoạt động ổn định |
| Triển khai multi-host | Hai EC2 cần security group và EFS mount chính xác để monitoring đọc được reports |

### 15.2. Giả định dữ liệu

- job_url là định danh tương đối ổn định cho một tin tuyển dụng.
- Website có thể thay đổi HTML, do đó parser có thể cần bảo trì định kỳ.
- Một số trường có thể null tùy website và tùy job.
- Taxonomy không bao phủ toàn bộ kỹ năng/ngành nghề, do đó cần trường unmapped để đánh giá bổ sung.

---

## 16. Rủi ro cần kiểm soát

| Rủi ro | Ảnh hưởng | Hướng giảm thiểu đề xuất |
|-------|-----------|--------------------------|
| Website thay đổi DOM | Crawler/parser lỗi hoặc thiếu trường | Tách parser theo site, theo dõi validation và dashboard |
| Anti-bot hoặc login fail | Không crawl được ITviec/VietnamWorks | Dùng nodriver, session browser, credential trong .env |
| Dữ liệu trùng do crawl nhiều lần | Bản ghi trùng ở Bronze/Silver | Supabase dedup/UPSERT; Gold chọn latest job_url |
| Taxonomy chưa đầy đủ | Kỹ năng/ngành nghề bị unmapped | Dùng Google Sheets để cập nhật taxonomy dễ hơn |
| Xóa temp sau Bronze | Mất local temp nếu cần debug sau upload | Bronze đã giữ bản raw trên S3; nên kiểm tra trước upload |
| Thiếu test tự động | Lỗi cleaning khó phát hiện sớm | Dùng --no_save, --export_parquet, dashboard và validation |
| Chi phí/credential cloud | Pipeline phụ thuộc AWS/Supabase/MotherDuck | Quản lý .env, IAM tối thiểu, theo dõi usage |
| Rate-limit Gemini/Qdrant | Index hoặc recommendation chậm khi vượt giới hạn free tier | Batch embedding, retry/backoff, giới hạn tần suất endpoint /recommend |
| CV chứa PII | Rò rỉ thông tin cá nhân nếu log/lưu trữ không đúng | Không log nội dung CV, lưu S3 bucket riêng, lifecycle 30 ngày |

---

## 17. Kết luận phân tích

Từ góc độ phân tích trước triển khai, Lakehouse-Lite được xác định là một hệ thống dữ liệu tuyển dụng tương đối đầy đủ theo mô hình Lakehouse hiện đại. Hệ thống cần bao phủ các năng lực cốt lõi: crawl đa nguồn, lưu trữ raw/cleaned data, kiểm tra chất lượng, chuẩn hóa dữ liệu bằng taxonomy, phục vụ dữ liệu cho ứng dụng, gợi ý việc làm theo CV bằng vector search, xây dựng Gold layer cho BI, điều phối tự động và giám sát vận hành.

Điểm mạnh chính của phương án là phân tầng rõ ràng, lưu trữ dữ liệu thô để có thể tái xử lý, schema Silver tập trung, pipeline Airflow tách biệt logic business và khả năng phục vụ đồng thời ba nhu cầu: ứng dụng tìm kiếm qua Supabase, gợi ý cá nhân hóa theo CV qua Qdrant/Gemini và phân tích BI qua MotherDuck/Power BI.

Các điểm cần lưu ý trong quá trình triển khai và vận hành gồm bổ sung test tự động, tăng khả năng cảnh báo khi crawler/parser lỗi, chuẩn hóa thêm taxonomy, quản lý bucket/config linh hoạt hơn, tối ưu chi phí gọi embedding và bổ sung đánh giá relevance định lượng cho recommendation.
