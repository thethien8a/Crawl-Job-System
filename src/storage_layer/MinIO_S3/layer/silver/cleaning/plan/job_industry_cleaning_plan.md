# Kế Hoạch Xử Lý Dữ Liệu Job Industry (Ngành Nghề)

## 1. Bối cảnh & Vấn đề
- Dữ liệu crawl về từ các nguồn (như TopCV) ở trường `job_industry` thường là các chuỗi kết hợp nhiều ngành nghề cách nhau bởi dấu phẩy. VD: `"Kế toán / Kiểm toán, Hành chính / Văn phòng, Dịch vụ khách hàng"`.
- Việc cố gắng tạo mapping 1-1 cho các tổ hợp chuỗi này (Combinatorics) trong file `industry_taxonomy.csv` là **anti-pattern**, dẫn đến số lượng rule bùng nổ và không thể maintain trong dài hạn.

## 2. Giải pháp Kiến trúc (Pattern: Split ➔ Explode ➔ Map ➔ Group)
Thay vì mapping toàn bộ chuỗi dài, quá trình ETL từ Bronze lên Silver sẽ thực hiện theo các bước sau bằng Polars:

1. **Giữ nguyên Bronze (Immutable):** Dữ liệu JSONL gốc vẫn giữ đúng nguyên trạng chuỗi raw ban đầu.
2. **Split & Explode:** 
   - Cắt chuỗi `job_industry` gốc bằng dấu phẩy `,`.
   - Explode danh sách vừa cắt để đưa mỗi ngành (keyword) xuống thành 1 dòng (row) biệt lập. Trim khoảng trắng 2 đầu.
3. **Map đơn lẻ:** Match từng dòng ngành đơn lẻ đó với các quy tắc từ khoá trong `industry_taxonomy.csv`.
4. **Group by Array:** Gom nhóm trở lại theo `job_url` hoặc `job_id` (`group_by().agg()`) để tái tạo lại thành cấu trúc List chứa các ngành nghề chuẩn.

## 3. Chiến lược Fallback & Lưu vết (Data Quality Tracking)
Để giải quyết bài toán "khi cập nhật taxonomy sau này", lớp Silver sẽ thiết kế đầu ra của job_industry gồm 2 cột chính:
- `job_industry_clean`: List chứa các ID/Tên ngành đã map thành công. Nếu một từ không map được, fallback về giá trị mặc định là `"Others"`.
- `job_industry_unmapped`: List chứa chính xác các từ gốc (raw values) bị rớt (không map được vào taxonomy).
  
> **Lợi ích:** Data Engineer/Analyst có thể dễ dàng query cột `job_industry_unmapped` bằng SQL/Polars để lấy ra danh sách các từ còn thiếu, đem bỏ vào `industry_taxonomy.csv` để chuẩn hóa dần hệ thống.

## 4. Chiến lược Cập nhật Dữ liệu (Reprocessing)
Khi có sự thay đổi hoặc bổ sung trong file `industry_taxonomy.csv`:
- **KHÔNG BẮT BUỘC** phải viết script UPDATE hay PATCH chạy trực tiếp trên file Parquet của Silver.
- **Tận dụng Data Lakehouse Reprocessing:** Chạy lại (Re-run) pipeline `etl_bronze_to_silver`.
- **Thực thi Incremental:** Script ETL nhận tham số thời gian (`--start-date`, `--end-date`). Khi taxonomy thay đổi, chỉ cần chạy lại ETL đè lên các partition (folder `year=YYYY/month=MM`) tương ứng chứa dữ liệu cần sửa. Polars sẽ load lại raw data từ Bronze, map với taxonomy mới và ghi đè Parquet ở Silver rất nhanh chóng (thường dưới vài phút).

## 5. Kế hoạch Triển khai (Implementation)
1. Cập nhật file `clean_job_industry.py` ở thư mục `common/` (hoặc cụ thể trong `clean_topcv/`) với logic Polars như đã thống nhất.
2. Đảm bảo Output schema của lớp Silver ở cột `job_industry` có định dạng là kiểu mảng `pl.List(pl.String)`.
3. Bổ sung tham số thời gian (Start/End date) vào file `etl_bronze_to_silver.py` để hỗ trợ cơ chế Incremental Reprocessing.
