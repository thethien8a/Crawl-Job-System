import polars as pl
from src.storage_layer.MinIO_S3.layer.silver.utils.normalize_data import remove_vietnamese_accents

def remove_accents_col(df: pl.DataFrame, column_name: str, new_column_name: str) -> pl.DataFrame:
    """
    Hàm apply loại bỏ dấu tiếng Việt cho toàn bộ một cột trong Polars DataFrame.
    """
    return df.with_columns(
        pl.col(column_name)
        .cast(pl.String)
        .map_elements(remove_vietnamese_accents, return_dtype=pl.String)
        .alias(new_column_name)
    )

def extract_job_keywords(df: pl.DataFrame, title_no_accent_col: str = "job_title_no_accent", keyword_col: str = "job_title_special_keyword") -> pl.DataFrame:
    """
    Trích xuất cấp bậc (Seniority/Level) và phân loại chức danh công việc thành các vị trí chuẩn trong ngành Dữ liệu.
    Gộp chung thành một cột list chứa các keyword. (ví dụ: ["Senior", "Data Analyst"])
    Sử dụng cột đã được loại bỏ dấu tiếng Việt để so khớp dễ dàng hơn.
    """
    title_lower = pl.col(title_no_accent_col).str.to_lowercase()
    
    # 1. Biểu thức trích xuất Level
    level_expr = (
        pl.when(title_lower.str.contains(r'(?i)(intern|thuc tap sinh)')).then(pl.lit("Intern"))
        .when(title_lower.str.contains(r'(?i)(fresher|moi ra truong)')).then(pl.lit("Fresher"))
        .when(title_lower.str.contains(r'(?i)\bjunior\b')).then(pl.lit("Junior"))
        .when(title_lower.str.contains(r'(?i)\b(middle|mid-level)\b')).then(pl.lit("Middle"))
        .when(title_lower.str.contains(r'(?i)\b(senior|cvcc|chuyen vien cap cao|chuyen vien chinh)\b')).then(pl.lit("Senior"))
        .when(title_lower.str.contains(r'(?i)\b(lead|manager|truong nhom|truong phong|truong bo phan|giam doc)\b')).then(pl.lit("Manager/Lead"))
        .otherwise(pl.lit(None))
    )
    
    # 2. Biểu thức trích xuất Role (Từng thành phần riêng biệt)
    role_domain_expr = (
        pl.when(title_lower.str.contains(r'(data|du lieu)')).then(pl.lit("Data"))
        .when(title_lower.str.contains(r'(business|nghiep vu)')).then(pl.lit("Business"))
        .when(title_lower.str.contains(r'(machine learning|\bml\b|hoc may)')).then(pl.lit("Machine Learning"))
        .when(title_lower.str.contains(r'(artificial intelligence|\bai\b|tri tue nhan tao)')).then(pl.lit("AI"))
        .when(title_lower.str.contains(r'(database|co so du lieu|\bdba\b)')).then(pl.lit("Database"))
        .when(title_lower.str.contains(r'(analytics)')).then(pl.lit("Analytics"))
        .otherwise(pl.lit(None))
    )

    role_action_expr = (
        pl.when(title_lower.str.contains(r'(scientist|science|nha khoa hoc)')).then(pl.lit("Scientist"))
        .when(title_lower.str.contains(r'(engineer|ky su)')).then(pl.lit("Engineer"))
        .when(title_lower.str.contains(r'(analyst|phan tich)')).then(pl.lit("Analyst"))
        .when(title_lower.str.contains(r'(administrator|quan tri)')).then(pl.lit("Administrator"))
        .otherwise(pl.lit(None))
    )
    
    # 3. Gộp các biểu thức thành 1 List, sau đó loại bỏ các giá trị None trong List
    return df.with_columns(
        pl.concat_list([level_expr, role_domain_expr, role_action_expr]).list.drop_nulls().alias(keyword_col)
    )

def clean_job_title(df: pl.DataFrame, column_name: str = "job_title", new_column_name: str = "clean_job_title") -> pl.DataFrame:
    """
    Làm sạch chức danh công việc bằng cách loại bỏ nhiễu: 
    - Lương (Upto, Up to)
    - Địa điểm, phòng ban (Hà Nội, Khối, Phòng)
    - Các tag ([Hot Job], [Hybrid])
    """
    # 1. Các thẻ tags [Hot Job], [Hybrid]
    tag_pattern = r'\[.*?\]'
    
    # 2. Thông tin về lương
    salary_pattern = r'(?i)(-\s*)?(up\s*to|upto|lương|salary|thu nhập)\s*[:\d].*?(\)|$)'
    
    # 3. Nội dung nằm trong ngoặc (kỹ năng, tiếng Anh phụ đề, phòng ban) 
    bracket_pattern = r'\(.*?\)'
    
    # 4. Các cụm theo sau dấu gạch ngang (phòng ban, địa điểm, domain)
    department_pattern = r'(?i)\s*-\s*(khối|phòng|ban|domain|k\.cntt|hà nội|hồ chí minh|tmo|\d+ năm|game|snop).*'
    
    # 5. Cụm tiền tố thừa như CV/CVCC, Chuyên Viên/, Chuyên Viên Chính
    prefix_pattern = r'(?i)^(cv/cvcc|chuyên viên/|chuyên viên chính|cv/ cvcc)\s*'

    return df.with_columns(
        pl.col(column_name).cast(pl.String)
        .str.replace_all(tag_pattern, ' ', literal=False)
        .str.replace_all(salary_pattern, '', literal=False)
        .str.replace_all(department_pattern, '', literal=False)
        .str.replace_all(bracket_pattern, '', literal=False)
        .str.replace_all(prefix_pattern, '', literal=False)
        .str.replace_all(r'\s+', ' ', literal=False)
        .str.strip_chars()
        .str.to_titlecase()
        .alias(new_column_name)
    )
    
# Hàm chuẩn hóa tổng hợp
def process_job_title_pipeline(df: pl.DataFrame, title_col: str = "job_title") -> pl.DataFrame:
    # 1. Tạo cột tạm không dấu
    df = remove_accents_col(df, column_name=title_col, new_column_name="job_title_no_accent")
    
    # 2. Sử dụng cột không dấu để extract các keyword gộp chung vào 1 List
    df = extract_job_keywords(df, title_no_accent_col="job_title_no_accent", keyword_col="job_title_special_keyword")
    
    # 3. Clean string trên cột gốc (để giữ được dấu tiếng Việt chuẩn)
    df = clean_job_title(df, column_name=title_col, new_column_name="clean_job_title")
    
    # 4. Xóa cột tạm
    df = df.drop("job_title_no_accent")
    
    return df
