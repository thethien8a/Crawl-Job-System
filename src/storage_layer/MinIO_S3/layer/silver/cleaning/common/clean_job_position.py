from __future__ import annotations
import polars as pl
from src.storage_layer.MinIO_S3.layer.silver.utils.normalize_data import remove_accents_col


def clean_job_position(df: pl.DataFrame, column_name: str = "job_position") -> pl.DataFrame:
    """
    Chuẩn hóa job_position thành:
        - Giám đốc           : C-level, Executive, Director, Trưởng chi nhánh, Founder, Chairman...
        - Trưởng phòng       : Department head/manager, Trưởng/Phó phòng, Quản lý...
        - Trưởng nhóm        : Team lead/Supervisor, Trưởng nhóm, Giám sát, Quản lý / Giám sát...
        - Nhân viên          : Staff/Officer, Nhân viên/Chuyên viên, Senior/Junior IC roles...
        - Mới tốt nghiệp     : Fresh graduate, Fresher, Entry level (tách riêng để phân tích HR rõ hơn)
        - Thực tập sinh      : Intern, Sinh viên
        - Không đề cập       : các giá trị không khớp

    Thứ tự kiểm tra (specific -> general) để xử lý các giá trị mơ hồ:
        1. Thực tập sinh   - cụm từ riêng biệt, kiểm tra trước để intern không bị nuốt bởi staff.
        2. Mới tốt nghiệp  - tách riêng khỏi "Nhân viên" so với macro SQL gốc.
        3. Giám đốc        - chứa "giam doc" / "director" / chief; phải đứng trước team lead
                             vì "giam doc" và "giam sat" có tiền tố giống nhau.
        4. Trưởng nhóm     - chứa "giam sat" / "lead" / "supervisor"; đứng TRƯỚC Trưởng phòng
                             để "Quản lý / Giám sát" được phân loại đúng là supervisor,
                             khác với SQL gốc (vốn xếp nhầm thành Trưởng phòng do match "quan ly").
        5. Trưởng phòng    - manager / quan ly / truong phong / pho phong.
        6. Nhân viên       - phần còn lại của staff / specialist / senior / junior IC.
    """

    df = remove_accents_col(df, column_name, "job_position_no_accent")
    t = pl.col("job_position_no_accent").str.to_lowercase()

    df = df.with_columns(
        pl.when(t.str.contains(r"(?i)(thuc tap sinh|sinh vien|intern|internship|student)"))
        .then(pl.lit("Thực tập sinh"))
        .when(t.str.contains(r"(?i)(moi tot nghiep|fresh graduate|fresher|\bfresh\b|entry[ -]?level|new grad)"))
        .then(pl.lit("Mới tốt nghiệp"))
        .when(t.str.contains(
            r"(?i)(giam doc|pho giam doc|tong giam doc|cap cao hon|truong chi nhanh|"
            r"c[- ]?level|chief|executive|\bceo\b|\bcto\b|\bcfo\b|\bcoo\b|\bcmo\b|\bciso\b|"
            r"founder|co[- ]?founder|chairman|president|vice president|\bvp\b|director|head of)"
        ))
        .then(pl.lit("Giám đốc"))
        .when(t.str.contains(r"(?i)(truong nhom|to truong|giam sat|supervisor|team lead|team leader|\blead\b|leader)"))
        .then(pl.lit("Trưởng nhóm"))
        .when(t.str.contains(
            r"(?i)(truong phong|pho phong|truong/pho phong|truong bo phan|pho bo phan|"
            r"quan ly|manager|\bmgr\b|head)"
        ))
        .then(pl.lit("Trưởng phòng"))
        .when(t.str.contains(
            r"(?i)(nhan vien|chuyen vien|staff|officer|employee|associate|"
            r"specialist|consultant|\bsenior\b|\bsr\b|\bjunior\b|\bjr\b|\bmid\b)"
        ))
        .then(pl.lit("Nhân viên"))
        .otherwise(pl.lit("Không đề cập"))
        .alias("job_position")
    )

    df = df.drop("job_position_no_accent")
    return df
