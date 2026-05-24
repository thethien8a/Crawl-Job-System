import polars as pl
import unicodedata

from src.storage_layer.MinIO_S3.layer.silver.cleaning.common.map_company_name import (
    map_canonical_company,
    RAW_COLUMN,
)

def normalize_unicode(df: pl.DataFrame, column_name: str) -> pl.DataFrame:
    # Vietnamese có 2 dạng tổ hợp dấu (NFC vs NFD); chuẩn hoá để dedup hoạt động chính xác
    return df.with_columns(
        pl.col(column_name)
        .cast(pl.String)
        .map_elements(lambda x: unicodedata.normalize("NFC", x) if x is not None else None, return_dtype=pl.String)
        .alias(column_name)
    )


# Bước 1
def clean_1(df: pl.DataFrame, column_name: str, new_column_name: str = RAW_COLUMN) -> pl.DataFrame:
    """
    Làm sạch tên công ty: Chỉ xóa loại hình pháp lý, giữ lại lĩnh vực kinh doanh và tên riêng.
    """
    # 1. Các từ chỉ loại hình pháp lý / cấu trúc tổ chức (Tuyệt đối không đưa "công nghệ", "giáo dục"... vào đây)
    # Sắp xếp các cụm từ dài lên trước để Regex ưu tiên match cụm dài (ví dụ: "công ty tnhh mtv" trước "công ty tnhh")
    legal_pattern = r'(?i)\b(ngân hàng thương mại cổ phần|ngân hàng tmcp|trách nhiệm hữu hạn|một thành viên|1 thành viên|công ty tnhh mtv|công ty tnhh|công ty cổ phần|công ty cp|tổng công ty|tổng cty|cty tnhh|cty cp|cttnhh|ctcp|công ty|cty|tập đoàn|chi nhánh|văn phòng đại diện|văn phòng|trung tâm|chuỗi nhà hàng|tnhh|mtv|cổ phần|cp)\b'

    # 2. Hậu tố tiếng Anh, URL, từ nối không cần thiết
    # Loosen "co., ltd" -> "co.?,?\s*ltd" để vẫn match được "CO LTD" sau khi clean_2 đã strip dấu chấm/phẩy
    suffix_and_noise_pattern = r'(?i)\b(https?://[^\s]+|member of viettel group|toàn cầu|corporation|corporate|limited|company|group|co\.?,?\s*ltd|ltd\.?|jsc\.,?|jsc|corp\.|corp|inc\.|inc|và|số|tm\s*&\s*dv|tmdv|tm\s*dv)\b'

    # 3. Ký tự đặc biệt
    special_chars_pattern = r'[\.\(\)\[\]\-\,\/\|]'

    return df.with_columns(
        pl.col(column_name).cast(pl.String)
        .str.replace_all(suffix_and_noise_pattern, ' ')
        .str.replace_all(legal_pattern, ' ')
        .str.replace_all(special_chars_pattern, ' ')
        .str.replace_all(r'\s+', ' ')
        .str.strip_chars()
        .str.to_uppercase()
        .alias(new_column_name)
    )


# Bước 2
def clean_2(df: pl.DataFrame, column_name: str = RAW_COLUMN) -> pl.DataFrame:

    return df.with_columns(
        pl.col(column_name)
        # 1. Xóa URL trước
        .str.replace_all(r'(?i)https?://\S+|www\.\S+', '')
        # 2. Xóa text trong ngoặc — phải chạy TRƯỚC clean_1 để loại "(SHB)", "(VIB)"...
        .str.replace_all(r'\([^)]*\)', '')
        # 3. Xóa ký tự đặc biệt — bổ sung _ " ' & dấu nháy Unicode và em/en-dash – —
        .str.replace_all(r'[\.\(\)\[\]\-\,\/\|_"\'’‘“”&–—]', ' ')
        # 4. Normalize whitespace
        .str.replace_all(r'\s+', ' ')
        .str.strip_chars()
        .alias(column_name)
    )


def main_clean_company(df: pl.DataFrame, column_name: str = RAW_COLUMN) -> pl.DataFrame:
    """Run the full company-name cleaning chain in the only correct order.

    NFC -> clean_2 (strip URLs, "(SHB)"-style bracketed text, special chars
    while keeping case) -> clean_1 (strip legal forms + uppercase) ->
    map_canonical_company (exact-join with the seed mapping to add
    `company_name_canonical`).

    `clean_2` must run BEFORE `clean_1` so bracketed text is dropped while
    the parentheses are still intact; otherwise `clean_1` strips just the
    parens and leaves the inner text behind.
    """
    df = normalize_unicode(df, column_name)
    df = clean_2(df, column_name)
    df = clean_1(df, column_name, new_column_name=column_name)
    df = map_canonical_company(df, source_col=column_name)
    return df
