import re
import unicodedata
import polars as pl

def remove_vietnamese_accents(text: str) -> str:
    """
    Loại bỏ dấu tiếng Việt khỏi chuỗi.
    """
    if text is None:
        return None
    # Xử lý riêng chữ đ/Đ vì unicodedata không tự loại bỏ được
    text = re.sub(r'[đĐ]', lambda m: 'd' if m.group(0) == 'đ' else 'D', text)
    # Tách các ký tự có dấu thành ký tự gốc và dấu (NFD), sau đó loại bỏ các ký tự dấu
    text = unicodedata.normalize('NFD', text)
    text = re.sub(r'[\u0300-\u036f]', '', text)
    return text

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