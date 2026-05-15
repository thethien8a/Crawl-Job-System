import re
import unicodedata

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