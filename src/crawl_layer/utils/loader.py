from dataclasses import asdict
import json
from datetime import datetime
from typing import Union, List, Dict

# Xác định đường dẫn tương đối để luôn trỏ đúng về thư mục temp
from src.crawl_layer.config.path import TEMP_DIR

def save_to_temp(data: Union[Dict, List[Dict]], source_name: str, entity_name: str = 'jobs'):
    """
    Lưu dữ liệu cào được vào thư mục temp cục bộ dưới dạng JSON Lines (.jsonl).
    Dữ liệu sẽ được tự động gom nhóm theo ngày hiện tại.
    
    :param data: Dữ liệu cần lưu (dict hoặc list các dicts).
    :param source_name: Nguồn cào (ví dụ: 'itviec', 'linkedin', 'topcv').
    :param entity_name: Loại thực thể (ví dụ: 'jobs', 'companies'). Mặc định là 'jobs'.
    """
    TEMP_DIR.mkdir(parents=True, exist_ok=True)
    
    # Lấy ngày hiện tại (YYYYMMDD) để gom file theo ngày ngay từ local
    today_str = datetime.now().strftime("%Y%m%d")
    
    # Định dạng tên file: source_entity_YYYYMMDD.jsonl (vd: itviec_jobs_20260512.jsonl)
    file_name = f"{source_name}_{entity_name}_{today_str}.jsonl"
    file_path = TEMP_DIR / file_name
    
    # Đảm bảo data luôn là một list để tiện xử lý vòng lặp
    if isinstance(data, dict):
        data = asdict(data)
        
    # Mở file mode 'a' (append) để ghi nối tiếp dữ liệu mới vào cuối file
    with open(str(file_path), 'a', encoding='utf-8') as f:
        for item in data:
            # Ghi mỗi dict thành 1 chuỗi JSON trên 1 dòng
            f.write(json.dumps(item, ensure_ascii=False) + '\n')
            
    return file_path
