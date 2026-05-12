import os
import glob
import logging

# Cấu hình logging cơ bản
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Xác định đường dẫn tương đối trỏ về thư mục temp
from src.crawl_layer.config.path import TEMP_DIR

def clean_temp_directory():
    """
    Xóa tất cả các file trong thư mục temp.
    Hàm này thường được gọi sau khi script upload dữ liệu lên MinIO báo thành công.
    """
    if not os.path.exists(TEMP_DIR):
        logging.info(f"Thư mục {TEMP_DIR} không tồn tại. Bỏ qua dọn dẹp.")
        return

    # Lấy danh sách tất cả các file trong temp
    files = glob.glob(os.path.join(TEMP_DIR, '*'))
    
    if not files:
        logging.info("Thư mục temp đang trống. Không có gì để xóa.")
        return

    deleted_count = 0
    for file_path in files:
        # Chỉ xóa file, bỏ qua nếu vô tình có thư mục con
        if os.path.isfile(file_path):
            try:
                os.remove(file_path)
                logging.info(f"Đã xóa: {os.path.basename(file_path)}")
                deleted_count += 1
            except Exception as e:
                logging.error(f"Không thể xóa file {file_path}. Lỗi: {e}")
                
    logging.info(f"Hoàn thành dọn dẹp! Tổng cộng đã xóa {deleted_count} file.")

if __name__ == "__main__":
    # Cho phép chạy script này độc lập từ terminal
    clean_temp_directory()
