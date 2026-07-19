import asyncio
import json
import os
import sys
from dataclasses import asdict
from datetime import datetime
# Thêm đường dẫn gốc vào sys.path để có thể import từ src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

from src.crawl_layer.crawler.topcv.http_client import TopcvHttpClient
from src.crawl_layer.crawler.topcv.parser import TopcvParser
import logging

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

async def get_html():
    url = "https://www.topcv.vn/viec-lam/chuyen-vien-tu-van-tuyen-sinh-sales-khoa-hoc-cham-soc-hoc-vien-15-22tr-thang-data-san-khong-ep-doanh-so-di-lam-ngay/2241105.html?ta_source=JobSearchList_LinkDetail&u_sr_id=d4fW9dR6M4Gz2RDOIgfnEZvSrG3fJq1emChatISr_1784437508"

    logger.info("Đang tải dữ liệu từ: %s", url)
    logger.info("Sử dụng TopcvHttpClient (curl_cffi với impersonate) để vượt qua chặn bot...")
    
    # Khởi tạo HTTP client được thiết kế sẵn trong project (có impersonation)
    async with TopcvHttpClient(concurrency=1, max_retries=3, request_delay=(1.0, 2.0)) as client:
        html = await client.fetch(url)

        now_time = datetime.now().strftime("%Y%m%d_%H%M%S")

        with open(f"sample_topcv_job_{now_time}.html", "w", encoding="utf-8") as f:
            f.write(html)

        parser = TopcvParser()

        a = parser.parse_job_detail(html, "brand", "test")

        if a is not None:
            job_dict = asdict(a)
            json_str = json.dumps(job_dict, ensure_ascii=False, indent=2)
            
            
            output_file = f"sample_topcv_job_{now_time}.json"
            with open(output_file, "w", encoding="utf-8") as f:
                f.write(json_str)
            logger.info("Đã lưu kết quả vào: %s", output_file)
        else:
            logger.info("Lỗi: Không thể parse được dữ liệu từ HTML.")

if __name__ == "__main__":
    asyncio.run(get_html())
