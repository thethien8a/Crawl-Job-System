import polars as pl
import logging
from src.storage_layer.MinIO_S3.layer.silver.cleaning.common.clean_benefit import main_clean_benefit

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

benefits = pl.DataFrame([
    {
        "benefits": """
        Lương cạnh tranh theo năng lực.
        Thưởng hiệu suất học thuật.
        Cơ hội phát triển lên Academic Supervisor/Manager.
        Được training về phương pháp giảng dạy & phát triển học liệu chuyên sâu.
        """
    },
    {
        "benefits": """
        13th-month salary and performance bonus.

        • Bonus upon successful completion of the probation period.

        • Premium healthcare insurance (PVI package) and family medical benefits (based on experience level).

        • Access to a well-known e-learning platform (Udemy) to support continuous learning and T-shaped skill development.

        • Flexible working hours: only 8 continuous working hours required per day.

        • Annual leave up to 17 days: 12 days of paid leave and 5 days of sick leave.

        • Professional and personal development training programs.

        • 4-star company trip in summer and an annual year-end party.

        • Coffee and snacks provided.

        • Holiday celebrations and events for employees and their families.
        """
    },
    {
        "benefits": """
        Tiên phong công nghệ, uy tín

        MISA là doanh nghiệp CNTT xuất sắc nhất khu vực Châu Á - Châu Đại Dương. Tiên phong xuất khẩu giải pháp SaaS.
        TOP đầu doanh nghiệp CNTT tăng trưởng liên tục với quy mô nhân sự tăng 20%/năm, doanh thu tăng 15%/năm.
        Hội tụ hơn 4000 nhân tài cùng khát vọng đưa sản phẩm công nghệ "Make In Vietnam" vươn tầm quốc tế.
        Xây dựng niềm tin với 500.000 khách hàng là cơ quan nhà nước, doanh nghiệp, hộ kinh doanh và 3.5 triệu khách hàng cá nhân tại Việt Nam và 22 quốc gia trên thế giới.
        Hơn 100 giải thưởng trong ngành CNTT trong nước và quốc tế.
        Nền tảng vững chắc cho phát triển sự nghiệp, thăng tiến, quyền lợi

        Lương cứng cạnh tranh. Thưởng năng suất dựa trên kết quả công việc từ 2 tháng lương.
        Đánh giá review lương 2 lần/năm, thưởng sáng kiến...
        Huấn luyện "Hổ tướng": chương trình đào tạo quản lý tài năng, bệ phóng trở thành Chiến tướng tinh nhuệ
        Giải thưởng "Gấu vàng": nơi tôn vinh những tài năng xuất sắc nhất
        Gói chăm sóc sức khỏe toàn diện tại Medlatec, cháy hết mình tại các CLB theo sở thích, chương trình teambuilding, du lịch định kỳ
        Môi trường thân thiện, chia sẻ, đồng hành

        Kết nối tài năng: tập trung phát triển những con người có chung lý tưởng, mục tiêu, cùng trao giá trị và nhận thành công
        Tư duy đột phá: môi trường tôn trọng sự khác biệt và đề cao sáng tạo, MISA-er được tự do phát triển các ý tưởng tiến bộ, cải tiến công việc
        Công nghệ cao: trang bị máy tính làm việc, tối ưu hiệu suất công việc bằng ứng dụng công nghệ, phần mềm tự động (AMIS, Jira, Power BI, AI Marketing,...)
        Nơi làm việc hạnh phúc: MISA mong muốn tạo một môi trường làm việc để bạn luôn cảm thấy hạnh phúc
        """
    },
])

logger.info("=== Raw benefits ===")
logger.info(benefits)

df = main_clean_benefit(benefits)
logger.info("\n=== Cleaned benefits ===")
logger.info(df)
