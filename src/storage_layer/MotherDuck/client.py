import logging
import duckdb
from src.storage_layer.MotherDuck.config import MOTHERDUCK_TOKEN
from src.storage_layer.MinIO_S3.config.key import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION

logger = logging.getLogger(__name__)

class MotherDuckClient:
    def __init__(self, token: str = MOTHERDUCK_TOKEN):
        if not token:
            raise ValueError("MOTHERDUCK_TOKEN is missing in environment variables. Please check your .env file.")
        
        # Kết nối tới không gian MotherDuck trên Cloud
        self.con = duckdb.connect(f'md:?motherduck_token={token}')
        
    def setup_s3_credentials(self):
        """
        Tạo Persistent Secret trên MotherDuck để có quyền truy cập S3.
        Chỉ cần chạy 1 lần, MotherDuck sẽ lưu an toàn thông tin này trên Cloud.
        """
        logger.info("Configuring AWS S3 credentials in MotherDuck...")
        query = f"""
        CREATE OR REPLACE SECRET aws_s3_secret IN MOTHERDUCK (
            TYPE S3,
            KEY_ID '{AWS_ACCESS_KEY_ID}',
            SECRET '{AWS_SECRET_ACCESS_KEY}',
            REGION '{AWS_REGION}'
        );
        """
        self.con.sql(query)
        logger.info("Successfully configured S3 credentials.")

    def execute_query(self, query: str):
        """
        Thực thi truy vấn SQL (SELECT) và trả về kết quả dạng DataFrame
        """
        return self.con.sql(query).df()
        
    def execute_statement(self, statement: str):
        """
        Thực thi các câu lệnh SQL không trả về dữ liệu (DDL: CREATE VIEW, DROP, CREATE TABLE...).
        """
        self.con.sql(statement)