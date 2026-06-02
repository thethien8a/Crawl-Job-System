import logging
from src.storage_layer.MotherDuck.client import MotherDuckClient
from src.storage_layer.MotherDuck.config import SILVER_BUCKET

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    client = MotherDuckClient()
    
    # 1. Cấp quyền S3 cho MotherDuck (tạo/ghi đè Secret)
    client.setup_s3_credentials()

    logger.info("Creating views in MotherDuck...")
    
    try:
        schemas = client.execute_query("""
            SELECT schema_name 
            FROM duckdb_schemas() 
            WHERE database_name = 'sample_data';
        """)
        print(schemas)
    except Exception as e:
        logger.error("Failed to execute query: %s", e)
        raise
   
    
    