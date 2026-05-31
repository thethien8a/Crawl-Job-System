from dotenv import load_dotenv
import os

load_dotenv()

if os.getenv("IS_DOCKER"):
    MINIO_ENDPOINT = "http://minio:9000"
else:
    MINIO_ENDPOINT = "http://localhost:9000"
    
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio123")