import os
from dotenv import load_dotenv
from src.storage_layer.MinIO_S3.layer.silver.utils.config_loader import load_config_yaml
from pathlib import Path
load_dotenv()

S3_CONFIG_PATH = Path(__file__).parents[1] / "MinIO_S3" / "config" / "bucket.yml"

# Load environment variables from .env file

MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")
SILVER_BUCKET = load_config_yaml(S3_CONFIG_PATH)["bucket_name"]["silver_layer"]