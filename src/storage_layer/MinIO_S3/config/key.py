from dotenv import load_dotenv
import os

load_dotenv()

# AWS S3 credentials read from .env at import time.
# AWS_REGION must match the region where the bronze/silver buckets live.
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "ap-southeast-1")
