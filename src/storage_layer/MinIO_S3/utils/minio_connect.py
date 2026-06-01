import boto3
from src.storage_layer.MinIO_S3.config.key import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_REGION,
)

# Module name kept as `minio_connect` to avoid touching ~30 import sites.
# This now returns a real AWS S3 client; no endpoint_url override.
def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )
