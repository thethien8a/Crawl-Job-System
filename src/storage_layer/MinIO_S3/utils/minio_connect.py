import boto3
from src.storage_layer.MinIO_S3.config.key import MINIO_ACCESS_KEY,MINIO_ENDPOINT,MINIO_SECRET_KEY

def get_s3_client():
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name='ap-southeast-1'
    )