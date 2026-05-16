from src.storage_layer.MinIO_S3.layer.silver.utils.reader import get_jobs_data_from_bronze

df = get_jobs_data_from_bronze("itviec", "jobs", "2026-05-14", "2026-05-14")

