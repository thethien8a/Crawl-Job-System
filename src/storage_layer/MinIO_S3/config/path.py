from pathlib import Path
from src.storage_layer.MinIO_S3.layer.silver.utils.config_loader import load_config_yaml

## Local Path
CONFIG_PATH = Path(__file__).parent
YAML_PATH = CONFIG_PATH / "bucket.yml"
## Bucket Paths
class BronzeBucketPaths:
    def __init__(self, source_name: str, entity_name: str = "jobs", year: str = "*", month: str = "*", day: str = "*"):
        self.source_name = source_name
        self.entity_name = entity_name
        self.year = year
        self.month = month
        self.day = day
        self.config = load_config_yaml(YAML_PATH)
        self.bronze_bucket_name = self._get_bronze_bucket_name()
        
    def _get_bronze_bucket_name(self):
        return self.config["bucket_name"]["bronze_layer"]

    def get_prefix(self) -> str:
        """
        Generate dynamic S3 prefix for Bronze layer folder with date (without bucket name and schema).
        """
        return f"{self.source_name}/{self.entity_name}/year={self.year}/month={self.month}/day={self.day}/"

    def get_folder_date_path(self) -> str:
        """
        Generate dynamic S3 path for Bronze layer folder with date.
        """
        return f"s3://{self.bronze_bucket_name}/{self.source_name}/{self.entity_name}/year={self.year}/month={self.month}/day={self.day}/"

    def get_files_path_json_gz(self) -> str:
        """
        Generate dynamic S3 path for Bronze layer.
        Can be used with specific date or with wildcards (*) to match multiple directories.
        """
        return f"s3://{self.bronze_bucket_name}/{self.source_name}/{self.entity_name}/year={self.year}/month={self.month}/day={self.day}/*.jsonl.gz"


class SilverBucketPaths:
    def __init__(self, source_name: str, entity_name: str = "jobs", year: str = "*", month: str = "*", day: str = "*"):
        self.source_name = source_name
        self.entity_name = entity_name
        self.year = year
        self.month = month
        self.day = day
        self.config = load_config_yaml(YAML_PATH)
        self.silver_bucket_name = self._get_silver_bucket_name()

    def _get_silver_bucket_name(self):
        return self.config["bucket_name"]["silver_layer"]

    def get_prefix(self) -> str:
        """
        Generate dynamic S3 prefix for Silver layer folder with date (without bucket name and schema).
        """
        return f"{self.source_name}/{self.entity_name}/year={self.year}/month={self.month}/day={self.day}/"

    def get_folder_date_path(self) -> str:
        return f"s3://{self.silver_bucket_name}/{self.source_name}/{self.entity_name}/year={self.year}/month={self.month}/day={self.day}/"

    def get_files_path_parquet(self) -> str:
        """
        Silver dùng parquet (columnar, schema-aware) thay vì jsonl.gz để query nhanh hơn ở downstream.
        """
        return f"s3://{self.silver_bucket_name}/{self.source_name}/{self.entity_name}/year={self.year}/month={self.month}/day={self.day}/*.parquet"
