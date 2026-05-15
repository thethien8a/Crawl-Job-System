from src.storage_layer.MinIO_S3.config.path import YAML_PATH
from src.storage_layer.MinIO_S3.layer.silver.utils.config_loader import load_config_yaml
from src.crawl_layer.utils.loader import load_to_bronze

if __name__ == "__main__":
    config = load_config_yaml(YAML_PATH)
    bronze_bucket_name = config["bucket_name"]["bronze_layer"]
    load_to_bronze(bronze_bucket_name)