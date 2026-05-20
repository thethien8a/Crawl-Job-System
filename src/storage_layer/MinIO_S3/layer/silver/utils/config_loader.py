import yaml
import polars as pl
from pathlib import Path

def load_config_yaml(yaml_path):
    with open(yaml_path, "r", encoding="utf-8") as file:
        # Sử dụng safe_load để đảm bảo an toàn bảo mật
        config = yaml.safe_load(file)
    
    return config

SEEDS_DIR = Path(__file__).resolve().parent.parent / "seeds"
def read_seeds(file_name_csv: str):
    """Load the skill taxonomy from CSV."""
    path = SEEDS_DIR / file_name_csv
    return pl.read_csv(path)