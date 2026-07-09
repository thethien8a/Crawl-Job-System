import os
from dataclasses import fields

from dotenv import load_dotenv

from src.storage_layer.MinIO_S3.config.path import DEFAULT_ENTITY_NAME, get_silver_bucket_name
from src.storage_layer.MotherDuck.schema.data_class import GoldJobItem

load_dotenv()

MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")
SILVER_BUCKET = get_silver_bucket_name()

# MotherDuck target for the Gold layer. Override the database via .env if needed.
MOTHERDUCK_DATABASE = os.getenv("MOTHERDUCK_DATABASE", "lakehouse-lite")
GOLD_SCHEMA = "gold"
GOLD_JOBS_TABLE = DEFAULT_ENTITY_NAME

# Silver entity + S3 glob. hive_partitioning exposes source_site/year/month/day,
# so a single wildcard scan covers every site and date in one read.
SILVER_ENTITY_NAME = DEFAULT_ENTITY_NAME
SILVER_PARQUET_GLOB = (
    f"s3://{SILVER_BUCKET}/{SILVER_ENTITY_NAME}/"
    "source_site=*/year=*/month=*/day=*/*.parquet"
)

# Child tables that hold unnested List[str] values for BI-friendly querying.
# Each child table joins back to gold.jobs through the compact integer job_id.
GOLD_INDUSTRIES_TABLE = "job_industries"       # unnested from job_industry_clean
GOLD_BENEFITS_TABLE = "job_benefits"           # unnested from benefits_categories_vi
GOLD_REQUIREMENTS_TABLE = "job_requirements"   # unnested from all require_* columns

# Star-schema dimensions for Power BI. The fact (gold.jobs) keeps the join keys
# inline: source_site (natural key) and date_key (yyyymmdd surrogate).
GOLD_DIM_DATE_TABLE = "dim_date"                # one row per calendar date (contiguous)


# Mapping from a GoldJobItem List[str] field name → (child_table, discriminator_label).
# discriminator_label is the string stored in the 'requirement_type' column
# of gold.job_requirements; None means the child table has no discriminator column.
LIST_FIELD_TO_CHILD: dict[str, tuple[str, str | None]] = {
    "job_title_special_keywords":    (GOLD_REQUIREMENTS_TABLE, "special_keyword"),
    "job_industry_clean":            (GOLD_INDUSTRIES_TABLE,    None),
    "benefits_categories_vi":        (GOLD_BENEFITS_TABLE,      None),
    "require_programming_languages": (GOLD_REQUIREMENTS_TABLE, "programming_language"),
    "require_frameworks":            (GOLD_REQUIREMENTS_TABLE, "framework"),
    "require_tools":                 (GOLD_REQUIREMENTS_TABLE, "tool"),
    "require_cloud_skills":          (GOLD_REQUIREMENTS_TABLE, "cloud_skill"),
    "require_knowledge":             (GOLD_REQUIREMENTS_TABLE, "knowledge"),
    "require_domain_knowledge":      (GOLD_REQUIREMENTS_TABLE, "domain_knowledge"),
    "require_foreign_languages":     (GOLD_REQUIREMENTS_TABLE, "foreign_language"),
    "require_domain_university":     (GOLD_REQUIREMENTS_TABLE, "domain_university"),
}

# Hive-partition date columns carried through staging; collapsed into the
# date_key surrogate on the fact and expanded into gold.dim_date.
GOLD_DATE_COLUMNS = ["year", "month", "day"]

# All scalar GoldJobItem fields (List[str] ones are unnested into child tables).
# These are the fact columns of gold.jobs -- including job_url and the
# source_site foreign key; the date_key foreign key is derived in the builder.
_ALL_FIELDS = [f.name for f in fields(GoldJobItem)]
GOLD_JOBS_COLUMNS = [
    name for name in _ALL_FIELDS
    if name not in LIST_FIELD_TO_CHILD
]
