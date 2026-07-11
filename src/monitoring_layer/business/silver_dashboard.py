from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from datetime import date, timedelta
from html import escape
from pathlib import Path

import polars as pl
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError

from src.monitoring_layer.business.dashboard_common import (
    DEFAULT_ENTITY_NAME,
    DEFAULT_MAX_TABLE_ROWS,
    DEFAULT_SOURCE_SITES,
    DateRange,
    S3ObjectInfo,
    dashboard_css,
    discover_silver_sources,
    last_or_default,
    list_silver_objects,
    ordered_unique,
    parse_iso_date,
    parse_source_sites,
    render_empty_state,
    render_table_section,
)
from src.storage_layer.MinIO_S3.config.path import SilverBucketPaths
from src.storage_layer.MinIO_S3.layer.silver.utils.google_sheets import (
    GoogleSheetsError,
    google_sheets_config_is_available,
    read_worksheet_as_polars,
    worksheet_title_for_csv,
)
from src.storage_layer.MinIO_S3.layer.silver.utils.reader import get_jobs_silver_by_site
from src.storage_layer.MinIO_S3.utils.minio_connect import get_s3_client

logger = logging.getLogger(__name__)

DEFAULT_OUTPUT_PATH = Path("src/monitoring_layer/business/reports/silver_dashboard.html")
DEFAULT_REVIEW_WINDOW_DAYS = 7
BENEFIT_SAMPLE_SIZE = 50
REQUIREMENT_SAMPLE_SIZE = 50
CLUSTERS_REVIEW_FILE_NAME = "clusters_review.csv"


@dataclass(frozen=True)
class SilverDashboardConfig:
    entity_name: str
    output_path: Path
    source_sites: tuple[str, ...]
    from_date: str | None
    to_date: str | None
    max_table_rows: int


def parse_args() -> SilverDashboardConfig:
    parser = argparse.ArgumentParser(description="Build the Lakehouse-Lite Silver business dashboard.")
    parser.add_argument("--entity_name", default=DEFAULT_ENTITY_NAME)
    parser.add_argument("--from_date", type=parse_iso_date)
    parser.add_argument("--to_date", type=parse_iso_date)
    parser.add_argument("--sources", help="Comma-separated source sites. Defaults to S3 discovery.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--max_table_rows", type=int, default=DEFAULT_MAX_TABLE_ROWS)
    args = parser.parse_args()

    if args.from_date and args.to_date and args.from_date > args.to_date:
        raise ValueError("--from_date must be earlier than or equal to --to_date")

    return SilverDashboardConfig(
        entity_name=args.entity_name,
        output_path=args.output,
        source_sites=parse_source_sites(args.sources),
        from_date=args.from_date,
        to_date=args.to_date,
        max_table_rows=args.max_table_rows,
    )


def build_dashboard_html(config: SilverDashboardConfig) -> str:
    client = get_s3_client()
    bucket_name = SilverBucketPaths("_", config.entity_name).silver_bucket_name
    source_sites = resolve_sources(client, bucket_name, config)
    objects = list_silver_objects(client, bucket_name, config.entity_name, source_sites)
    date_range = resolve_date_range(config, objects)
    frame = load_silver_frame(source_sites, config.entity_name, date_range)
    return render_dashboard(frame, date_range, config.max_table_rows)


def resolve_sources(client: object, bucket_name: str, config: SilverDashboardConfig) -> tuple[str, ...]:
    if config.source_sites:
        return config.source_sites
    discovered = discover_silver_sources(client, bucket_name, config.entity_name)
    return ordered_unique((*DEFAULT_SOURCE_SITES, *discovered))


def resolve_date_range(config: SilverDashboardConfig, silver_objects: list[S3ObjectInfo]) -> DateRange:
    discovered_dates = sorted({item.collection_date for item in silver_objects})
    latest_date = last_or_default(discovered_dates, date.today())
    to_date = date.fromisoformat(config.to_date) if config.to_date else latest_date
    from_date = (
        date.fromisoformat(config.from_date)
        if config.from_date
        else to_date - timedelta(days=DEFAULT_REVIEW_WINDOW_DAYS - 1)
    )
    if from_date > to_date:
        raise ValueError("Resolved Silver dashboard date range is invalid")
    return DateRange(from_date=from_date, to_date=to_date)


def load_silver_frame(source_sites: tuple[str, ...], entity_name: str, date_range: DateRange) -> pl.DataFrame:
    from_date, to_date = date_range.as_strings()
    frames: list[pl.LazyFrame] = []
    for source_site in source_sites:
        lazy_frame = get_jobs_silver_by_site(source_site, entity_name, from_date, to_date)
        if lazy_frame is not None:
            frames.append(lazy_frame)
    if not frames:
        return pl.DataFrame()
    return pl.concat(frames, how="diagonal_relaxed").collect()


def render_dashboard(frame: pl.DataFrame, date_range: DateRange, max_table_rows: int) -> str:
    table_sections = build_table_sections(frame, max_table_rows)
    return build_html_document(table_sections, date_range, max_table_rows)


def build_table_sections(frame: pl.DataFrame, max_table_rows: int) -> list[str]:
    return [
        render_unmapped_industry_table(frame, max_table_rows),
        render_company_review_table(max_table_rows),
        render_benefit_review_table(frame),
        render_requirement_review_table(frame),
    ]



def render_unmapped_industry_table(frame: pl.DataFrame, max_table_rows: int) -> str:
    table = find_unmapped_industries(frame, max_table_rows)
    note = 'Cập nhật thêm vào Google Sheets worksheet "industry_taxonomy" nếu các ngành này cần được map.'
    return render_table_section("Job industry chưa map", table, "Không có job_industry_unmapped khả nghi.", note)


def find_unmapped_industries(frame: pl.DataFrame, max_table_rows: int) -> pl.DataFrame:
    column = "job_industry_unmapped"
    if frame.is_empty() or column not in frame.columns:
        return pl.DataFrame()
    columns = ["source_site", "job_url", column]
    return frame.filter(non_empty_value_expr(frame, column)).pipe(select_existing_columns, columns).head(max_table_rows)


def non_empty_value_expr(frame: pl.DataFrame, column: str) -> pl.Expr:
    dtype = frame.schema[column]
    if str(dtype).startswith("List"):
        return pl.col(column).list.len() > 0
    cleaned = pl.col(column).cast(pl.String).str.strip_chars()
    return cleaned.is_not_null() & cleaned.is_not_in(["", "[]", "null"])


def render_company_review_table(max_table_rows: int) -> str:
    table = read_company_review_table(max_table_rows)
    note = 'Cập nhật thêm vào Google Sheets worksheet "company_mapping" nếu các công ty trong cùng cluster là cùng một doanh nghiệp.'
    return render_table_section("Review các công ty giống nhau", table, "Chưa có file clusters_review.csv hoặc file đang rỗng.", note)


def read_company_review_table(max_table_rows: int) -> pl.DataFrame:
    sheet_table = read_company_review_table_from_google_sheets(max_table_rows)
    if not sheet_table.is_empty():
        return sheet_table

    path = Path("src/storage_layer/MinIO_S3/layer/silver/utils/clusters_review.csv")
    if not path.exists():
        return pl.DataFrame()
    return pl.read_csv(path, encoding="utf8-lossy").head(max_table_rows)


def read_company_review_table_from_google_sheets(max_table_rows: int) -> pl.DataFrame:
    if not google_sheets_config_is_available():
        return pl.DataFrame()
    try:
        return read_worksheet_as_polars(worksheet_title_for_csv(CLUSTERS_REVIEW_FILE_NAME)).head(max_table_rows)
    except GoogleSheetsError as exc:
        logger.warning("Failed to read clusters_review from Google Sheets. Falling back to local CSV: %s", exc)
        return pl.DataFrame()


def render_benefit_review_table(frame: pl.DataFrame) -> str:
    columns = ["source_site", "job_url", "benefits_text_clean", "benefits_categories_vi"]
    table = recent_rows(frame, columns, BENEFIT_SAMPLE_SIZE)
    note = 'Cập nhật thêm vào Google Sheets worksheet "benefit_taxonomy" nếu cần bổ sung taxonomy phúc lợi.'
    return render_table_section("Review benefits gần đây", table, "Không có dữ liệu benefits để review.", note)


def render_requirement_review_table(frame: pl.DataFrame) -> str:
    columns = [
        "source_site",
        "job_url",
        "requirements",
        "requirements_cleaned",
        "require_programming_languages",
        "require_frameworks",
        "require_tools",
        "require_cloud_skills",
        "require_knowledge",
        "require_domain_knowledge",
        "require_foreign_languages",
        "require_domain_university",
    ]
    table = recent_rows(frame, columns, REQUIREMENT_SAMPLE_SIZE)
    note = (
        "Cập nhật các taxonomy skill trong Google Sheets "
        "(program_lang_taxonomy, framework_taxonomy, tools_taxonomy, "
        "cloud_skill_taxonomy, knowledge_taxonomy, domain_taxonomy, "
        "language_taxonomy, domain_university_taxonomy) "
        "nếu các requirement chưa được phân loại đúng."
    )
    return render_table_section("Review requirements gần đây", table, "Không có dữ liệu requirements để review.", note)


def recent_rows(frame: pl.DataFrame, columns: list[str], limit: int) -> pl.DataFrame:
    if frame.is_empty():
        return pl.DataFrame()
    recent = sort_recent_first(frame)
    if "source_site" in recent.columns:
        recent = recent.group_by("source_site", maintain_order=True).head(limit)
    else:
        recent = recent.head(limit)
    return select_existing_columns(recent, columns)


def sort_recent_first(frame: pl.DataFrame) -> pl.DataFrame:
    sort_columns = [column for column in ("year", "month", "day") if column in frame.columns]
    if not sort_columns:
        return frame
    return frame.sort(sort_columns, descending=[True] * len(sort_columns))


def select_existing_columns(frame: pl.DataFrame, columns: list[str]) -> pl.DataFrame:
    existing_columns = [column for column in columns if column in frame.columns]
    if not existing_columns:
        return pl.DataFrame()
    return frame.select(existing_columns)


def build_html_document(table_sections: list[str], date_range: DateRange, max_table_rows: int) -> str:
    return f"""<!doctype html>
<html lang="vi">
<head>
  <meta charset="utf-8">
  <title>Lakehouse-Lite Silver Dashboard</title>
  <style>{dashboard_css()}</style>
</head>
<body>
  <main>
    <header class="page-header">
      <p class="eyebrow">Lakehouse-Lite Monitoring</p>
      <h1>Dashboard tầng Silver</h1>
      <p>Review range: {escape(date_range.from_date.isoformat())} đến {escape(date_range.to_date.isoformat())}. Bảng dài hiển thị tối đa {max_table_rows:,} dòng.</p>
    </header>
    <p class="section-note" style="margin-bottom: 24px;">Các bảng bên dưới tập trung vào dữ liệu khả nghi để cập nhật cleaning logic hoặc seed taxonomy.</p>
    {''.join(table_sections) if table_sections else render_empty_state("Không có dữ liệu Silver để review.")}
  </main>
</body>
</html>
"""


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    config = parse_args()
    try:
        html = build_dashboard_html(config)
    except (BotoCoreError, ClientError, NoCredentialsError) as exc:
        raise RuntimeError("Failed to read S3 objects for the Silver dashboard. Check AWS credentials and bucket access.") from exc
    config.output_path.parent.mkdir(parents=True, exist_ok=True)
    config.output_path.write_text(html, encoding="utf-8")
    logger.info("Saved Silver dashboard to %s", config.output_path)


if __name__ == "__main__":
    main()
