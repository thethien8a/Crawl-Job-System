from __future__ import annotations

import argparse
import gzip
import logging
from dataclasses import dataclass
from html import escape
from pathlib import Path
from typing import Any

import altair as alt
import polars as pl
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError

from src.monitoring_layer.business.dashboard_common import (
    BYTES_PER_MB,
    DEFAULT_ENTITY_NAME,
    DEFAULT_SOURCE_SITES,
    S3ObjectInfo,
    build_object_frame,
    chart_to_json,
    dashboard_css,
    discover_bronze_sources,
    list_bronze_objects,
    ordered_unique,
    parse_source_sites,
    render_chart_container,
    render_chart_script,
    render_empty_state,
)
from src.storage_layer.MinIO_S3.config.path import BronzeBucketPaths
from src.storage_layer.MinIO_S3.utils.minio_connect import get_s3_client

logger = logging.getLogger(__name__)

DEFAULT_OUTPUT_PATH = Path("src/monitoring_layer/business/bronze_dashboard.html")


@dataclass(frozen=True)
class BronzeDashboardConfig:
    entity_name: str
    output_path: Path
    source_sites: tuple[str, ...]


@dataclass(frozen=True)
class BronzeSummary:
    total_storage_mb: float
    source_count: int
    object_count: int


def parse_args() -> BronzeDashboardConfig:
    parser = argparse.ArgumentParser(description="Build the Lakehouse-Lite Bronze business dashboard.")
    parser.add_argument("--entity_name", default=DEFAULT_ENTITY_NAME)
    parser.add_argument("--sources", help="Comma-separated source sites. Defaults to S3 discovery.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    args = parser.parse_args()
    return BronzeDashboardConfig(
        entity_name=args.entity_name,
        output_path=args.output,
        source_sites=parse_source_sites(args.sources),
    )


def build_dashboard_html(config: BronzeDashboardConfig) -> str:
    client = get_s3_client()
    bucket_name = BronzeBucketPaths("_", config.entity_name).bronze_bucket_name
    source_sites = resolve_sources(client, bucket_name, config)
    objects = list_bronze_objects(client, bucket_name, config.entity_name, source_sites)
    return render_dashboard(build_bronze_frame(client, bucket_name, objects))


def resolve_sources(client: object, bucket_name: str, config: BronzeDashboardConfig) -> tuple[str, ...]:
    if config.source_sites:
        return config.source_sites
    discovered = discover_bronze_sources(client, bucket_name)
    return ordered_unique((*DEFAULT_SOURCE_SITES, *discovered))


def build_bronze_frame(client: Any, bucket_name: str, objects: list[S3ObjectInfo]) -> pl.DataFrame:
    frame = build_object_frame(objects)
    if frame.is_empty():
        return frame.with_columns(pl.lit(None, dtype=pl.Int64).alias("record_count"))
    record_counts = [count_jsonl_gzip_records(client, bucket_name, item.key) for item in objects]
    return frame.with_columns(pl.Series("record_count", record_counts, dtype=pl.Int64))


def count_jsonl_gzip_records(client: Any, bucket_name: str, key: str) -> int:
    response = client.get_object(Bucket=bucket_name, Key=key)
    with response["Body"] as body:
        with gzip.GzipFile(fileobj=body) as compressed_file:
            return sum(1 for line in compressed_file if line.strip())


def render_dashboard(frame: pl.DataFrame) -> str:
    summary = build_summary(frame)
    chart_blocks = build_chart_blocks(frame)
    return build_html_document(summary, chart_blocks)


def build_summary(frame: pl.DataFrame) -> BronzeSummary:
    if frame.is_empty():
        return BronzeSummary(0, 0, 0)
    total_storage_mb = frame.get_column("size_bytes").sum() / BYTES_PER_MB
    source_count = frame.get_column("source_site").n_unique()
    return BronzeSummary(total_storage_mb, source_count, frame.height)


def build_chart_blocks(frame: pl.DataFrame) -> list[tuple[str, str, str]]:
    if frame.is_empty():
        return []
    return [
        ("bronze-storage-by-site", "Tổng dung lượng Bronze theo site", chart_to_json(build_storage_by_site_chart(frame))),
        ("bronze-daily-storage", "Dung lượng Bronze theo ngày và source site", chart_to_json(build_daily_storage_chart(frame))),
        ("bronze-daily-records", "Số lượng dữ liệu thu thập theo ngày và source site", chart_to_json(build_daily_record_count_chart(frame))),
    ]


def build_storage_by_site_chart(frame: pl.DataFrame) -> alt.Chart:
    records = (
        frame.group_by("source_site")
        .agg((pl.col("size_bytes").sum() / BYTES_PER_MB).round(2).alias("storage_mb"))
        .sort("storage_mb", descending=True)
        .to_dicts()
    )
    return (
        alt.Chart({"values": records})
        .mark_bar(color="#4318FF", cornerRadiusEnd=8)
        .encode(
            x=alt.X("source_site:N", title="Source site", sort="-y", axis=alt.Axis(labelAngle=0)),
            y=alt.Y("storage_mb:Q", title="Dung lượng (MB)"),
            tooltip=["source_site:N", alt.Tooltip("storage_mb:Q", format=",.2f")],
        )
        .properties(height=320)
    )


def build_daily_storage_chart(frame: pl.DataFrame) -> alt.Chart:
    records = (
        frame.group_by("collection_date", "source_site")
        .agg(
            (pl.col("size_bytes").sum() / BYTES_PER_MB).round(2).alias("storage_mb"),
            pl.len().alias("object_count"),
        )
        .sort("collection_date")
        .to_dicts()
    )
    return (
        alt.Chart({"values": records})
        .mark_line(point=alt.OverlayMarkDef(filled=True, size=65), strokeWidth=3, interpolate="monotone")
        .encode(
            x=alt.X("collection_date:T", title="Ngày thu thập", axis=alt.Axis(format="%d/%m", labelAngle=0)),
            y=alt.Y("storage_mb:Q", title="Dung lượng (MB)"),
            color=alt.Color("source_site:N", title="Source site"),
            tooltip=[
                "source_site:N",
                "collection_date:T",
                alt.Tooltip("storage_mb:Q", format=",.2f"),
                "object_count:Q",
            ],
        )
        .properties(height=360)
    )


def build_daily_record_count_chart(frame: pl.DataFrame) -> alt.Chart:
    records = (
        frame.group_by("collection_date", "source_site")
        .agg(pl.col("record_count").sum().alias("record_count"))
        .sort("collection_date")
        .to_dicts()
    )
    return (
        alt.Chart({"values": records})
        .mark_line(point=alt.OverlayMarkDef(filled=True, size=65), strokeWidth=3, interpolate="monotone")
        .encode(
            x=alt.X("collection_date:T", title="Ngày thu thập", axis=alt.Axis(format="%d/%m", labelAngle=0)),
            y=alt.Y("record_count:Q", title="Số lượng dữ liệu"),
            color=alt.Color("source_site:N", title="Source site"),
            tooltip=[
                "source_site:N",
                "collection_date:T",
                alt.Tooltip("record_count:Q", title="Số lượng dữ liệu", format=",d"),
            ],
        )
        .properties(height=360)
    )


def build_html_document(summary: BronzeSummary, chart_blocks: list[tuple[str, str, str]]) -> str:
    charts_html = "".join(render_chart_container(chart_id, title) for chart_id, title, _ in chart_blocks)
    chart_scripts = "\n".join(render_chart_script(chart_id, spec_json) for chart_id, _, spec_json in chart_blocks)
    empty_chart = "" if chart_blocks else render_empty_state("Không có object Bronze để vẽ biểu đồ.")
    return f"""<!doctype html>
<html lang="vi">
<head>
  <meta charset="utf-8">
  <title>Lakehouse-Lite Bronze Dashboard</title>
  <script src="https://cdn.jsdelivr.net/npm/vega@5"></script>
  <script src="https://cdn.jsdelivr.net/npm/vega-lite@5"></script>
  <script src="https://cdn.jsdelivr.net/npm/vega-embed@6"></script>
  <style>{dashboard_css()}</style>
</head>
<body>
  <main>
    <header class="page-header">
      <p class="eyebrow">Lakehouse-Lite Monitoring</p>
      <h1>Dashboard tầng Bronze</h1>
      <p>Theo dõi dung lượng lưu trữ hiện tại và lịch sử dữ liệu đã crawl ở Bronze.</p>
    </header>
    {render_metric_cards(summary)}
    {charts_html}
    {empty_chart}
  </main>
  <script>{chart_scripts}</script>
</body>
</html>
"""


def render_metric_cards(summary: BronzeSummary) -> str:
    cards = [
        ("Tổng dung lượng Bronze", f"{summary.total_storage_mb:,.2f} MB"),
        ("Tổng số trang web thu thập", f"{summary.source_count:,}"),
        ("Tổng số object Bronze", f"{summary.object_count:,}"),
    ]
    return '<div class="metrics">' + "".join(render_metric_card(label, value) for label, value in cards) + "</div>"


def render_metric_card(label: str, value: str) -> str:
    return f'<article class="metric"><span>{escape(label)}</span><strong>{escape(value)}</strong></article>'


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    config = parse_args()
    try:
        html = build_dashboard_html(config)
    except (BotoCoreError, ClientError, NoCredentialsError) as exc:
        raise RuntimeError("Failed to read S3 objects for the Bronze dashboard. Check AWS credentials and bucket access.") from exc
    config.output_path.parent.mkdir(parents=True, exist_ok=True)
    config.output_path.write_text(html, encoding="utf-8")
    logger.info("Saved Bronze dashboard to %s", config.output_path)


if __name__ == "__main__":
    main()
