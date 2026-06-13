from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date
from html import escape
from pathlib import Path
from typing import Any, Iterable

import polars as pl

DEFAULT_ENTITY_NAME = "jobs"
DEFAULT_SOURCE_SITES = ("topcv", "vietnamworks", "itviec")
DEFAULT_MAX_TABLE_ROWS = 500
BYTES_PER_MB = 1024 * 1024

CHART_FONT = "'Inter', system-ui, -apple-system, sans-serif"
CHART_CATEGORY_COLORS = ["#4318FF", "#39B8FF", "#01B574", "#FFB547", "#EE5D50", "#868CFF"]

BRONZE_KEY_PATTERN = re.compile(
    r"^(?P<source_site>[^/]+)/(?P<entity_name>[^/]+)/"
    r"year=(?P<year>\d{4})/month=(?P<month>\d{1,2})/day=(?P<day>\d{1,2})/"
)
SILVER_KEY_PATTERN = re.compile(
    r"^(?P<entity_name>[^/]+)/source_site=(?P<source_site>[^/]+)/"
    r"year=(?P<year>\d{4})/month=(?P<month>\d{1,2})/day=(?P<day>\d{1,2})/"
)


@dataclass(frozen=True)
class S3ObjectInfo:
    source_site: str
    collection_date: date
    size_bytes: int
    key: str


@dataclass(frozen=True)
class DateRange:
    from_date: date
    to_date: date

    def as_strings(self) -> tuple[str, str]:
        return self.from_date.isoformat(), self.to_date.isoformat()


def parse_iso_date(value: str) -> str:
    date.fromisoformat(value)
    return value


def parse_source_sites(raw_sources: str | None) -> tuple[str, ...]:
    if not raw_sources:
        return ()
    return tuple(source.strip() for source in raw_sources.split(",") if source.strip())


def ordered_unique(values: Iterable[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return tuple(result)


def list_common_prefixes(client: Any, bucket_name: str, prefix: str) -> tuple[str, ...]:
    paginator = client.get_paginator("list_objects_v2")
    prefixes: list[str] = []
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix, Delimiter="/"):
        prefixes.extend(item["Prefix"] for item in page.get("CommonPrefixes", []))
    return tuple(prefixes)


def discover_bronze_sources(client: Any, bucket_name: str) -> tuple[str, ...]:
    prefixes = list_common_prefixes(client, bucket_name, prefix="")
    return tuple(prefix.strip("/") for prefix in prefixes if prefix.strip("/"))


def discover_silver_sources(client: Any, bucket_name: str, entity_name: str) -> tuple[str, ...]:
    prefixes = list_common_prefixes(client, bucket_name, prefix=f"{entity_name}/")
    sites = []
    for prefix in prefixes:
        match = re.search(r"source_site=([^/]+)/$", prefix)
        if match:
            sites.append(match.group(1))
    return tuple(sites)


def list_bronze_objects(
    client: Any,
    bucket_name: str,
    entity_name: str,
    source_sites: tuple[str, ...],
) -> list[S3ObjectInfo]:
    objects: list[S3ObjectInfo] = []
    for source_site in source_sites:
        prefix = f"{source_site}/{entity_name}/"
        objects.extend(parse_objects(client, bucket_name, prefix, BRONZE_KEY_PATTERN, entity_name, ".jsonl.gz"))
    return objects


def list_silver_objects(
    client: Any,
    bucket_name: str,
    entity_name: str,
    source_sites: tuple[str, ...],
) -> list[S3ObjectInfo]:
    objects: list[S3ObjectInfo] = []
    for source_site in source_sites:
        prefix = f"{entity_name}/source_site={source_site}/"
        objects.extend(parse_objects(client, bucket_name, prefix, SILVER_KEY_PATTERN, entity_name, ".parquet"))
    return objects


def parse_objects(
    client: Any,
    bucket_name: str,
    prefix: str,
    key_pattern: re.Pattern[str],
    entity_name: str,
    file_suffix: str,
) -> list[S3ObjectInfo]:
    parsed_objects: list[S3ObjectInfo] = []
    for item in iter_s3_objects(client, bucket_name, prefix):
        parsed = parse_object_info(item, key_pattern, entity_name, file_suffix)
        if parsed:
            parsed_objects.append(parsed)
    return parsed_objects


def iter_s3_objects(client: Any, bucket_name: str, prefix: str) -> Iterable[dict[str, Any]]:
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        yield from page.get("Contents", [])


def parse_object_info(
    item: dict[str, Any],
    key_pattern: re.Pattern[str],
    entity_name: str,
    file_suffix: str,
) -> S3ObjectInfo | None:
    key = item.get("Key", "")
    if not key.endswith(file_suffix):
        return None
    match = key_pattern.match(key)
    if not match or match.group("entity_name") != entity_name:
        return None
    return S3ObjectInfo(
        source_site=match.group("source_site"),
        collection_date=date(
            int(match.group("year")),
            int(match.group("month")),
            int(match.group("day")),
        ),
        size_bytes=int(item.get("Size", 0)),
        key=key,
    )


def first_or_default(values: list[date], default: date) -> date:
    return values[0] if values else default


def last_or_default(values: list[date], default: date) -> date:
    return values[-1] if values else default


def build_object_frame(objects: list[S3ObjectInfo]) -> pl.DataFrame:
    if not objects:
        return pl.DataFrame(schema=object_schema())
    return pl.DataFrame(
        {
            "source_site": [item.source_site for item in objects],
            "collection_date": [item.collection_date.isoformat() for item in objects],
            "size_bytes": [item.size_bytes for item in objects],
            "key": [item.key for item in objects],
        },
        schema=object_schema(),
    )


def object_schema() -> dict[str, pl.DataType]:
    return {
        "source_site": pl.String,
        "collection_date": pl.String,
        "size_bytes": pl.Int64,
        "key": pl.String,
    }


def style_chart(chart: Any) -> Any:
    return (
        chart.properties(width="container", autosize={"type": "fit-x", "contains": "padding"})
        .configure_view(stroke=None)
        .configure_axis(
            labelFont=CHART_FONT,
            titleFont=CHART_FONT,
            labelColor="#64748b",
            titleColor="#1e293b",
            labelFontSize=13,
            titleFontSize=14,
            titleFontWeight=600,
            titlePadding=16,
            labelPadding=8,
            gridColor="#e2e8f0",
            domainColor="#e2e8f0",
            tickColor="#e2e8f0",
        )
        .configure_legend(
            labelFont=CHART_FONT,
            titleFont=CHART_FONT,
            labelColor="#64748b",
            titleColor="#1e293b",
            labelFontSize=13,
            titleFontSize=14,
            symbolType="circle",
            padding=16,
        )
        .configure_range(category=CHART_CATEGORY_COLORS)
    )


def chart_to_json(chart: Any) -> str:
    return json.dumps(style_chart(chart).to_dict(), ensure_ascii=False)


def render_chart_container(chart_id: str, title: str) -> str:
    return f'<section class="chart-block"><h3>{escape(title)}</h3><div id="{escape(chart_id)}"></div></section>'


def render_chart_script(chart_id: str, spec_json: str) -> str:
    return f'vegaEmbed("#{chart_id}", {spec_json}, {{actions: false}});'


def render_table_section(title: str, frame: pl.DataFrame, empty_message: str, note: str) -> str:
    table_html = render_empty_state(empty_message) if frame.is_empty() else frame_to_html_table(frame)
    return f"""
      <section class="table-section">
        <h3>{escape(title)}</h3>
        {table_html}
        <p class="table-note">{escape(note)}</p>
      </section>
    """


def frame_to_html_table(frame: pl.DataFrame) -> str:
    headers = "".join(f"<th>{escape(column)}</th>" for column in frame.columns)
    rows = "".join(render_table_row(row, frame.columns) for row in frame.to_dicts())
    return f'<div class="table-scroll"><table><thead><tr>{headers}</tr></thead><tbody>{rows}</tbody></table></div>'


def render_table_row(row: dict[str, Any], columns: list[str]) -> str:
    cells = "".join(f"<td>{escape(format_cell(row.get(column)))}</td>" for column in columns)
    return f"<tr>{cells}</tr>"


def format_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        return ", ".join(str(item) for item in value)
    return str(value)


def render_empty_state(message: str) -> str:
    return f'<div class="empty-state">{escape(message)}</div>'


def dashboard_css() -> str:
    return """
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

    :root {
      color-scheme: light;
      --bg: #f8fafc;
      --card: #ffffff;
      --line: #e2e8f0;
      --text: #0f172a;
      --muted: #475569;
      --accent: #2563eb;
      --accent-light: #eff6ff;
      --shadow-sm: 0 1px 2px 0 rgb(0 0 0 / 0.05);
      --shadow-md: 0 4px 6px -1px rgb(0 0 0 / 0.1), 0 2px 4px -2px rgb(0 0 0 / 0.1);
      --shadow-lg: 0 10px 15px -3px rgb(0 0 0 / 0.1), 0 4px 6px -4px rgb(0 0 0 / 0.1);
      --shadow-card: 0 4px 20px -2px rgba(15, 23, 42, 0.05);
      --radius-md: 12px;
      --radius-lg: 16px;
      --radius-xl: 20px;
    }

    body {
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font-family: 'Inter', system-ui, -apple-system, sans-serif;
      -webkit-font-smoothing: antialiased;
    }

    main {
      width: min(1400px, calc(100% - 48px));
      margin: 40px auto;
      animation: fadeIn 0.4s ease-out;
    }

    @keyframes fadeIn {
      from { opacity: 0; transform: translateY(10px); }
      to { opacity: 1; transform: translateY(0); }
    }

    h1, h2, h3 {
      margin: 0 0 16px;
      font-weight: 700;
      letter-spacing: -0.02em;
    }

    h1 { font-size: 32px; line-height: 1.2; }
    h3 { font-size: 18px; color: var(--text); }

    .page-header {
      margin-bottom: 32px;
      padding: 32px 40px;
      background: linear-gradient(135deg, var(--accent) 0%, #3b82f6 100%);
      border-radius: var(--radius-xl);
      color: white;
      box-shadow: var(--shadow-md);
      position: relative;
      overflow: hidden;
    }

    .page-header h1 {
      color: white;
      margin-bottom: 8px;
      position: relative;
      z-index: 1;
    }

    .page-header p {
      color: rgba(255, 255, 255, 0.9);
      font-size: 15px;
      margin: 0;
      max-width: 600px;
      line-height: 1.5;
      position: relative;
      z-index: 1;
    }

    .eyebrow {
      color: rgba(255, 255, 255, 0.8);
      font-weight: 700;
      letter-spacing: 0.1em;
      text-transform: uppercase;
      font-size: 12px;
      margin-bottom: 12px;
      position: relative;
      z-index: 1;
    }

    .card, .table-section, .chart-block {
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: var(--radius-lg);
      padding: 24px;
      box-shadow: var(--shadow-card);
    }

    .card { margin-bottom: 32px; padding: 28px; }

    .metrics {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      gap: 20px;
      margin-bottom: 32px;
    }

    .metric {
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: var(--radius-lg);
      padding: 20px 24px;
      display: flex;
      flex-direction: column;
      justify-content: center;
      box-shadow: var(--shadow-sm);
      transition: border-color 0.2s ease, box-shadow 0.2s ease;
    }

    .metric:hover {
      box-shadow: var(--shadow-md);
      border-color: #cbd5e1;
    }

    .metric span {
      display: block;
      color: var(--muted);
      font-size: 13px;
      font-weight: 600;
      margin-bottom: 8px;
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }

    .metric strong {
      font-size: 32px;
      font-weight: 700;
      color: var(--text);
      line-height: 1.1;
    }

    .chart-block, .table-section {
      margin-top: 24px;
      background: var(--card);
    }

    .chart-block > div[id], .chart-block .vega-embed { width: 100%; }
    .vega-embed summary { display: none !important; }

    .section-note, .table-note {
      color: var(--muted);
      font-size: 14px;
      line-height: 1.5;
      margin-top: 16px;
      padding: 16px;
      background: var(--accent-light);
      border-radius: var(--radius-md);
      border-left: 4px solid var(--accent);
    }

    .table-scroll {
      max-height: 500px;
      overflow: auto;
      border: 1px solid var(--line);
      border-radius: var(--radius-md);
    }

    .table-scroll::-webkit-scrollbar {
      width: 8px;
      height: 8px;
    }
    .table-scroll::-webkit-scrollbar-track {
      background: var(--bg);
      border-radius: 4px;
    }
    .table-scroll::-webkit-scrollbar-thumb {
      background: #cbd5e1;
      border-radius: 4px;
    }
    .table-scroll::-webkit-scrollbar-thumb:hover {
      background: #94a3b8;
    }

    table {
      width: 100%;
      border-collapse: separate;
      border-spacing: 0;
      font-size: 14px;
    }

    th, td {
      padding: 12px 16px;
      vertical-align: middle;
      text-align: left;
      border-bottom: 1px solid var(--line);
      line-height: 1.5;
    }

    th {
      position: sticky;
      top: 0;
      background: #f1f5f9;
      font-weight: 600;
      color: var(--text);
      z-index: 10;
      white-space: nowrap;
    }

    th:first-child { border-top-left-radius: var(--radius-md); }
    th:last-child { border-top-right-radius: var(--radius-md); }

    td {
      color: var(--muted);
      max-width: 500px;
    }

    tbody tr:hover {
      background-color: var(--accent-light);
    }

    tbody tr:last-child td { border-bottom: none; }

    .empty-state {
      border: 1px dashed var(--muted);
      border-radius: var(--radius-lg);
      padding: 32px 24px;
      color: var(--muted);
      background: var(--bg);
      text-align: center;
      font-weight: 500;
    }

    @media (max-width: 900px) {
      .page-header { padding: 24px; }
      h1 { font-size: 28px; }
    }
    @media (max-width: 560px) {
      main { margin: 20px auto; }
    }
    """
