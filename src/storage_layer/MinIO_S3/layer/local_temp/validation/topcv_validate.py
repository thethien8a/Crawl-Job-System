from __future__ import annotations
from src.crawl_layer.config.path import TEMP_DIR
import logging
import os
import sys
from pathlib import Path
import great_expectations as gx
import pandas as pd

os.environ.setdefault("GX_PROGRESS_BARS_ENABLED", "False")


logger = logging.getLogger(__name__)

FILE_PREFIX = "topcv_jobs"

# Allow up to 5% null across all columns (< 5% null still passes).
MIN_NON_NULL_RATIO = 0.95

NULLABLE_WHEN_BRAND_URL = {"company_size"}
BRAND_URL_TOKEN = "brand/"

def _latest_jsonl(directory: Path, prefix: str) -> Path:
    """Return the newest jsonl file whose name starts with ``prefix``.

    The crawler appends an ISO-style date to the filename, so a plain
    lexicographic sort already orders files chronologically.
    """
    candidates = sorted(directory.glob(f"{prefix}*.jsonl"))
    if not candidates:
        raise FileNotFoundError(
            f"No '{prefix}*.jsonl' file found under {directory}"
        )
    return candidates[-1]


def _build_suite(
    context: gx.data_context.AbstractDataContext,
    columns: list[str],
) -> gx.ExpectationSuite:
    suite = context.suites.add(gx.ExpectationSuite(name="topcv_suite"))

    # Skip rows where job_url is a brand profile page; those legitimately
    # have no company_size.
    non_brand_row_condition = (
        f'not job_url.str.contains("{BRAND_URL_TOKEN}", na=False)'
    )

    for column in columns:
        if column in NULLABLE_WHEN_BRAND_URL:
            suite.add_expectation(
                gx.expectations.ExpectColumnValuesToNotBeNull(
                    column=column,
                    row_condition=non_brand_row_condition,
                    condition_parser="pandas",
                )
            )
        else:
            suite.add_expectation(
                gx.expectations.ExpectColumnValuesToNotBeNull(
                    column=column, mostly=MIN_NON_NULL_RATIO
                )
            )

    return suite


def _print_summary(result: gx.core.ExpectationSuiteValidationResult) -> None:
    logger.info(f"\nOverall success: {result.success}")
    for item in result.results:
        cfg = item.expectation_config
        column = cfg.kwargs.get("column", "<n/a>")
        mostly = cfg.kwargs.get("mostly")
        observed = item.result.get("unexpected_percent")
        status = "PASS" if item.success else "FAIL"
        threshold = f" mostly>={mostly}" if mostly is not None else ""
        logger.info(
            f"  [{status}] {cfg.type} column={column}{threshold} "
            f"null%={observed}"
        )


def validate() -> bool:
    jsonl_path  = _latest_jsonl(TEMP_DIR, FILE_PREFIX)
    df = pd.read_json(jsonl_path, lines=True)

    columns = df.columns.tolist()

    logger.info(f"Loaded {len(df):,} rows from {jsonl_path.name}")

    context = gx.get_context(mode="ephemeral")
    data_source = context.data_sources.add_pandas("topcv_pandas")
    data_asset = data_source.add_dataframe_asset("topcv_jobs")
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        "topcv_batch"
    )
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    suite = _build_suite(context, columns)
    result = batch.validate(suite)

    _print_summary(result)
    return bool(result.success)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        stream=sys.stdout,
    )
    sys.exit(0 if validate() else 1)
