from __future__ import annotations

import logging
import os
import sys
from dataclasses import fields
from pathlib import Path

# Mute GE's tqdm progress bar before the library is imported.
os.environ.setdefault("GX_PROGRESS_BARS_ENABLED", "False")

import great_expectations as gx  # noqa: E402
import pandas as pd  # noqa: E402

from src.crawl_layer.config.path import TEMP_DIR  # noqa: E402
from src.crawl_layer.data_model.data_class import ITViecJobItem, JobItem  # noqa: E402

logger = logging.getLogger(__name__)

FILE_PREFIX = "itviec_jobs"

# Required columns: allow up to 5% null (mostly >= 0.95 means < 5% null still passes).
REQUIRED_MIN_NON_NULL_RATIO = 0.95

# Optional columns: at least 5% of rows must be non-null.
OPTIONAL_MAX_NULL_RATIO = 0.95
OPTIONAL_MIN_NON_NULL_RATIO = round(1 - OPTIONAL_MAX_NULL_RATIO, 4)

EXCEPTIONAL_OPTION_COL = [
    "job_industry"
]
EXCEPTIONAL_MIN_NON_NULL_RATIO = 0.8
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


def _split_columns() -> tuple[list[str], list[str]]:
    """Split the ITViec schema into (required, optional) column groups.

    Required = base ``JobItem`` fields, optional = whatever ITViec adds
    on top. Deriving these from the dataclasses keeps the validator in
    sync with the schema automatically.
    """
    base_fields = {f.name for f in fields(JobItem)}
    all_fields = [f.name for f in fields(ITViecJobItem)]
    required = [name for name in all_fields if name in base_fields and name not in EXCEPTIONAL_OPTION_COL]
    optional = [name for name in all_fields if name not in base_fields]
    
    logger.info(f"Required columns: {required}")
    logger.info(f"Optional columns: {optional}")
    
    return required, optional


def _build_suite(
    context: gx.data_context.AbstractDataContext,
    required: list[str],
    optional: list[str],
) -> gx.ExpectationSuite:
    suite = context.suites.add(gx.ExpectationSuite(name="itviec_suite"))

    # Required columns: allow up to 5% null (mostly >= 0.95).
    for column in required:
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column=column, mostly=REQUIRED_MIN_NON_NULL_RATIO
            )
        )

    # Optional columns: at least 5% non-null is enough.
    for column in optional:
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column=column, mostly=OPTIONAL_MIN_NON_NULL_RATIO
            )
        )

    # Exceptional columns: more lenient threshold (e.g. job_industry).
    for column in EXCEPTIONAL_OPTION_COL:
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column=column, mostly=EXCEPTIONAL_MIN_NON_NULL_RATIO
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

    try:
        df = pd.read_json(jsonl_path, lines=True)
    except Exception as e:
        logger.error(f"Failed to read/parse {jsonl_path.name}. Error: {e}")
        return False
        
    logger.info(f"Loaded {len(df):,} rows from {jsonl_path.name}")

    required, optional = _split_columns()

    context = gx.get_context(mode="ephemeral")
    data_source = context.data_sources.add_pandas("itviec_pandas")
    data_asset = data_source.add_dataframe_asset("itviec_jobs")
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        "itviec_batch"
    )
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    suite = _build_suite(context, required, optional)
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
