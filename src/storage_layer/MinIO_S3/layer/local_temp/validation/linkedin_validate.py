from __future__ import annotations
import logging
import os
import sys
from pathlib import Path
import great_expectations as gx
import pandas as pd
from src.crawl_layer.config.path import TEMP_DIR 
from src.crawl_layer.data_model.data_class import LinkedinJobItem, JobItem

logger = logging.getLogger(__name__)

# Mute GE's tqdm progress bar before the library is imported.
os.environ.setdefault("GX_PROGRESS_BARS_ENABLED", "False")

FILE_PREFIX = "linkedin_jobs"

MAX_NULL_RATIO = 0.5
MIN_NON_NULL_RATIO = round(1 - MAX_NULL_RATIO, 4)

COLUMNS_NULLABLE = [
    'job_type'
]

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
    """Split the Linkedin schema into (required, optional) column groups.

    Required = base ``JobItem`` fields, optional = whatever Linkedin adds
    on top. Deriving these from the dataclasses keeps the validator in
    sync with the schema automatically.
    """
    all_fields = [f.name for f in fields(LinkedinJobItem)]
    required = [name for name in all_fields if name not in COLUMNS_NULLABLE]
    optional = [name for name in all_fields if name in COLUMNS_NULLABLE]
    return required, optional

def _build_suite(
    context: gx.data_context.AbstractDataContext,
    required: list[str],
    optional: list[str],
) -> gx.ExpectationSuite:
    suite = context.suites.add(gx.ExpectationSuite(name="linkedin_suite"))

    for column in required:
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(column=column)
        )

    # ``mostly`` is the minimum acceptable share of non-null values.
    for column in optional:
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

    try:
        df = pd.read_json(jsonl_path, lines=True)
    except Exception as e:
        logger.error(f"Failed to read/parse {jsonl_path.name}. Error: {e}")
        return False
    
    if df.empty:
        logger.error(f"File {jsonl_path.name} loaded successfully but contains no data.")
        return False
    
    logger.info(f"Loaded {len(df):,} rows from {jsonl_path.name}")
    
    required, optional = _split_columns()
    
    context = gx.get_context(mode="ephemeral")
    data_source = context.data_sources.add_pandas("linkedin_pandas")
    data_asset = data_source.add_dataframe_asset("linkedin_jobs")
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        "linkedin_batch"
    )
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    suite = _build_suite(context, required, optional)
    result = batch.validate(suite)

    _print_summary(result)
    return bool(result.success)


if __name__ == "__main__":
    # Without basicConfig the root logger filters out INFO, so nothing
    # reaches stdout and the run looks silent.
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        stream=sys.stdout,
    )
    sys.exit(0 if validate() else 1)
