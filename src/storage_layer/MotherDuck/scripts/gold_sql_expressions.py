from dataclasses import fields
from typing import Union, get_args, get_origin

from src.storage_layer.MotherDuck.config import (
    GOLD_DATE_COLUMNS,
    GOLD_SCHEMA,
    SILVER_PARQUET_GLOB,
)
from src.storage_layer.MotherDuck.schema.data_class import GoldJobItem

NONE_TYPE = type(None)
DUCKDB_TYPE_BY_PYTHON_TYPE = {
    str: "VARCHAR",
    int: "BIGINT",
    float: "DOUBLE",
    bool: "BOOLEAN",
}
LOSSY_CAST_TYPES = {"BIGINT", "DOUBLE", "BOOLEAN"}
JOB_ID_COLUMN = "job_id"
LEGACY_COLUMN_FALLBACKS = {
    "clean_company_name": ("company_name_canonical", "company_name"),
    "clean_job_title": ("job_title",),
    "clean_location": ("location",),
}


def qualified(table_name: str) -> str:
    return f"{GOLD_SCHEMA}.{table_name}"


def read_silver_parquet_sql() -> str:
    return f"""
    read_parquet(
        '{SILVER_PARQUET_GLOB}',
        hive_partitioning = true,
        filename = true,
        union_by_name = true
    )
    """


def build_select_list(column_names: list[str], available_columns: set[str]) -> str:
    return ",\n            ".join(
        _select_expression(column_name, available_columns)
        for column_name in column_names
    )


def _select_expression(column_name: str, available_columns: set[str]) -> str:
    column_type = GOLD_COLUMN_TYPES.get(column_name, "VARCHAR")

    if column_name == "is_vietnam" and column_name in available_columns:
        return _is_vietnam_expression()

    if column_name in available_columns:
        return _cast_expression(column_name, column_type, column_name)

    fallback_expression = _fallback_expression(column_name, available_columns)
    if fallback_expression:
        return _cast_expression(fallback_expression, column_type, column_name)

    return f"CAST(NULL AS {column_type}) AS {column_name}"


def _is_vietnam_expression() -> str:
    normalized_value = "lower(trim(CAST(is_vietnam AS VARCHAR)))"
    return f"""CASE
                WHEN {normalized_value} = 'việt nam' THEN TRUE
                WHEN {normalized_value} = 'nước ngoài' THEN FALSE
                ELSE NULL
            END AS is_vietnam"""


def _cast_expression(expression: str, column_type: str, alias: str) -> str:
    cast_function = "TRY_CAST" if column_type in LOSSY_CAST_TYPES else "CAST"
    return f"{cast_function}({expression} AS {column_type}) AS {alias}"


def _fallback_expression(column_name: str, available_columns: set[str]) -> str | None:
    fallback_columns = [
        fallback_column
        for fallback_column in LEGACY_COLUMN_FALLBACKS.get(column_name, ())
        if fallback_column in available_columns
    ]
    if not fallback_columns:
        return None

    return f"COALESCE({', '.join(fallback_columns)})"


def _duckdb_type(python_type: type) -> str:
    unwrapped_type = _unwrap_optional(python_type)
    origin = get_origin(unwrapped_type)

    if origin is list:
        inner_types = get_args(unwrapped_type)
        inner_type = inner_types[0] if inner_types else str
        return f"{_duckdb_type(inner_type)}[]"

    return DUCKDB_TYPE_BY_PYTHON_TYPE.get(unwrapped_type, "VARCHAR")


def _unwrap_optional(python_type: type) -> type:
    origin = get_origin(python_type)
    if origin is not Union:
        return python_type

    non_none_types = [arg for arg in get_args(python_type) if arg is not NONE_TYPE]
    return non_none_types[0] if non_none_types else python_type


GOLD_COLUMN_TYPES = {
    field.name: _duckdb_type(field.type)
    for field in fields(GoldJobItem)
}
GOLD_COLUMN_TYPES.update({column: "VARCHAR" for column in GOLD_DATE_COLUMNS})
