import logging
import os
from pathlib import Path
from typing import Any

import polars as pl
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

GOOGLE_SHEETS_CREDENTIALS_FILE_ENV = "GOOGLE_SHEETS_CREDENTIALS_FILE"
GOOGLE_SHEETS_SPREADSHEET_ID_ENV = "GOOGLE_SHEETS_SPREADSHEET_ID"
MAX_WORKSHEET_TITLE_LENGTH = 100


class GoogleSheetsError(RuntimeError):
    """Raised when Google Sheets config or API access is not available."""


def google_sheets_config_is_available() -> bool:
    load_dotenv()
    return bool(_credentials_file() and _spreadsheet_id())


def worksheet_title_for_csv(file_name_csv: str | Path) -> str:
    title = Path(file_name_csv).stem.strip()
    if not title:
        raise GoogleSheetsError(f"Cannot derive worksheet title from CSV name: {file_name_csv}")
    return title[:MAX_WORKSHEET_TITLE_LENGTH]


def open_spreadsheet() -> Any:
    gspread = _import_gspread()
    credentials_file = _required_credentials_file()
    spreadsheet_id = _required_spreadsheet_id()
    client = gspread.service_account(filename=str(credentials_file))
    return client.open_by_key(spreadsheet_id)


def read_worksheet_as_polars(worksheet_title: str) -> pl.DataFrame:
    try:
        worksheet = open_spreadsheet().worksheet(worksheet_title)
        values = worksheet.get_all_values()
    except GoogleSheetsError:
        raise
    except Exception as exc:
        raise GoogleSheetsError(f"Failed to read Google Sheets worksheet '{worksheet_title}'") from exc

    return _values_to_frame(values)


def get_or_create_worksheet(spreadsheet: Any, title: str, rows: int, cols: int) -> Any:
    gspread = _import_gspread()
    try:
        return spreadsheet.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        return spreadsheet.add_worksheet(
            title=title,
            rows=max(rows, 1),
            cols=max(cols, 1),
        )


def replace_worksheet_values(worksheet: Any, values: list[list[str]]) -> None:
    row_count = max(len(values), 1)
    col_count = max((len(row) for row in values), default=1)
    worksheet.resize(rows=row_count, cols=col_count)
    worksheet.clear()
    if values:
        worksheet.update(values=values, range_name="A1", raw=True)


def _values_to_frame(values: list[list[Any]]) -> pl.DataFrame:
    if not values:
        return pl.DataFrame()

    header = [str(cell).lstrip("\ufeff").strip() for cell in values[0]]
    while header and not header[-1]:
        header.pop()

    if not header:
        return pl.DataFrame()

    rows = [_normalize_row(row, len(header)) for row in values[1:] if _has_any_value(row)]
    if not rows:
        return pl.DataFrame(schema={column: pl.String for column in header})

    return pl.DataFrame(rows, schema=header, orient="row")


def _normalize_row(row: list[Any], width: int) -> list[str]:
    values = ["" if value is None else str(value) for value in row[:width]]
    return values + [""] * (width - len(values))


def _has_any_value(row: list[Any]) -> bool:
    return any(str(value).strip() for value in row if value is not None)


def _import_gspread() -> Any:
    try:
        import gspread
    except ImportError as exc:
        raise GoogleSheetsError("gspread is not installed. Run: pip install -r requirements.txt") from exc
    return gspread


def _required_credentials_file() -> Path:
    credentials_file = _credentials_file()
    if not credentials_file:
        raise GoogleSheetsError(f"Missing {GOOGLE_SHEETS_CREDENTIALS_FILE_ENV}")
    if not credentials_file.exists():
        raise GoogleSheetsError(f"Google Sheets credentials file does not exist: {credentials_file}")
    return credentials_file


def _required_spreadsheet_id() -> str:
    spreadsheet_id = _spreadsheet_id()
    if not spreadsheet_id:
        raise GoogleSheetsError(f"Missing {GOOGLE_SHEETS_SPREADSHEET_ID_ENV}")
    return spreadsheet_id


def _credentials_file() -> Path | None:
    value = _env_value(GOOGLE_SHEETS_CREDENTIALS_FILE_ENV)
    return Path(value) if value else None


def _spreadsheet_id() -> str | None:
    return _env_value(GOOGLE_SHEETS_SPREADSHEET_ID_ENV)


def _env_value(name: str) -> str | None:
    load_dotenv()
    value = os.getenv(name)
    return value.strip() if value else None
