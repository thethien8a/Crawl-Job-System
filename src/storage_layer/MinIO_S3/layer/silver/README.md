Response for storing clean data, serving for Supabase and Clickhouse

## Google Sheets seed source

Seed CSVs in `seeds/` and the fuzzy-review output `utils/clusters_review.csv` can be stored in one Google Spreadsheet, with one worksheet per CSV file.

Required environment variables:

- `GOOGLE_SHEETS_CREDENTIALS_FILE`: path to the Google service-account JSON key.
- `GOOGLE_SHEETS_SPREADSHEET_ID`: spreadsheet ID from the Google Sheets URL.

First sync from local CSV files to Google Sheets:

```bash
python -m src.storage_layer.MinIO_S3.layer.silver.scripts.sync_local_csv_to_google_sheets
```

At runtime, `read_seeds(<file>.csv)` reads the worksheet named after the CSV stem first, then falls back to the local CSV file if Google Sheets is not configured or unavailable. For example, `company_mapping.csv` maps to worksheet `company_mapping`, and `clusters_review.csv` maps to worksheet `clusters_review`.

Structure of Silver layer:
```
silver/
├── {entity_name}/
│   ├── source_site={site}/
│   │   ├── year={year}/
│   │   │   ├── month={month}/
│   │   │   │   ├── day={day}/
│   │   │   │   │   ├── clean_bronze_{timestamp}.parquet
│   │   │   │   │   └── clean_bronze_{timestamp}.parquet
```

We not overwrite the data, we just append the new data to the existing data.
