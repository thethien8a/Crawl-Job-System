Response for storing clean data, serving for Supabase and Clickhouse

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