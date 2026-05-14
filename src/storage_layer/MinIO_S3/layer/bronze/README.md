### Local Storage Structure

This layer is loaded from the `temp/` directory in local storage within the folder `crawl_layer/temp/`.

Structure of BRONZE layer:

```text
bronze/
├── itviec/
│   └── jobs/
│       └── year=2026/
│           └── month=05/
│               └── day=12/
│                   └── itviec_jobs_20260512_170702.jsonl.gz
└── linkedin/
    └── jobs/
        └── year=2026/
            └── month=05/
                └── day=12/
                    └── linkedin_jobs_20260512_170702.jsonl.gz
```

**For more information, please refer to source code: `src/crawl_layer/crawler/utils/`**
