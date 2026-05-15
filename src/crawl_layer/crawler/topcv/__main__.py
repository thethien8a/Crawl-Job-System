"""CLI entry point.

Usage:
    python -m src.crawl_layer.crawler.topcv --keyword "data analyst" --max-pages 2
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from .crawler import TopcvCrawler
from .config import DEFAULT_KEYWORD, DEFAULT_MAX_PAGES

async def _run(keyword: str, max_pages: int) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    crawler = TopcvCrawler(
        keyword=keyword, 
        max_pages=max_pages,
        concurrency=5,  
        request_delay=(4.0, 6.0) 
    )
    
    items = await crawler.crawl()

    logging.info("Exported %d items to topcv_jobs.jsonl", len(items))

def main() -> None:
    parser = argparse.ArgumentParser(description="TopCV async crawler")
    parser.add_argument("--keyword", default=DEFAULT_KEYWORD)
    parser.add_argument("--max-pages", type=int, default=DEFAULT_MAX_PAGES)
    args = parser.parse_args()

    asyncio.run(_run(args.keyword, args.max_pages))


if __name__ == "__main__":
    main()
