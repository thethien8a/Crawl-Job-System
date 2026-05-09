"""CLI entry point.

Usage:
    python -m src.crawl_layer.crawler.topcv --keyword "data analyst" --max-pages 2
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
from dataclasses import asdict

from .crawler import TopcvCrawler


async def _run(keyword: str, max_pages: int) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    crawler = TopcvCrawler(
        keyword=keyword, 
        max_pages=max_pages,
        concurrency=1,  # Phải hạ xuống 1 để tránh Cloudflare phát hiện
        request_delay=(7.0, 12.0)  # Tăng delay lên cao hơn nữa
    )
    
    items = await crawler.crawl()
    
    with open("topcv_items.json", "w", encoding="utf-8") as f:
        json.dump([asdict(item) for item in items], f, ensure_ascii=False, indent=2)
    
    logging.info("Exported %d items to topcv_items.json", len(items))

def main() -> None:
    parser = argparse.ArgumentParser(description="TopCV async crawler")
    parser.add_argument("--keyword", default="data analyst")
    parser.add_argument("--max-pages", type=int, default=2)
    args = parser.parse_args()

    asyncio.run(_run(args.keyword, args.max_pages))


if __name__ == "__main__":
    main()
