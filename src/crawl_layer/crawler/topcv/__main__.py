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


async def _run(keyword: str, max_pages: int, headless: bool) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    crawler = TopcvCrawler(
        keyword=keyword, max_pages=max_pages, headless=headless
    )
    items = await crawler.crawl()
    print(json.dumps([asdict(i) for i in items], ensure_ascii=False, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description="TopCV async crawler")
    parser.add_argument("--keyword", default="data analyst")
    parser.add_argument("--max-pages", type=int, default=2)
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run nodriver headless (less reliable vs. CF; default off).",
    )
    args = parser.parse_args()

    asyncio.run(_run(args.keyword, args.max_pages, args.headless))


if __name__ == "__main__":
    main()
