"""CLI entry point.

Usage:
    python -m src.crawl_layer.crawler.itviec --keyword "data analyst" --max-pages 2

Requires environment variables ITVIEC_USERNAME and ITVIEC_PASSWORD to be
set before running (see .env.example).
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from src.crawl_layer.utils.loader import save_to_temp   
# --- Patch for Windows asyncio ProactorEventLoop cleanup issues ---
if sys.platform == "win32":
    try:
        from asyncio.proactor_events import _ProactorBasePipeTransport
        from asyncio.base_subprocess import BaseSubprocessTransport

        def silence_del(cls):
            orig_del = getattr(cls, "__del__", None)
            if not orig_del:
                return
            def new_del(self):
                try:
                    orig_del(self)
                except Exception:
                    pass
            cls.__del__ = new_del

        silence_del(_ProactorBasePipeTransport)
        silence_del(BaseSubprocessTransport)
    except ImportError:
        pass
# -----------------------------------------------------------------

from .crawler import ItviecCrawler


async def _run(keyword: str, max_pages: int, headless: bool) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    crawler = ItviecCrawler(
        keyword=keyword,
        max_pages=max_pages,
        headless=headless,
    )

    items = await crawler.crawl()

    save_to_temp(items, "itviec", "jobs")

    logging.info("Exported %d items to itviec_jobs.jsonl", len(items))


def main() -> None:
    parser = argparse.ArgumentParser(description="ITviec async crawler")
    parser.add_argument("--keyword", default="data analyst")
    parser.add_argument("--max-pages", type=int, default=2)
    parser.add_argument(
        "--no-headless",
        action="store_true",
        help="Show the browser window (useful for debugging Cloudflare).",
    )
    args = parser.parse_args()

    asyncio.run(
        _run(args.keyword, args.max_pages, headless=not args.no_headless)
    )


if __name__ == "__main__":
    main()
