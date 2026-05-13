"""CLI entry point for VietnamWorks crawler.

Usage:
    python -m src.crawl_layer.crawler.vietnamworks --keyword "data analyst" --max-pages 2
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys

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

from .crawler import VietnamWorksCrawler


async def _run(keyword: str, max_pages: int, headless: bool) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    crawler = VietnamWorksCrawler(
        keyword=keyword,
        max_pages=max_pages,
        headless=headless,
    )

    items = await crawler.crawl()

    logging.info("Exported %d items to vietnamworks_jobs.jsonl", len(items))


def main() -> None:
    parser = argparse.ArgumentParser(description="VietnamWorks async crawler")
    parser.add_argument("--keyword", default="data analyst")
    parser.add_argument("--max-pages", type=int, default=2)
    parser.add_argument(
        "--no-headless",
        action="store_true",
        help="Show the browser window.",
    )
    args = parser.parse_args()

    asyncio.run(
        _run(args.keyword, args.max_pages, headless=not args.no_headless)
    )


if __name__ == "__main__":
    main()
