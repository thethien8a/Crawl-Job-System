"""LinkedIn crawler package — public surface.

Importers should only need:
    from src.crawl_layer.crawler.linkedin import LinkedinCrawler

Internal modules (parser, browser, config, utils) are implementation
details and may be reorganised without notice.
"""

from .crawler import LinkedinCrawler

__all__ = ["LinkedinCrawler"]
