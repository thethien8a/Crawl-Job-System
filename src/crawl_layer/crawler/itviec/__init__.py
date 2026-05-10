"""ITviec crawler package — public surface.

Importers should only need:
    from src.crawl_layer.crawler.itviec import ItviecCrawler

Internal modules (parser, browser, config, utils) are implementation
details and may be reorganised without notice.
"""

from .crawler import ItviecCrawler

__all__ = ["ItviecCrawler"]
