"""TopCV crawler package — public surface.

Importers should only need:
    from src.crawl_layer.crawler.topcv import TopcvCrawler

Internal modules (parser, http_client, config, utils) are implementation
details and may be reorganised without notice.
"""

from .crawler import TopcvCrawler

__all__ = ["TopcvCrawler"]
