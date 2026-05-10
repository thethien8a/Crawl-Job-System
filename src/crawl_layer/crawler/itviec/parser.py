"""HTML → JobItem extraction for ITviec preview panels.

Pure parsing layer: takes the raw HTML of an ITviec preview-job-wrapper and
returns a JobItem. No HTTP, no I/O, no browser. Selectors are ported from
the original Scrapy spider verbatim — many ITviec class names look noisy
but they are stable identifiers, do not "simplify" without checking the
live page first.
"""

from __future__ import annotations

from datetime import datetime

from parsel import Selector

from src.crawl_layer.data_model.data_class import ITViecJobItem

from .config import (
    BENEFITS_SELECTOR,
    COMPANY_SELECTOR,
    DESCRIPTION_SELECTOR,
    INDUSTRY_SELECTOR,
    LOCATION_SELECTOR,
    REQUIREMENTS_SELECTOR,
    SALARY_SELECTOR,
    SOURCE_NAME,
    TITLE_SELECTOR,
    COMPANY_SIZE_SELECTOR,
)
from .utils import join_clean


class ItviecParser:
    """Stateless parser for ITviec preview-panel HTML.

    Method-on-class (not module functions) so future per-field overrides
    can be added via subclassing without rewriting the orchestrator —
    same convention as TopcvParser.
    """

    def parse_preview_panel(
        self,
        panel_html: str,
        job_url: str,
        keyword: str,
    ) -> ITViecJobItem:
        """Build a JobItem from a single preview-job-wrapper HTML fragment."""
        sel = Selector(text=panel_html)

        item = ITViecJobItem(
            job_url=job_url,
            search_keyword=keyword,
            scraped_at=datetime.now().isoformat(),
            source_site=SOURCE_NAME,
        )

        item.job_title = self._extract_text(sel, TITLE_SELECTOR)
        item.company_name = self._extract_text(sel, COMPANY_SELECTOR)
        item.salary = self._extract_text(sel, SALARY_SELECTOR)
        item.location = self._extract_text(sel, LOCATION_SELECTOR)
        item.job_industry = self._extract_text(sel, INDUSTRY_SELECTOR)
        item.job_description = self._extract_block(sel, DESCRIPTION_SELECTOR)
        item.requirements = self._extract_block(sel, REQUIREMENTS_SELECTOR)
        item.benefits = self._extract_block(sel, BENEFITS_SELECTOR)
        item.company_size = self._extract_company_size(sel, COMPANY_SIZE_SELECTOR)

        return item

    # -- helpers -----------------------------------------------------------
    @staticmethod
    def _extract_text(sel: Selector, css: str) -> str | None:
        """Extract all nested text from the first matched element, trimmed."""
        match = sel.css(css)
        if not match:
            return None
        
        # Lấy tất cả text node bên trong element đầu tiên khớp
        parts = match[0].css("::text").getall()
        cleaned = " ".join(p.strip() for p in parts if p.strip())
        return cleaned or None


    @staticmethod
    def _extract_company_size(sel: Selector, css: str) -> str | None:
        """Extract company size from the company size selector."""
        match = sel.css(css)
        if not match:
            return None
        
        # Lấy text node thứ 2 (chỉ số 1) từ element đầu tiên khớp
        text = match[1].css("::text").get()
        return text.strip() if text else None

    @staticmethod
    def _extract_block(sel: Selector, css: str) -> str | None:
        """All descendant text, joined and trimmed.

        Used for free-form sections (description, requirements, benefits)
        where formatting tags are scattered through the markup.
        """
        parts = sel.css(f"{css} ::text").getall()
        return join_clean(parts)
