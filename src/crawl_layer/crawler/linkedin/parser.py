"""HTML → JobItem extraction for LinkedIn detail panels.

Pure parsing layer: takes the raw HTML of a `job-view-layout jobs-details`
panel and returns a JobItem. No HTTP, no I/O, no browser. Selectors are
ported from the original spider verbatim — LinkedIn's class names look
noisy but they are stable identifiers, do not "simplify" without checking
the live page first.
"""

from __future__ import annotations

import re
from datetime import datetime

from parsel import Selector

from src.crawl_layer.data_model.data_class import LinkedinJobItem

from .config import (
    COMPANY_SELECTOR,
    DESCRIPTION_SELECTOR,
    JOB_TYPE_SELECTOR,
    INDUSTRY_INFO_SELECTOR,
    LOCATION_SELECTOR,
    SOURCE_NAME,
    TITLE_SELECTOR,
    URL_SELECTOR,
)
from .utils import join_clean


class LinkedinParser:
    """Stateless parser for LinkedIn detail-panel HTML."""

    def parse_detail_panel(
        self,
        panel_html: str,
        keyword: str,
    ) -> LinkedinJobItem:
        """Build a LinkedinJobItem from a single detail-panel HTML fragment."""
        sel = Selector(text=panel_html)

        item = LinkedinJobItem(
            search_keyword=keyword,
            scraped_at=datetime.now().isoformat(),
            source_site=SOURCE_NAME,
        )
        
        item.job_title = self._extract_text(sel, TITLE_SELECTOR)
        item.company_name = self._extract_text(sel, COMPANY_SELECTOR)
        item.location = self._extract_text(sel, LOCATION_SELECTOR)
        item.job_description = self._extract_block(sel, DESCRIPTION_SELECTOR)
        item.job_industry = self._extract_industry(sel)
        item.company_size = self._extract_company_size(sel)
        item.job_type = self._extract_job_type(sel, JOB_TYPE_SELECTOR)
        item.job_url = self._extract_job_url(sel, URL_SELECTOR)
        return item

    # -- helpers -----------------------------------------------------------
    @staticmethod
    def _extract_text(sel: Selector, css: str) -> str | None:
        """Return all nested text of the first match, joined and trimmed."""
        match = sel.css(css)
        if not match:
            return None
        parts = match[0].css("::text").getall()
        return join_clean(parts)

    @staticmethod
    def _extract_block(sel: Selector, css: str) -> str | None:
        """All descendant text of the first match, joined and trimmed."""
        match = sel.css(css)
        if not match:
            return None
        parts = match[0].css(" ::text").getall()
        return join_clean(parts)
    
    @staticmethod
    def _extract_job_type(sel: Selector, css: str) -> str | None:
        """Extract job type from the page."""
        match = sel.css(css)
        if not match:
            return None
        parts = match[0].css("::text").getall()
        
        for text in parts:
            if text in ("On-site", "Remote", "Hybrid"):
                return text
        return None
    
    @staticmethod
    def _extract_job_url(sel: Selector, css: str) -> str | None:
        """Extract job URL from the page."""
        match = sel.css(css)
        if not match:
            return None
        return "https://www.linkedin.com" + match[0].attrib.get("href")
    
    @staticmethod
    def _extract_industry(sel: Selector) -> str | None:
        info = sel.css(INDUSTRY_INFO_SELECTOR)
        if info:
            # Lấy text nằm trực tiếp dưới div, bỏ qua thẻ span con
            direct_texts = info[0].xpath("./text()").getall()
            for text in direct_texts:
                cleaned = text.strip()
                if cleaned and cleaned not in (",", "·", "•", "-"):
                    return cleaned
        
        # Fallback: Tìm trong các thẻ top card insight
        insights = sel.css("li.job-details-jobs-unified-top-card__job-insight span::text").getall()
        for text in insights:
            cleaned = text.strip()
            # Industry thường không chứa số
            if cleaned and not re.search(r"\d", cleaned) and "·" not in cleaned:
                lower = cleaned.lower()
                if lower not in ("on-site", "remote", "hybrid", "full-time", "part-time", "contract", "internship"):
                    return cleaned
                    
        return None

    @staticmethod
    def _extract_company_size(sel: Selector) -> str | None:
        """Trích xuất quy mô công ty bằng cách tìm chuỗi có chứa số và 'nhân viên' hoặc 'employees'."""
        info = sel.css(INDUSTRY_INFO_SELECTOR)
        if info:
            # Các thông tin phụ (company size, followers) nằm trong span.jobs-company__inline-information
            inline_infos = info[0].css("span.jobs-company__inline-information::text").getall()
            
            for text in inline_infos:
                cleaned = text.strip()
                if not cleaned:
                    continue
                lower_text = cleaned.lower()
                # Tìm số nhân viên, loại trừ số lượng người theo dõi trên linkedin
                if "nhân viên" in lower_text or "employees" in lower_text or re.search(r"\d", cleaned):
                    if "linkedin" not in lower_text and "followers" not in lower_text and "trên" not in lower_text:
                        return cleaned
        
        # Fallback: Quét rộng ra các insight card khác trên đầu trang
        insights = sel.css("li.job-details-jobs-unified-top-card__job-insight span::text").getall()
        for text in insights:
            cleaned = text.strip()
            lower_text = cleaned.lower()
            if ("nhân viên" in lower_text or "employees" in lower_text) and re.search(r"\d", cleaned):
                return cleaned
                
        return None
