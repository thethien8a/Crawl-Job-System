"""HTML → TopCVJobItem extraction for TopCV pages.

Pure parsing layer: takes raw HTML strings, returns dataclass instances. No
HTTP, no I/O. Selectors and brand vs non-brand branching are ported verbatim
from the original Scrapy spider — do not "simplify" them without testing
both page templates, the markup is genuinely different.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timedelta

from parsel import Selector

from src.crawl_layer.data_model.data_class import TopCVTopCVJobItem
from .config import SOURCE_NAME, UNWANTED_TITLE_FRAGMENTS
from .utils import join_clean, sanitize_title


class TopcvParser:
    """Stateless parser. Method-on-class kept (not module functions) so the
    extractors can share helpers via `self` and be subclassed if a future
    TopCV layout change needs an override per field."""

    # -- public surface -----------------------------------------------------
    def parse_search_page(self, html: str) -> tuple[list[str], str | None]:
        """Return (job_urls_on_page, next_page_url_or_None)."""
        sel = Selector(text=html)
        page_urls = sel.css(
            'a[href*="/viec-lam/"]::attr(href), '
            'a[href*="/brand/"][href*="tuyen-dung"]::attr(href)'
        ).getall()
        next_url = sel.css('a[data-href*="?page="]::attr(data-href)').get()
        return page_urls, next_url

    def parse_job_detail(self, html: str, url: str, keyword: str) -> TopCVJobItem:
        sel = Selector(text=html)
        is_brand = "brand" in url

        item = TopCVJobItem(
            job_url=url,
            search_keyword=keyword,
            scraped_at=datetime.now().isoformat(),
            source_site=SOURCE_NAME,
        )

        item.job_title = sanitize_title(
            self._extract_title(sel, is_brand), UNWANTED_TITLE_FRAGMENTS
        )
        item.company_name = self._extract_company(sel, is_brand)
        item.company_size = self._extract_company_size(sel, is_brand)
        item.salary = self._extract_salary(sel, is_brand)
        item.location = self._extract_location(sel, is_brand)
        item.job_type = self._extract_label(sel, "Hình thức làm việc", is_brand)

        # Experience: JS object first, then brand fallback, then sentinel value.
        experience = self._extract_from_js(sel, "experience")
        if not experience and is_brand:
            experience = self._extract_label(sel, "Kinh nghiệm", True)
        item.experience_level = experience or None
        
        item.education_level = self._extract_label(sel, "Học vấn", is_brand)
        item.job_industry = self._extract_industry(sel)
        item.job_position = self._extract_label(sel, "Cấp bậc", is_brand)
        item.job_deadline = self._extract_deadline(sel, is_brand)
        item.job_description = self._extract_paragraph(sel, "Mô tả công việc")
        item.requirements = self._extract_paragraph(sel, "Yêu cầu ứng viên")
        item.benefits = self._extract_paragraph(sel, "Quyền lợi")

        return item

    # -- field extractors ---------------------------------------------------
    def _extract_title(self, sel: Selector, is_brand: bool) -> str | None:
        title = self._extract_from_js(sel, "job_title")
        if title:
            return title.strip()

        if is_brand:
            # Lấy chính xác h2 chứa title của job để không bị nhầm lẫn với các thẻ khác
            title = sel.css('h2.job-title::text').get()
            if not title:
                title = sel.css('div.job-detail__info--title h1::text').get()
            if not title:
                parts = sel.css("h2.premium-job-basic-information__content--title::text").getall()
                title = join_clean(parts)
            return title

        parts = sel.css("h1.box-header-job__title ::text").getall()
        return join_clean(parts)

    def _extract_company(self, sel: Selector, is_brand: bool) -> str | None:
        if is_brand:
            company = sel.css(
                'div[class="footer-info-content footer-info-company-name"]::text'
            ).get()
            if not company:
                company = sel.css(
                    'h1[class="company-content__title--name"]::text'
                ).get()
            if not company:
                company = sel.css('a.company-content__name h1.title::text').get()
            return company.strip() if company else None

        company = self._extract_from_js(sel, "recruiter_company")
        if company:
            return company.strip()
        parts = sel.css("div.box-job-info a.text-dark-blue ::text").getall()
        if not parts:
            parts = sel.css('a[class="name"][href*="cong-ty"] ::text').getall()
        return join_clean(parts)

    def _extract_company_size(self, sel: Selector, is_brand: bool) -> str | None:
        if is_brand:
            xp = (
                '//*[contains(text(), "Quy mô")]'
                '/following-sibling::*[position()<=2]//text()'
            )
            return join_clean(sel.xpath(xp).getall())

        xp = (
            '//*[contains(@class, "company-title") and contains(normalize-space(), "Quy mô")]'
            '/following-sibling::*[contains(@class, "company-value")]//text()'
        )
        return join_clean(sel.xpath(xp).getall())

    def _extract_salary(self, sel: Selector, is_brand: bool) -> str | None:
        if is_brand:
            return self._extract_label(sel, "Mức lương", True) or self._extract_label(
                sel, "Thu nhập", True
            )

        salary = self._extract_from_js(sel, "salary_range")
        if salary:
            return salary.strip()
        salary = sel.css("h4.box-header-job__salary::text").get()
        return salary.strip() if salary else None

    def _extract_location(self, sel: Selector, is_brand: bool) -> str | None:
        if is_brand:
            location = self._extract_label(sel, "Địa điểm", True)
            if location:
                return location
            parts = sel.xpath(
                '//*[contains(@class, "premium-job-basic-information__content")]'
                '//*[contains(@class, "item")][.//*[contains(@class, "label") and '
                '(contains(normalize-space(), "Địa điểm") or '
                'contains(normalize-space(), "Địa điểm làm việc"))]]'
                '//*[contains(@class, "value") or contains(@class, "content")]//text()'
            ).getall()
            return join_clean(parts)

        location = self._extract_from_js(sel, "work_location")
        if location:
            return location.strip()
        parts = sel.css("span.hight-light.city-name ::text").getall()
        text = " ".join(p.strip() for p in parts if p.strip())
        text = text.replace("Địa điểm:", "").replace("&nbsp", "").strip()
        return text or None

    def _extract_industry(self, sel: Selector) -> str | None:
        industry = self._extract_from_js(sel, "job_category")
        if industry:
            return industry.strip()
        industry = sel.xpath(
            '//a[contains(@href, "cong-ty") and contains(@class, "text-dark-blue")]'
            "/following-sibling::*[1]//text()"
        ).get()
        return industry.strip() if industry else None

    def _extract_deadline(self, sel: Selector, is_brand: bool) -> str | None:
        if is_brand:
            deadline = self._extract_label(sel, "Hạn nộp hồ sơ", True)
            if deadline:
                return deadline
            for text in sel.css('span[class="deadline"] ::text').getall():
                m = re.search(r"\d+", text)
                if m:
                    days = int(m.group())
                    return (datetime.now() + timedelta(days=days)).strftime("%d/%m/%Y")
            return None

        deadline = sel.css('div[class="job-detail__info--deadline-date"]::text').get()
        return deadline.strip() if deadline else None

    def _extract_label(
        self, sel: Selector, label_text: str, is_brand: bool
    ) -> str | None:
        # Brand and non-brand pages put the label in different containers, so
        # keep two distinct XPaths instead of trying to over-generalise.
        if is_brand:
            xp = (
                f'//*[contains(text(), "{label_text}")]'
                "/following-sibling::*[position()<=2]//text()"
            )
        else:
            xp = (
                f'//*[contains(text(), "{label_text}") and '
                'contains(@class, "box-general-group-info-title")]'
                "/following-sibling::*[position()<=2]//text()"
            )
        return join_clean(sel.xpath(xp).getall())

    def _extract_paragraph(self, sel: Selector, label_text: str) -> str | None:
        xp = (
            f'//*[contains(text(), "{label_text}")]'
            "/following-sibling::div[position()<=2]//text()"
        )
        return join_clean(sel.xpath(xp).getall())

    @staticmethod
    def _extract_from_js(sel: Selector, field_name: str) -> str | None:
        # TopCV embeds a window.qgTracking JSON-ish blob in a <script> tag;
        # parsing it directly is far cheaper and more reliable than the DOM.
        js_text = sel.xpath(
            '//script[contains(text(), "window.qgTracking")]/text()'
        ).get()
        if not js_text:
            return None
        m = re.search(rf'"{field_name}"\s*:\s*"([^"]*)"', js_text)
        if not m:
            return None
        value = m.group(1)
        if "\\" in value:
            # Round-trip through json.loads so JS-style escapes like \/ , \"
            # and \uXXXX are decoded by a parser that knows the JSON grammar
            # instead of Python's stricter unicode_escape codec (which raises
            # DeprecationWarning on \/ because it's not a Python escape).
            try:
                value = json.loads(f'"{value}"')
            except (ValueError, json.JSONDecodeError):
                value = value.replace("\\/", "/").replace('\\"', '"')
        return value
