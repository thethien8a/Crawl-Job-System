"""HTML → JobItem extraction for VietnamWorks pages."""

from __future__ import annotations
import re
from datetime import datetime, timedelta

from parsel import Selector

from src.crawl_layer.data_model.data_class import VietnamWorksJobItem as JobItem

from .config import (
    COMPANY_SELECTOR,
    DEADLINE_XPATH,
    LOCATION_SELECTOR,
    SALARY_SELECTOR,
    SOURCE_NAME,
    TITLE_SELECTOR,
)
from .utils import join_clean


class VietnamWorksParser:
    """Stateless parser for VietnamWorks HTML."""

    def parse_job_detail(
        self,
        html: str,
        job_url: str,
        keyword: str,
    ) -> JobItem:
        sel = Selector(text=html)

        item = JobItem(
            job_url=job_url,
            search_keyword=keyword,
            scraped_at=datetime.now().isoformat(),
            source_site=SOURCE_NAME,
        )

        item.job_title = self._extract_text(sel, TITLE_SELECTOR)
        item.company_name = self._extract_text(sel, COMPANY_SELECTOR)
        item.salary = self._extract_text(sel, SALARY_SELECTOR)
        item.location = self._extract_text(sel, LOCATION_SELECTOR)

        # General info
        item.job_type = self._get_general_information(sel, "LOẠI HÌNH LÀM VIỆC")
        
        experience_raw = self._get_general_information(sel, "SỐ NĂM KINH NGHIỆM TỐI THIỂU")
        item.experience_level = f"{experience_raw} năm" if experience_raw else None
        
        item.education_level = self._get_general_information(sel, "TRÌNH ĐỘ HỌC VẤN TỐI THIỂU")
        item.job_industry = self._get_general_information(sel, "LĨNH VỰC")
        item.job_position = self._get_general_information(sel, "CẤP BẬC")

        # Description / Requirements / Benefits
        item.job_description = self._get_descrip_require_benefits(sel, "Mô tả công việc")
        item.requirements = self._get_descrip_require_benefits(sel, "Yêu cầu")
        item.benefits = self._get_descrip_require_benefits(sel, "phúc lợi dành cho bạn")

        # Deadline
        job_deadline_text = sel.xpath(DEADLINE_XPATH).get()
        if job_deadline_text:
            match = re.search(r"\d+", job_deadline_text)
            number_in_text = int(match.group()) if match else 0
            if "ngày" in job_deadline_text:
                item.job_deadline = (
                    datetime.now() + timedelta(days=number_in_text)
                ).strftime("%Y-%m-%d")
            else:
                item.job_deadline = (
                    datetime.now() + timedelta(days=number_in_text * 30)
                ).strftime("%Y-%m-%d")
        
        return item

    @staticmethod
    def _extract_text(sel: Selector, css: str) -> str | None:
        """Extract text from CSS selector."""
        match = sel.css(f"{css}::text").get()
        return match.strip() if match else None

    @staticmethod
    def _get_general_information(sel: Selector, label_text: str) -> str | None:
        """Find element by label text and get the following paragraph."""
        xpath = f"//label[contains(text(), '{label_text}')]/following-sibling::p//text()"
        texts = sel.xpath(xpath).getall()
        return join_clean(texts)

    @staticmethod
    def _get_descrip_require_benefits(sel: Selector, label_text: str) -> str | None:
        """Find element by heading text and get following div text."""
        xpath = f"//h2[contains(text(), '{label_text}')]/following-sibling::div//text()"
        texts = sel.xpath(xpath).getall()
        return join_clean(texts)
