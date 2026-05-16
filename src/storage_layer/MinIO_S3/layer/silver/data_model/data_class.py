from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class SilverJobItem:
    job_title: Optional[str] = None
    company_name: Optional[str] = None
    location: Optional[str] = None
    # Raw value preserved verbatim for audit / re-classification.
    job_industry_raw: Optional[str] = None
    # Multi-label list of canonical Vietnamese industry names.
    job_industries: List[str] = field(default_factory=list)
    # Highest-confidence single label (used by most BI dashboards).
    job_industry_primary: Optional[str] = None
    # Parent group (e.g. "Công nghệ") for rollup analytics.
    job_industry_l1: Optional[str] = None
    # How job_industry_primary was derived: exact|keyword|fuzzy|unknown|empty.
    industry_mapping_method: Optional[str] = None
    industry_mapping_confidence: float = 0.0
    job_description: Optional[str] = None
    source_site: Optional[str] = None
    job_url: Optional[str] = None
    search_keyword: Optional[str] = None
    scraped_at: Optional[str] = None
    salary: Optional[str] = None
    benefits: Optional[str] = None
    requirements: Optional[str] = None
    company_size: Optional[str] = None
    job_type: Optional[str] = None
    experience_level: Optional[str] = None
    education_level: Optional[str] = None
    job_position: Optional[str] = None
    job_deadline: Optional[str] = None
