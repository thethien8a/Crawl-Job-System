from dataclasses import dataclass
from typing import Optional

@dataclass
class JobItem:
    job_title: Optional[str] = None
    company_name: Optional[str] = None
    salary: Optional[str] = None
    location: str | None = None
    job_type: str | None = None
    experience_level: str | None = None
    education_level: str | None = None
    job_industry: str | None = None
    job_position: str | None = None
    job_description: str | None = None
    requirements: str | None = None
    benefits: str | None = None
    job_deadline: str | None = None
    source_site: str | None = None
    job_url: str | None = None
    search_keyword: str | None = None
    scraped_at: str | None = None

