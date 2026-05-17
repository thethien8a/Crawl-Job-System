from dataclasses import dataclass
from typing import Optional

@dataclass
class JobItem:
    job_title: Optional[str] = None
    company_name: Optional[str] = None
    location: str | None = None
    job_industry: str | None = None
    job_description: str | None = None
    source_site: str | None = None
    job_url: str | None = None
    search_keyword: str | None = None
    scraped_at: str | None = None
    salary: Optional[str] = None
    benefits: str | None = None
    requirements: str | None = None

@dataclass
class TopCVJobItem(JobItem):
    company_size: Optional[str] = None
    job_type: str | None = None
    experience_level: str | None = None
    education_level: str | None = None
    job_position: str | None = None
    job_deadline: str | None = None

@dataclass
class ITViecJobItem(JobItem):
    company_size: Optional[str] = None

@dataclass
class VietnamWorksJobItem(JobItem):
    job_type: str | None = None
    experience_level: str | None = None
    education_level: str | None = None
    job_position: str | None = None
    job_deadline: str | None = None