from dataclasses import dataclass
from typing import Optional

@dataclass
class SilverJobItem:
    job_title: Optional[str] = None
    company_name: Optional[str] = None
    location: Optional[str] = None
    job_industry: Optional[str] = None
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
