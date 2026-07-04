from dataclasses import dataclass
from datetime import date


@dataclass
class JobData:
    job_url: str
    unique_url: str
    job_title: str
    company_name: str
    location: str
    job_deadline: str
    job_title_special_keywords: list[str]
    source_site: str
    scraped_at: date
