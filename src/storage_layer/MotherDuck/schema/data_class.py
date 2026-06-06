from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class GoldJobItem:
    # ==========================================
    # 1. KEYS & METADATA
    # ==========================================
    job_url: Optional[str] = None
    source_site: Optional[str] = None  # Hive partition column exposed by Silver
    search_keyword: Optional[str] = None
    job_deadline: Optional[str] = None

    # ==========================================
    # 2. JOB TITLE
    # ==========================================
    clean_job_title: Optional[str] = None
    job_title_special_keywords: List[str] = field(default_factory=list)

    # ==========================================
    # 3. COMPANY
    # ==========================================
    company_name_canonical: Optional[str] = None
    min_company_size: Optional[int] = None
    max_company_size: Optional[int] = None

    # ==========================================
    # 4. LOCATION
    # ==========================================
    clean_location: Optional[str] = None
    is_vietnam: Optional[str] = None

    # ==========================================
    # 5. INDUSTRY
    # ==========================================
    job_industry_clean: List[str] = field(default_factory=list)

    # ==========================================
    # 6. JOB DETAILS
    # ==========================================
    job_type: Optional[str] = None
    job_position: Optional[str] = None

    # ==========================================
    # 7. EXPERIENCE & EDUCATION
    # ==========================================
    min_exp_level: Optional[float] = None
    max_exp_level: Optional[float] = None
    education_level: Optional[str] = None

    # ==========================================
    # 8. SALARY & BENEFITS
    # ==========================================
    min_monthly_salary: Optional[float] = None
    max_monthly_salary: Optional[float] = None
    benefits_categories_vi: List[str] = field(default_factory=list)

    # ==========================================
    # 9. REQUIREMENTS & SKILLS
    # ==========================================
    require_programming_languages: List[str] = field(default_factory=list)
    require_frameworks: List[str] = field(default_factory=list)
    require_tools: List[str] = field(default_factory=list)
    require_cloud_skills: List[str] = field(default_factory=list)
    require_knowledge: List[str] = field(default_factory=list)
    require_domain_knowledge: List[str] = field(default_factory=list)
    require_foreign_languages: List[str] = field(default_factory=list)
    require_domain_university: List[str] = field(default_factory=list)
