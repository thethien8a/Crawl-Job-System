from dataclasses import dataclass, field
from typing import List, Optional, Union, get_args, get_origin, get_type_hints

import polars as pl


@dataclass
class SilverJobItem:
    # ==========================================
    # 1. METADATA & SOURCE
    # ==========================================
    job_url: Optional[str] = None
    search_keyword: Optional[str] = None
    job_deadline: Optional[str] = None

    # ==========================================
    # 2. JOB TITLE
    # ==========================================
    job_title: Optional[str] = None
    clean_job_title: Optional[str] = None
    job_title_special_keywords: List[str] = field(default_factory=list)

    # ==========================================
    # 3. COMPANY INFO
    # ==========================================
    company_name: Optional[str] = None
    company_name_canonical: Optional[str] = None
    company_size: Optional[str] = None
    min_company_size: Optional[int] = None
    max_company_size: Optional[int] = None

    # ==========================================
    # 4. LOCATION
    # ==========================================
    location: Optional[str] = None
    clean_location: Optional[str] = None
    is_vietnam: Optional[str] = None

    # ==========================================
    # 5. JOB INDUSTRY
    # ==========================================
    job_industry_clean: List[str] = field(default_factory=list)
    job_industry_unmapped: List[str] = field(default_factory=list)

    # ==========================================
    # 6. JOB DETAILS (Type, Position)
    # ==========================================
    job_type: Optional[str] = None
    job_position: Optional[str] = None

    # ==========================================
    # 7. EXPERIENCE & EDUCATION
    # ==========================================
    experience_level: Optional[str] = None
    min_exp_level: Optional[float] = None
    max_exp_level: Optional[float] = None
    education_level: Optional[str] = None

    # ==========================================
    # 8. SALARY & BENEFITS
    # ==========================================
    salary: Optional[str] = None
    min_monthly_salary: Optional[float] = None
    max_monthly_salary: Optional[float] = None
    benefits: Optional[str] = None
    benefits_text_clean: Optional[str] = None
    benefits_categories_vi: List[str] = field(default_factory=list)

    # ==========================================
    # 9. JOB DESCRIPTION
    # ==========================================
    job_description: Optional[str] = None
    job_description_cleaned: Optional[str] = None

    # ==========================================
    # 10. REQUIREMENTS & SKILLS
    # ==========================================
    requirements: Optional[str] = None
    requirements_cleaned: Optional[str] = None

    require_programming_languages: List[str] = field(default_factory=list)
    require_frameworks: List[str] = field(default_factory=list)
    require_tools: List[str] = field(default_factory=list)
    require_cloud_skills: List[str] = field(default_factory=list)
    require_knowledge: List[str] = field(default_factory=list)
    require_domain_knowledge: List[str] = field(default_factory=list)
    require_foreign_languages: List[str] = field(default_factory=list)
    require_domain_university: List[str] = field(default_factory=list)


# Utils function to convert Python types to Polars types
_SIMPLE_TYPE_MAP: dict[type, pl.DataType] = {
    str:   pl.String,
    int:   pl.Int64,
    float: pl.Float64,
    bool:  pl.Boolean,
}


def _unwrap_optional(tp: type) -> type:
    """Strip Optional[X] (Union[X, None]) wrapper, returning X."""
    if get_origin(tp) is Union:
        non_none = [a for a in get_args(tp) if a is not type(None)]
        return non_none[0] if non_none else tp
    return tp


def silver_schema_to_polars() -> dict[str, pl.DataType]:
    """Derive a Polars column-name -> dtype mapping from SilverJobItem.

    Any field added or removed from the dataclass automatically appears
    in the returned schema, so the dataclass remains the single source
    of truth for the Silver layer column contract.
    """
    hints = get_type_hints(SilverJobItem)
    schema: dict[str, pl.DataType] = {}
    for name, tp in hints.items():
        unwrapped = _unwrap_optional(tp)
        origin = get_origin(unwrapped)

        if origin is list:
            inner_args = get_args(unwrapped)
            inner = inner_args[0] if inner_args else str
            schema[name] = pl.List(_SIMPLE_TYPE_MAP.get(inner, pl.String))
        elif unwrapped in _SIMPLE_TYPE_MAP:
            schema[name] = _SIMPLE_TYPE_MAP[unwrapped]
        else:
            # Unknown type -> fallback to String so the column is still present.
            schema[name] = pl.String
    return schema