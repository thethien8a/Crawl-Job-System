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
    
    # --- Structured Requirement Fields ---
    # Programming languages (e.g., Python, Java, SQL, JavaScript)
    require_programming_languages: List[str] = field(default_factory=list)
    # Database technologies (e.g., PostgreSQL, MongoDB, Redis, ClickHouse)
    require_databases: List[str] = field(default_factory=list)
    # Cloud platforms (e.g., AWS, Azure, GCP)
    require_cloud_platforms: List[str] = field(default_factory=list)
    # Cloud services (e.g., S3, Lambda, BigQuery, Dataflow)
    require_cloud_services: List[str] = field(default_factory=list)
    # Big data & ETL tools (e.g., Spark, Kafka, Airflow, dbt)
    require_big_data_tools: List[str] = field(default_factory=list)
    # ML/DL frameworks (e.g., PyTorch, TensorFlow, scikit-learn)
    require_ml_frameworks: List[str] = field(default_factory=list)
    # BI & Visualization tools (e.g., Tableau, Power BI, Plotly)
    require_visualization_tools: List[str] = field(default_factory=list)
    # NLP-specific skills (e.g., BERT, spaCy, LLM, RAG)
    require_nlp_skills: List[str] = field(default_factory=list)
    # Computer Vision skills (e.g., OpenCV, YOLO, CNN)
    require_cv_skills: List[str] = field(default_factory=list)
    # DevOps/MLOps tools (e.g., Docker, Kubernetes, MLflow, CI/CD)
    require_devops_tools: List[str] = field(default_factory=list)
    # Domain knowledge (e.g., Fintech, Healthcare, E-commerce)
    require_domain_knowledge: List[str] = field(default_factory=list)
    # Foreign languages (e.g., English, Japanese)
    require_foreign_languages: List[str] = field(default_factory=list)
    
    # Boolean flags for common requirements
    has_sql_requirement: bool = False
    has_python_requirement: bool = False
    has_cloud_requirement: bool = False
    has_ml_requirement: bool = False
    has_big_data_requirement: bool = False
