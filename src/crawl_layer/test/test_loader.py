from dataclasses import asdict
from pathlib import Path
import tempfile
import shutil
import logging

from src.crawl_layer.data_model.data_class import (
    JobItem,
)
from src.crawl_layer.utils.loader import save_to_temp

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

def test_save_list_of_items():
    """Test saving multiple items as a list"""
    items = [
        JobItem(job_title="Job A", company_name="Co A", scraped_at="2026-05-12"),
        JobItem(job_title="Job B", company_name="Co B", scraped_at="2026-05-12"),
    ]
    path = save_to_temp([asdict(i) for i in items], "multi", "jobs")
    logger.info("File saved to: %s", path)
    
    if path.exists():
        logger.info("✓ File exists")
        content = path.read_text(encoding="utf-8").strip()
        lines = content.splitlines()
        logger.info("Number of lines: %d", len(lines))
        logger.info("Content: %s", content)
        
        if len(lines) == 2 and "Job A" in lines[0] and "Job B" in lines[1]:
            logger.info("✓ Multiple items verification passed")
        else:
            logger.info("✗ Multiple items verification failed")
    else:
        logger.info("✗ File does not exist")


if __name__ == "__main__":
    logger.info("Test 3: Save list of items")
    test_save_list_of_items()
    logger.info("")
 
