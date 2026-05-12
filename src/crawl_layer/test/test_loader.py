from dataclasses import asdict
from pathlib import Path
import tempfile
import shutil

from src.crawl_layer.data_model.data_class import (
    JobItem,
)
from src.crawl_layer.utils.loader import save_to_temp

def test_save_list_of_items():
    """Test saving multiple items as a list"""
    items = [
        JobItem(job_title="Job A", company_name="Co A", scraped_at="2026-05-12"),
        JobItem(job_title="Job B", company_name="Co B", scraped_at="2026-05-12"),
    ]
    path = save_to_temp([asdict(i) for i in items], "multi", "jobs")
    print(f"File saved to: {path}")
    
    if path.exists():
        print("✓ File exists")
        content = path.read_text(encoding="utf-8").strip()
        lines = content.splitlines()
        print(f"Number of lines: {len(lines)}")
        print(f"Content: {content}")
        
        if len(lines) == 2 and "Job A" in lines[0] and "Job B" in lines[1]:
            print("✓ Multiple items verification passed")
        else:
            print("✗ Multiple items verification failed")
    else:
        print("✗ File does not exist")


if __name__ == "__main__":
    print("Test 3: Save list of items")
    test_save_list_of_items()
    print()
 
