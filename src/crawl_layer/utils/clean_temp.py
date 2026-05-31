import os
import glob
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define the relative path to the temp directory
from src.crawl_layer.config.path import TEMP_DIR

def clean_temp_directory(prefix: str | None = None):
    """
    Delete files in the temp directory.

    :param prefix: When provided, only delete files whose **basename** starts
        with this string (e.g. ``"topcv"``). When ``None``, delete all files.
    """
    if not os.path.exists(TEMP_DIR):
        logging.info(f"Directory {TEMP_DIR} does not exist. Skipping cleanup.")
        return

    # Get a list of all files in the temp directory
    files = glob.glob(os.path.join(TEMP_DIR, '*'))
    
    if not files:
        logging.info("Temp directory is empty. Nothing to delete.")
        return

    deleted_count = 0
    for file_path in files:
        # Only delete files, skip if there are subdirectories
        if os.path.isfile(file_path):
            if prefix and not os.path.basename(file_path).startswith(prefix):
                continue
            try:
                os.remove(file_path)
                logging.info(f"Deleted: {os.path.basename(file_path)}")
                deleted_count += 1
            except Exception as e:
                logging.error(f"Cannot delete file {file_path}. Error: {e}")
                
    logging.info(f"Done cleaning! Deleted {deleted_count} file(s).")

# if __name__ == "__main__":
#     # Allow running this script independently from the terminal
#     clean_temp_directory()
