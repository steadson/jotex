from pathlib import Path
import shutil
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(message)s")

DOWNLOADS_FOLDER = Path("data/downloads/new_rows")
TEMP_FOLDER = Path("data/temp")

def delete_contents(folder: Path):
    """
    Delete all files and subdirectories in the given folder.
    """
    if folder.exists():
        for item in folder.iterdir():
            if item.is_file():
                item.unlink()
            elif item.is_dir():
                shutil.rmtree(item)
        logging.info(f"Deleted all contents in: {folder}")
    else:
        logging.warning(f"Folder does not exist: {folder}")

if __name__ == "__main__":
    delete_contents(DOWNLOADS_FOLDER)
    delete_contents(TEMP_FOLDER)