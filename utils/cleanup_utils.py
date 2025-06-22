"""
Utility functions for cleaning up workflow files and directories.
"""

import logging
import shutil
from pathlib import Path


def delete_workflow_files():
    """
    Delete files in data/downloads/new_rows and data/temp folders.
    
    Returns:
        bool: True if cleanup was successful, False if any errors occurred
    """
    folders_to_clean = [
        Path("data/downloads/new_rows"),
        Path("data/temp")
    ]
    
    success = True
    
    for folder in folders_to_clean:
        if folder.exists():
            try:
                for item in folder.iterdir():
                    if item.is_file():
                        item.unlink()
                        logging.info(f"Deleted file: {item}")
                    elif item.is_dir():
                        shutil.rmtree(item)
                        logging.info(f"Deleted directory: {item}")
                logging.info(f"Cleaned folder: {folder}")
            except Exception as e:
                logging.error(f"Error cleaning folder {folder}: {e}")
                success = False
        else:
            logging.warning(f"Folder does not exist: {folder}")
    
    return success


def delete_contents(folder: Path):
    """
    Delete all files and subdirectories in the given folder.
    
    Args:
        folder (Path): Path to the folder to clean
    
    Returns:
        bool: True if cleanup was successful, False if any errors occurred
    """
    if folder.exists():
        try:
            for item in folder.iterdir():
                if item.is_file():
                    item.unlink()
                    logging.info(f"Deleted file: {item}")
                elif item.is_dir():
                    shutil.rmtree(item)
                    logging.info(f"Deleted directory: {item}")
            logging.info(f"Deleted all contents in: {folder}")
            return True
        except Exception as e:
            logging.error(f"Error cleaning folder {folder}: {e}")
            return False
    else:
        logging.warning(f"Folder does not exist: {folder}")
        return False


if __name__ == "__main__":
    # Test the function
    print("Cleaning up workflow files...")
    result = delete_workflow_files()
    print(f"Cleanup result: {result}") 