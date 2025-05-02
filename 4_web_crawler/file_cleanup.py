#!/usr/bin/env python3
"""
Script to remove all simple_crawler related files without interactive confirmation
"""

import os
import sys
import logging
import shutil

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s'
)
logger = logging.getLogger("file_cleanup")

def cleanup_files(dry_run=False):
    """List and remove files related to simple_crawler"""
    try:
        crawler_dir = os.path.dirname(os.path.abspath(__file__))
        
        # Files directly related to simple_crawler
        simple_crawler_files = [
            os.path.join(crawler_dir, "simple_crawler.py"),
            os.path.join(crawler_dir, "README_SIMPLE.md"),
            os.path.join(crawler_dir, "simple_crawler.log"),
            os.path.join(crawler_dir, "local_config.py")
        ]
        
        # Check storage directories
        storage_dir = os.path.join(crawler_dir, "storage")
        if os.path.exists(storage_dir):
            logger.info(f"Adding storage directory to removal list: {storage_dir}")
            simple_crawler_files.append(storage_dir)
        
        # Check for any log files with 'crawler' in the name
        for filename in os.listdir(crawler_dir):
            if ('crawler' in filename.lower() or 'crawl' in filename.lower()) and filename.endswith('.log'):
                full_path = os.path.join(crawler_dir, filename)
                if full_path not in simple_crawler_files:
                    logger.info(f"Adding log file to removal list: {filename}")
                    simple_crawler_files.append(full_path)
        
        # List files that will be removed
        logger.info("The following files will be removed:")
        files_to_remove = []
        
        for file_path in simple_crawler_files:
            if os.path.exists(file_path):
                logger.info(f"  - {file_path}")
                files_to_remove.append(file_path)
            else:
                logger.info(f"  - {file_path} (not found)")
        
        if dry_run:
            logger.info("Dry run mode - no files will be removed")
            return True
        
        # Remove files and directories
        for file_path in files_to_remove:
            if os.path.isdir(file_path):
                logger.info(f"Removing directory: {file_path}")
                shutil.rmtree(file_path)
            else:
                logger.info(f"Removing file: {file_path}")
                os.remove(file_path)
        
        logger.info("File cleanup completed")
        return True
        
    except Exception as e:
        logger.error(f"Error cleaning up files: {e}")
        return False

if __name__ == "__main__":
    print("Simple Crawler File Cleanup")
    print("--------------------------")
    print("This script will remove all files related to simple_crawler")
    print()
    
    # Check for dry-run flag
    dry_run = '--dry-run' in sys.argv
    
    if '--force' in sys.argv:
        # Non-interactive mode for scripting
        success = cleanup_files(dry_run)
        sys.exit(0 if success else 1)
    else:
        # Interactive mode
        if dry_run:
            print("DRY RUN MODE: Files will be listed but not removed")
            
        proceed = input("Do you want to proceed with file cleanup? (y/n): ")
        if proceed.lower() != 'y':
            print("Cleanup cancelled")
            sys.exit(0)
        
        success = cleanup_files(dry_run)
        print(f"\nFile cleanup: {'Completed' if success else 'Failed'}") 