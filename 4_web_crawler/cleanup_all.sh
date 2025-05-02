#!/bin/bash
# Master cleanup script for web crawler - runs both MongoDB and file cleanup

set -e  # Exit on error

echo "====================================================="
echo "          WEB CRAWLER COMPLETE CLEANUP               "
echo "====================================================="
echo

# Get script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Check if scripts exist
if [ ! -f "./mongo_cleanup.py" ] || [ ! -f "./file_cleanup.py" ]; then
    echo "Error: Required cleanup scripts not found in $SCRIPT_DIR"
    exit 1
fi

# Ensure scripts are executable
chmod +x ./mongo_cleanup.py
chmod +x ./file_cleanup.py

# Step 1: MongoDB cleanup
echo "Step 1: MongoDB Cleanup"
echo "----------------------"
if [ "$1" == "--force" ]; then
    python3 ./mongo_cleanup.py --force
else 
    python3 ./mongo_cleanup.py
fi

# Step 2: File cleanup
echo
echo "Step 2: File Cleanup"
echo "------------------"
if [ "$1" == "--force" ]; then
    python3 ./file_cleanup.py --force
else
    python3 ./file_cleanup.py
fi

echo
echo "====================================================="
echo "          CLEANUP PROCESS COMPLETED                  "
echo "=====================================================" 