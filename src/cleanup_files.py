#!/usr/bin/env python3
"""
Script to clean up and organize files between data directories.
"""

import glob
import logging
import os
import shutil

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def cleanup_files() -> None:
    """Move files between directories to organize the data folder."""
    base = "data"

    # 1. waiting_room -> used_input_files
    waiting_files = glob.glob(os.path.join(base, "waiting_room", "*.csv"))
    for f in waiting_files:
        dest = os.path.join(base, "used_input_files", os.path.basename(f))
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        shutil.move(f, dest)
        logger.info(f"Moved {os.path.basename(f)} to used_input_files")

    # 2. ticker_room -> ready_to_use
    ticker_files = glob.glob(os.path.join(base, "ticker_room", "*.csv"))
    for f in ticker_files:
        dest = os.path.join(base, "ready_to_use", os.path.basename(f))
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        shutil.move(f, dest)
        logger.info(f"Moved {os.path.basename(f)} to ready_to_use")

    # 3. ready_to_use (non-enhanced) -> used_input_files
    ready_files = glob.glob(os.path.join(base, "ready_to_use", "*.csv"))
    for f in ready_files:
        if "_enhanced" not in os.path.basename(f):
            dest = os.path.join(base, "used_input_files", os.path.basename(f))
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            shutil.move(f, dest)
            logger.info(f"Moved {os.path.basename(f)} to used_input_files")


if __name__ == "__main__":
    cleanup_files()
