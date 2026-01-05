#!/usr/bin/env python3
"""
Script to analyze and enrich TOP MONDE CSV files with calculated columns.
"""

import glob
import logging
import os

import numpy as np
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def _calculate_performance_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate performance columns (perf_sum, perf_norm, perfsum_2, perf_norm_2)."""
    perf_columns = [
        "Performance % 1 year",
        "Performance % 6 months",
        "Performance % 3 months",
    ]
    df["perf_sum"] = df[perf_columns].sum(axis=1)
    df["perf_norm"] = 1 + df["perf_sum"] / 1000

    perf_columns_2 = [
        "Performance % 1 month",
        "Performance % 3 months",
        "Performance % 6 months",
    ]
    df["perfsum_2"] = df[perf_columns_2].sum(axis=1)
    df["perf_norm_2"] = 1 + df["perfsum_2"] / 1000

    return df


def _calculate_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df["MRAT"] = (
        df["Simple Moving Average (21) 1 day"] / df["Simple Moving Average (200) 1 day"]
    )
    df["Diff"] = df["Price"] / df["Simple Moving Average (200) 1 day"]
    return df


def _calculate_scores(df: pd.DataFrame) -> pd.DataFrame:
    df["score"] = df["perf_norm"] + df["MRAT"]
    df["score_2"] = df["perf_norm_2"] + df["MRAT"]
    return df


def _process_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Process DataFrame by calculating all derived columns, cleaning data, and sorting."""
    df = _calculate_performance_columns(df)
    df = _calculate_technical_indicators(df)
    df = _calculate_scores(df)

    calculated_columns = [
        "perf_sum",
        "perf_norm",
        "perfsum_2",
        "perf_norm_2",
        "MRAT",
        "Diff",
        "score",
        "score_2",
    ]
    df[calculated_columns] = df[calculated_columns].fillna(0)

    numeric_columns = df.select_dtypes(include=[np.number]).columns
    df[numeric_columns] = df[numeric_columns].round(2)

    df = df.sort_values(by="score", ascending=False)
    return df


def _log_summary(df: pd.DataFrame, filename: str) -> None:
    """Log summary statistics for processed file."""
    logger.info(f"Summary for {filename}:")
    logger.info(f"  - perf_sum - Average: {df['perf_sum'].mean():.2f}%")
    logger.info(f"  - perfsum_2 - Average: {df['perfsum_2'].mean():.2f}%")
    logger.info(f"  - MRAT - Average: {df['MRAT'].mean():.4f}")
    logger.info(f"  - Diff - Average: {df['Diff'].mean():.4f}")
    logger.info(f"  - score - Average: {df['score'].mean():.4f}")
    logger.info(f"  - score_2 - Average: {df['score_2'].mean():.4f}")


def _process_single_file(input_file: str, output_file: str) -> bool:
    """
    Process a single CSV file.

    Args:
        input_file: Path to input CSV file.
        output_file: Path to output CSV file.

    Returns:
        True if processing succeeded, False otherwise.
    """
    filename = os.path.basename(input_file)

    try:
        logger.info(f"Processing file: {filename}")
        df = pd.read_csv(input_file)
        logger.info(f"Data loaded: {df.shape[0]} rows × {df.shape[1]} columns")

        df = _process_dataframe(df)

        df.to_csv(output_file, index=False)
        logger.info(f"File saved: {os.path.basename(output_file)}")

        _log_summary(df, filename)
        return True

    except Exception as e:
        logger.error(f"Error processing {filename}: {e}", exc_info=True)
        return False


def top_monde_ranking() -> bool:
    """
    Analyze and enrich TOP MONDE CSV files with calculated columns.

    Processes all TOP MONDE CSV files in the waiting_room directory,
    calculates derived columns, and saves enhanced versions to ticker_room.

    Returns:
        True if at least one file was processed successfully, False otherwise.
    """
    waiting_room_path = os.path.join("data", "waiting_room")
    ticker_room_path = os.path.join("data", "ticker_room")

    os.makedirs(ticker_room_path, exist_ok=True)

    pattern = os.path.join(waiting_room_path, "TOP MONDE*.csv")
    csv_files = glob.glob(pattern)

    if not csv_files:
        logger.warning("No TOP MONDE CSV files found in waiting_room")
        return False

    logger.info(f"Found {len(csv_files)} TOP MONDE file(s):")
    for file in csv_files:
        logger.info(f"  - {os.path.basename(file)}")

    processed_count = 0
    for input_file in csv_files:
        filename = os.path.basename(input_file)
        output_filename = filename.replace(".csv", "_enhanced.csv")
        output_file = os.path.join(ticker_room_path, output_filename)

        if os.path.exists(output_file):
            logger.info(f"Enhanced file already exists: {output_filename}. Skipping.")
            processed_count += 1
            continue

        if _process_single_file(input_file, output_file):
            processed_count += 1

    logger.info(
        f"Processing completed: {processed_count} file(s) processed out of {len(csv_files)} found"
    )

    return processed_count > 0


if __name__ == "__main__":
    logger.info("Starting top_monde_ranking...")
    success = top_monde_ranking()
    if success:
        logger.info("TOP MONDE analysis completed successfully")
    else:
        logger.error("TOP MONDE analysis failed")
