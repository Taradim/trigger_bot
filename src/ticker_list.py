#!/usr/bin/env python3
"""
Script to create ticker lists for TradingView import.
"""

import glob
import logging
import os
from datetime import datetime
from typing import List

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def get_latest_top_monde_file() -> str:
    """Get the most recent TOP MONDE file from ticker_room."""
    ticker_room_path = os.path.join("data", "ticker_room")
    top_monde_files = glob.glob(
        os.path.join(ticker_room_path, "TOP MONDE*_enhanced.csv")
    )

    if not top_monde_files:
        raise FileNotFoundError("No TOP MONDE files found in data/ticker_room/")

    logger.debug(f"Found files: {top_monde_files}")

    latest_file = None
    latest_date = None

    for file_path in top_monde_files:
        filename = os.path.basename(file_path)
        try:
            if "TOP MONDE_" in filename:
                date_str = (
                    filename.replace("TOP MONDE_", "")
                    .replace("_enhanced.csv", "")
                    .replace(".csv", "")
                )
                date_str = date_str.split(" ")[0]
                file_date = datetime.strptime(date_str, "%Y-%m-%d")

                if latest_date is None or file_date > latest_date:
                    latest_date = file_date
                    latest_file = file_path
        except ValueError as e:
            logger.warning(f"Error parsing date '{date_str}': {e}")
            continue

    if latest_file is None:
        raise ValueError("No TOP MONDE file with valid date found")

    logger.info(f"Using most recent file: {os.path.basename(latest_file)}")
    return latest_file


def _format_ticker_list(dataframe: pd.DataFrame) -> List[str]:
    """Format DataFrame rows as market:symbol ticker strings."""
    ticker_list = []
    for _, row in dataframe.iterrows():
        ticker_list.append(f"{row['Exchange']}:{row['Symbol']}")
    return ticker_list


def _create_unified_list(
    top_30_list: List[str], top_50_global_list: List[str]
) -> List[str]:
    """Create unified list with sections, excluding duplicates."""
    unified_list = ["// Top 30 Big"]
    unified_list.extend(top_30_list)
    unified_list.append("// Top 50 Global")

    top_30_set = set(top_30_list)
    for ticker in top_50_global_list:
        if ticker not in top_30_set:
            unified_list.append(ticker)

    return unified_list


def _save_ticker_list(ticker_list: List[str], filepath: str) -> None:
    """Save ticker list to file."""
    with open(filepath, "w", encoding="utf-8") as f:
        for ticker in ticker_list:
            f.write(f"{ticker}\n")


def create_ticker_lists() -> bool:
    """
    Create ticker lists in TradingView format.

    Generates three lists:
    - Unified list (Top 30 Big + Top 50 Global without duplicates)
      Top 30 Big = Top 15 by score_2 + Top 15 by score (excluding duplicates)
    - Score >= 2.7 list
    - Worst 100 performers list

    Returns:
        True if successful, False otherwise.
    """
    try:
        latest_file = get_latest_top_monde_file()
        df = pd.read_csv(latest_file)
        logger.info(f"Data loaded: {df.shape[0]} rows × {df.shape[1]} columns")

        required_columns = [
            "Symbol",
            "Exchange",
            "Market capitalization",
            "score",
            "score_2",
        ]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing columns: {missing_columns}")

        df["Market capitalization"] = pd.to_numeric(
            df["Market capitalization"], errors="coerce"
        )

        df_filtered = df[df["Market capitalization"] >= 10000000000].copy()
        logger.info(f"Companies with market cap > 10B: {len(df_filtered)}")

        # Top 15 by score_2
        df_sorted_score_2 = df_filtered.sort_values(by="score_2", ascending=False)
        top_15_score_2 = df_sorted_score_2.head(15)
        top_15_score_2_list = _format_ticker_list(top_15_score_2)
        top_15_score_2_set = set(top_15_score_2_list)

        # Top 15 by score, excluding those already in top_15_score_2
        df_sorted_score = df_filtered.sort_values(by="score", ascending=False)
        top_15_score = []
        skipped_tickers = []
        for _, row in df_sorted_score.iterrows():
            ticker = f"{row['Exchange']}:{row['Symbol']}"
            if ticker in top_15_score_2_set:
                skipped_tickers.append(ticker)
            elif len(top_15_score) < 15:
                top_15_score.append(ticker)

        if skipped_tickers:
            logger.info(
                f"Tickers skipped from top 15 by score (already in top 15 by score_2): {', '.join(skipped_tickers)}"
            )

        # Combine both lists to get top 30
        top_30_list = top_15_score_2_list + top_15_score

        df_all_sorted = df.sort_values(by="score", ascending=False)
        top_50_global = df_all_sorted.head(50)
        top_50_global_list = _format_ticker_list(top_50_global)

        unified_ticker_list = _create_unified_list(top_30_list, top_50_global_list)

        df_score_filtered = df[df["score"] >= 2.7].copy()
        df_score_filtered = df_score_filtered.sort_values(by="score", ascending=False)
        score_27_list = _format_ticker_list(df_score_filtered)

        df_worst_performers = df[df["score"] > 0].copy()
        df_worst_performers = df_worst_performers.sort_values(
            by="score", ascending=True
        )
        worst_100 = df_worst_performers.head(100)
        worst_100_list = _format_ticker_list(worst_100)

        output_dir = "data"
        os.makedirs(output_dir, exist_ok=True)

        today_date = datetime.now().strftime("%Y-%m-%d")
        unified_path = os.path.join(output_dir, f"top_monde_{today_date}.txt")
        score_27_path = os.path.join(output_dir, f"top_monde_2_7_{today_date}.txt")
        worst_100_path = os.path.join(
            output_dir, f"top_monde_worst_100_{today_date}.txt"
        )

        _save_ticker_list(unified_ticker_list, unified_path)
        _save_ticker_list(score_27_list, score_27_path)
        _save_ticker_list(worst_100_list, worst_100_path)

        logger.info(f"Unified list: {unified_path}")
        logger.info(f"Score >= 2.7 list: {score_27_path}")
        logger.info(f"Worst 100 performers list: {worst_100_path}")

        top_30_set = set(top_30_list)
        unique_global_tickers = [t for t in top_50_global_list if t not in top_30_set]

        logger.info("Summary:")
        logger.info(f"  Top 30 Big: {len(top_30_list)} tickers")
        logger.info(f"  Top 50 Global (unique): {len(unique_global_tickers)} tickers")
        logger.info(f"  Total unique (unified): {len(unified_ticker_list) - 2} tickers")
        logger.info(f"  Score >= 2.7: {len(score_27_list)} tickers")
        logger.info(f"  Worst 100 performers: {len(worst_100_list)} tickers")

        return True

    except Exception as e:
        logger.error(f"Error creating ticker lists: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    logger.info("Starting ticker_list creation...")
    success = create_ticker_lists()
    if success:
        logger.info("Ticker lists generation completed successfully")
    else:
        logger.error("Ticker lists generation failed")
