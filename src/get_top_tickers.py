#!/usr/bin/env python3
"""
Script to retrieve and analyze TOP MONDE tickers historical data.
"""

import glob
import logging
import os
import time
import warnings
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

import pandas as pd
import yfinance as yf

warnings.filterwarnings("ignore")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def get_top_monde_tickers() -> List[str]:
    """Get tickers from the most recent TOP MONDE file."""
    history_path = os.path.join("data", "history")
    top_monde_files = glob.glob(os.path.join(history_path, "TOP MONDE_*.csv"))

    if not top_monde_files:
        raise FileNotFoundError("No TOP MONDE files found in data/history/")

    latest_file = None
    latest_date = None

    for file_path in top_monde_files:
        filename = os.path.basename(file_path)
        try:
            date_str = filename.replace("TOP MONDE_", "").replace(".csv", "")
            file_date = datetime.strptime(date_str, "%Y-%m-%d")

            if latest_date is None or file_date > latest_date:
                latest_date = file_date
                latest_file = file_path
        except ValueError:
            continue

    if latest_file is None:
        raise ValueError("No TOP MONDE file with valid date found")

    logger.info(f"Using most recent TOP MONDE file: {os.path.basename(latest_file)}")

    try:
        df = pd.read_csv(latest_file, sep=",")
        symbol_column = df.columns[0]
        if symbol_column != "Symbole":
            logger.warning(f"First column is '{symbol_column}', not 'Symbole'")

        tickers = df[symbol_column].dropna().tolist()
        logger.info(f"Loaded {len(tickers)} tickers from TOP MONDE")
        return tickers
    except Exception as e:
        logger.error(f"Error reading TOP MONDE file: {e}", exc_info=True)
        raise


def _extract_ticker_data(
    data: pd.DataFrame, ticker: str, batch_size: int
) -> Optional[pd.DataFrame]:
    """Extract ticker-specific data from batch download."""
    if batch_size == 1:
        return data.copy() if isinstance(data, pd.DataFrame) else None
    if (ticker,) in data.columns or ticker in data.columns.get_level_values(0):
        result = data[ticker].copy()
        return result if isinstance(result, pd.DataFrame) else None
    return None


def _get_company_info(ticker: str) -> Tuple[str, Optional[float]]:
    """Get company name and calculate shares from market cap."""
    try:
        info = yf.Ticker(ticker).info
        name = info.get("longName", ticker)
        mcap, price = info.get("marketCap"), info.get("regularMarketPrice")
        shares = (mcap / price) if (mcap and price and price > 0) else None
        return name, shares
    except Exception:
        return ticker, None


def _process_ticker_monthly_data(
    ticker: str, ticker_data: pd.DataFrame, name: str, shares: Optional[float]
) -> List[dict]:
    """Process monthly data for a single ticker."""
    ticker_data = ticker_data.reset_index()
    return [
        {
            "ticker": ticker,
            "date": row.get("Date"),
            "open": row.get("Open"),
            "close": row.get("Close"),
            "name": name,
            "market_cap_est": shares * row.get("Open")
            if shares and row.get("Open") and shares > 0
            else None,
        }
        for _, row in ticker_data.iterrows()
        if all(row.get(k) is not None for k in ["Open", "Close", "Date"])
    ]


def get_monthly_data(tickers: List[str], months: int = 13) -> pd.DataFrame:
    """Download monthly historical data for tickers over specified months."""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=months * 30)
    logger.info(
        f"Downloading data for {len(tickers)} tickers from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
    )

    all_data = []
    batch_size = 100

    for i in range(0, len(tickers), batch_size):
        batch = tickers[i : i + batch_size]
        batch_num = i // batch_size + 1
        total_batches = (len(tickers) - 1) // batch_size + 1
        logger.info(
            f"Processing batch {batch_num}/{total_batches} ({len(batch)} tickers)"
        )

        try:
            data = yf.download(
                batch,
                start=start_date.strftime("%Y-%m-%d"),
                end=end_date.strftime("%Y-%m-%d"),
                interval="1mo",
                group_by="ticker",
                auto_adjust=False,
                progress=False,
            )

            for ticker in batch:
                ticker_data = _extract_ticker_data(data, ticker, len(batch))
                if ticker_data is None:
                    continue
                name, shares = _get_company_info(ticker)
                all_data.extend(
                    _process_ticker_monthly_data(ticker, ticker_data, name, shares)
                )
                time.sleep(0.1)

        except Exception as e:
            logger.error(f"Error processing batch {batch}: {e}", exc_info=True)
            continue

    return pd.DataFrame(all_data)


def _calculate_period_performance(
    ticker_data: pd.DataFrame, period: int, earliest_date: pd.Timestamp
) -> Optional[dict]:
    """Calculate performance metrics for a specific 12-month period."""
    latest_date = ticker_data["date"].max()
    if pd.isna(latest_date):
        return None

    period_end = earliest_date + pd.DateOffset(months=11 + period)
    if period_end > latest_date:
        return None

    dates = {
        "12m": earliest_date + pd.DateOffset(months=period),
        "6m": earliest_date + pd.DateOffset(months=6 + period),
        "3m": earliest_date + pd.DateOffset(months=9 + period),
    }

    def get_price(target_date: pd.Timestamp, use_max: bool = False) -> Optional[float]:
        if pd.isna(target_date):
            return None
        filtered = (
            ticker_data[ticker_data["date"] >= target_date]
            if not use_max
            else ticker_data[ticker_data["date"] <= target_date]
        )
        if len(filtered) == 0:
            return None
        price = filtered["close"].iloc[-1 if use_max else 0]
        return float(price) if pd.notna(price) and price > 0 else None

    prices = {k: get_price(v) for k, v in dates.items()}
    prices["current"] = get_price(period_end, use_max=True)

    if not all(prices.values()):
        return None

    def calc_perf(current: float, ref: float) -> float:
        return ((current - ref) / ref * 100) if ref > 0 else 0.0

    perf_12m = calc_perf(prices["current"], prices["12m"])
    perf_6m = calc_perf(prices["current"], prices["6m"])
    perf_3m = calc_perf(prices["current"], prices["3m"])

    return {
        "period_start": dates["12m"],
        "period_end": period_end,
        "perf_12m_pct": perf_12m,
        "perf_6m_pct": perf_6m,
        "perf_3m_pct": perf_3m,
        "score_total": perf_12m + perf_6m + perf_3m,
    }


def calculate_performance_metrics(monthly_data: pd.DataFrame) -> pd.DataFrame:
    """Calculate performance metrics for all available 12-month periods."""
    logger.info("Calculating performance metrics...")
    performance_data = []

    for ticker in monthly_data["ticker"].unique():
        ticker_data = (
            monthly_data[monthly_data["ticker"] == ticker].copy().sort_values("date")
        )
        if len(ticker_data) < 12:
            continue

        ticker_data = ticker_data.dropna(subset=["date"])
        if len(ticker_data) < 12:
            continue

        earliest_date = pd.Timestamp(ticker_data["date"].min())
        if pd.isna(earliest_date):
            continue

        company_name = ticker_data["name"].iloc[0]
        available_periods = len(ticker_data) - 11

        for period in range(available_periods):
            period_perf = _calculate_period_performance(
                ticker_data, period, earliest_date
            )
            if period_perf:
                performance_data.append(
                    {"ticker": ticker, "name": company_name, **period_perf}
                )

    return pd.DataFrame(performance_data)


def check_existing_data() -> Tuple[Optional[pd.DataFrame], bool]:
    """Check if existing data file is up to date based on modification date."""
    csv_path = os.path.join("data", "history", "top_monde_monthly_data.csv")
    if not os.path.exists(csv_path):
        return None, False

    try:
        file_mod_date = datetime.fromtimestamp(os.path.getmtime(csv_path)).date()
        is_up_to_date = file_mod_date == datetime.now().date()
        logger.info(
            f"CSV found: {csv_path}, Last mod: {file_mod_date}, Up to date: {is_up_to_date}"
        )

        existing_data = pd.read_csv(csv_path)
        existing_data["date"] = pd.to_datetime(existing_data["date"])
        return existing_data, is_up_to_date
    except Exception as e:
        logger.error(f"Error reading existing CSV: {e}", exc_info=True)
        return None, False


def _save_top_performers(performance_df: pd.DataFrame, output_dir: str) -> None:
    """Save top 10 performers for each period to CSV."""
    all_periods = (
        performance_df[["period_start", "period_end"]]
        .drop_duplicates()
        .sort_values("period_end", ascending=False)
    )

    all_top_performers = []
    for _, period_row in all_periods.iterrows():
        period_data = performance_df[
            (performance_df["period_start"] == period_row["period_start"])
            & (performance_df["period_end"] == period_row["period_end"])
        ]
        top_10 = period_data.nlargest(10, "score_total", keep="first")[
            [
                "ticker",
                "name",
                "perf_12m_pct",
                "perf_6m_pct",
                "perf_3m_pct",
                "score_total",
            ]
        ].copy()
        top_10["period_start"] = period_row["period_start"]
        top_10["period_end"] = period_row["period_end"]
        top_10["period_rank"] = range(1, len(top_10) + 1)
        all_top_performers.append(
            top_10[
                [
                    "period_start",
                    "period_end",
                    "period_rank",
                    "ticker",
                    "name",
                    "perf_12m_pct",
                    "perf_6m_pct",
                    "perf_3m_pct",
                    "score_total",
                ]
            ]
        )

    if all_top_performers:
        combined = pd.concat(all_top_performers, ignore_index=True)
        output_path = os.path.join(output_dir, "top_monde_performers_all_periods.csv")
        combined.to_csv(output_path, index=False)
        logger.info(
            f"Saved: Top 10 for all periods ({len(combined)} entries, {len(all_periods)} periods) to {output_path}"
        )


def _format_date(date_val) -> str:
    """Format date for logging."""
    return (
        pd.Timestamp(date_val).strftime("%Y-%m")
        if hasattr(date_val, "strftime")
        else str(date_val)
    )


def _log_summary(monthly_data: pd.DataFrame, performance_df: pd.DataFrame) -> None:
    """Log summary statistics."""
    logger.info(f"Retrieved data: {len(monthly_data)} rows")
    logger.info(f"Period: {monthly_data['date'].min()} to {monthly_data['date'].max()}")
    logger.info(f"Tickers with data: {monthly_data['ticker'].nunique()}")
    logger.info(f"Performance metrics: {len(performance_df)} entries (all periods)")

    periods = (
        performance_df[["period_start", "period_end"]]
        .drop_duplicates()
        .sort_values("period_start")
    )
    logger.info("12-month periods analyzed:")
    for i, (_, row) in enumerate(periods.iterrows(), 1):
        logger.info(
            f"  Period {i}: {_format_date(row['period_start'])} to {_format_date(row['period_end'])}"
        )

    latest_period = performance_df["period_end"].max()
    latest_perf = performance_df[performance_df["period_end"] == latest_period]
    logger.info(f"Top 10 TOP MONDE for latest period ({_format_date(latest_period)}):")
    logger.info(
        f"\n{latest_perf.nlargest(10, 'score_total', keep='first')[['ticker', 'name', 'perf_12m_pct', 'perf_6m_pct', 'perf_3m_pct', 'score_total']].round(2)}"
    )

    logger.info("Top 10 TOP MONDE across all periods:")
    logger.info(
        f"\n{performance_df.nlargest(10, 'score_total', keep='first')[['ticker', 'name', 'period_start', 'period_end', 'score_total']].round(2)}"
    )


def main() -> None:
    """Main execution function."""
    existing_data, is_up_to_date = check_existing_data()

    if existing_data is not None and is_up_to_date:
        logger.info("Using existing CSV data (already up to date)")
        monthly_data = existing_data
    else:
        logger.info("Downloading new data...")
        monthly_data = get_monthly_data(get_top_monde_tickers(), months=13)
        output_dir = os.path.join("data", "history")
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, "top_monde_monthly_data.csv")
        monthly_data.to_csv(output_path, index=False)
        logger.info(f"Saved: {len(monthly_data)} rows to {output_path}")

    performance_df = calculate_performance_metrics(monthly_data)
    _save_top_performers(performance_df, os.path.join("data", "history"))
    _log_summary(monthly_data, performance_df)


if __name__ == "__main__":
    logger.info("Starting TOP MONDE tickers history retrieval...")
    main()
    logger.info("TOP MONDE analysis completed")
