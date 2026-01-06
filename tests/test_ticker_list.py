"""
Unit tests for ticker_list module.

Tests cover the core list creation logic.
"""

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from ticker_list import _create_unified_list, _format_ticker_list


class TestTickerListCreation:
    """Tests for ticker list generation."""

    def test_format_ticker_produces_exchange_symbol_format(self) -> None:
        """Verify output format is 'EXCHANGE:SYMBOL'."""
        df = pd.DataFrame(
            {
                "Exchange": ["NASDAQ", "NYSE"],
                "Symbol": ["AAPL", "JPM"],
            }
        )
        result = _format_ticker_list(df)

        assert result == ["NASDAQ:AAPL", "NYSE:JPM"]

    def test_unified_list_removes_duplicates(self) -> None:
        """Verify tickers in top_30 are excluded from top_50 section."""
        top_30 = ["NASDAQ:AAPL", "NYSE:JPM"]
        top_50 = ["NASDAQ:AAPL", "NASDAQ:GOOGL"]  # AAPL is duplicate

        result = _create_unified_list(top_30, top_50)

        # AAPL should appear only once
        assert result.count("NASDAQ:AAPL") == 1
        # GOOGL should be in the list
        assert "NASDAQ:GOOGL" in result
