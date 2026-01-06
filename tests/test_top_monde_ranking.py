"""
Unit tests for top_monde_ranking module.

Tests cover the core calculation functions (score, performance metrics).
"""

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from top_monde_ranking import (
    _calculate_performance_columns,
    _calculate_scores,
    validate_input_dataframe,
)


class TestCalculatePerformanceColumns:
    """Tests for performance column calculations."""

    def test_perf_sum_calculation(self, sample_top_monde_df: pd.DataFrame) -> None:
        """Verify perf_sum = 12m + 6m + 3m performance."""
        result = _calculate_performance_columns(sample_top_monde_df.copy())

        # AAPL: 25 + 12 + 5 = 42
        assert result.loc[0, "perf_sum"] == 42.0
        # MSFT: 30 + 18 + 10 = 58
        assert result.loc[1, "perf_sum"] == 58.0

    def test_perf_norm_formula(self, sample_top_monde_df: pd.DataFrame) -> None:
        """Verify perf_norm = 1 + perf_sum/1000."""
        result = _calculate_performance_columns(sample_top_monde_df.copy())

        # AAPL: 1 + 42/1000 = 1.042
        assert result.loc[0, "perf_norm"] == pytest.approx(1.042, rel=1e-3)

    def test_handles_negative_performance(
        self, sample_top_monde_df: pd.DataFrame
    ) -> None:
        """Verify calculations work with negative values."""
        result = _calculate_performance_columns(sample_top_monde_df.copy())

        # TSLA: -10 + -5 + -2 = -17
        assert result.loc[3, "perf_sum"] == -17.0


class TestCalculateScores:
    """Tests for score calculations."""

    def test_score_formula(self, sample_top_monde_df: pd.DataFrame) -> None:
        """Verify score = perf_norm + MRAT."""
        from top_monde_ranking import _calculate_technical_indicators

        df = _calculate_performance_columns(sample_top_monde_df.copy())
        df = _calculate_technical_indicators(df)
        result = _calculate_scores(df)

        expected = result.loc[0, "perf_norm"] + result.loc[0, "MRAT"]
        assert result.loc[0, "score"] == pytest.approx(expected, rel=1e-3)


class TestValidation:
    """Tests for input validation."""

    def test_returns_empty_when_all_columns_present(
        self, sample_top_monde_df: pd.DataFrame
    ) -> None:
        """Validation passes when all required columns exist."""
        result = validate_input_dataframe(sample_top_monde_df)
        assert result == []

    def test_detects_missing_columns(self) -> None:
        """Validation returns list of missing columns."""
        incomplete_df = pd.DataFrame({"Symbol": ["AAPL"], "Price": [150.0]})
        result = validate_input_dataframe(incomplete_df)

        assert len(result) > 0
        assert "Exchange" in result
