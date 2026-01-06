"""
Shared pytest fixtures for trigger_bot tests.
"""

import pandas as pd
import pytest


@pytest.fixture
def sample_top_monde_df() -> pd.DataFrame:
    """Create a minimal DataFrame mimicking TOP MONDE input data."""
    return pd.DataFrame(
        {
            "Symbol": ["AAPL", "MSFT", "GOOGL", "TSLA", "INVALID"],
            "Exchange": ["NASDAQ", "NASDAQ", "NASDAQ", "NASDAQ", "NYSE"],
            "Price": [150.0, 300.0, 140.0, 250.0, 50.0],
            "Market capitalization": [2.5e12, 2.8e12, 1.8e12, 8e11, 5e9],
            "Performance % 1 year": [25.0, 30.0, 15.0, -10.0, 0.0],
            "Performance % 6 months": [12.0, 18.0, 8.0, -5.0, 0.0],
            "Performance % 3 months": [5.0, 10.0, 3.0, -2.0, 0.0],
            "Performance % 1 month": [2.0, 5.0, 1.0, -1.0, 0.0],
            "Simple Moving Average (21) 1 day": [148.0, 295.0, 138.0, 245.0, 48.0],
            "Simple Moving Average (200) 1 day": [140.0, 280.0, 130.0, 260.0, 52.0],
        }
    )
