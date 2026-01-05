# Trigger Bot

Analysis pipeline and ticker list generator for TradingView from TOP MONDE data.

## Overview

This project processes market data (TOP MONDE) to calculate performance scores, enrich data with technical indicators, and generate optimized ticker lists for TradingView.

## Project Structure

```
trigger_bot/
├── src/
│   ├── top_monde_ranking.py      # TOP MONDE data enrichment
│   ├── ticker_list.py             # TradingView list generation
│   ├── cleanup_files.py           # File organization script
│   ├── get_sp500_tickers_history.py  # S&P 500 historical data retrieval
│   └── get_top_tickers.py         # TOP MONDE historical data retrieval
├── data/
│   ├── waiting_room/               # Raw TOP MONDE files (input)
│   ├── ticker_room/                # Enriched files (output from top_monde_ranking.py)
│   ├── ready_to_use/               # Processed files (after ticker_list.py)
│   ├── used_input_files/           # Archived input files
│   └── history/                    # Historical data
└── main.py
```

## Main Scripts

### 1. `top_monde_ranking.py` - TOP MONDE Data Enrichment

**Purpose**: Analyzes and enriches TOP MONDE CSV files with calculated columns for scoring.

**How it works**:

1. **File reading**: Reads all `TOP MONDE*.csv` files from `data/waiting_room/`

2. **Performance columns calculation**:
   - `perf_sum`: Sum of performances over 1 year, 6 months, and 3 months
   - `perf_norm`: Normalization of `perf_sum` (1 + perf_sum/1000)
   - `perfsum_2`: Sum of performances over 1 month, 3 months, and 6 months
   - `perf_norm_2`: Normalization of `perfsum_2`
   
   *Based on: [stock_backtest_engine](https://github.com/Taradim/stock_backtest_engine)*

3. **Technical indicators calculation**:
   - `MRAT`: Moving average ratio (MA21 / MA200)
   - `Diff`: Price / MA200 ratio
   
   *Based on: [SSRN Study](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=3111334)*

4. **Score calculation**:
   - `score`: `perf_norm` + `MRAT`
   - `score_2`: `perf_norm_2` + `MRAT`

5. **Post-processing**:
   - Replace null values with 0
   - Round to 2 decimal places
   - Sort by `score` descending

6. **Save**: Generates `*_enhanced.csv` files in `data/ticker_room/`

**Usage**:
```bash
python src/top_monde_ranking.py
```

**Logs**: Logs are displayed in the console at INFO level.

---

### 2. `ticker_list.py` - TradingView List Generation

**Purpose**: Generates ticker lists in TradingView format from enriched files.

**How it works**:

1. **File selection**: Automatically finds the most recent enriched TOP MONDE file in `data/ticker_room/`

2. **Top 30 Big creation** (companies > 10 billion market cap):
   - **Top 15 by `score_2`**: The 15 best companies according to the score based on short-term performance (1, 3, 6 months)
   - **Top 15 by `score`**: The 15 best companies according to the classic score, excluding those already present in the top 15 `score_2`
   - Duplicate tickers are automatically skipped and logged

3. **Top 50 Global creation**: The 50 best companies across all market caps, sorted by `score`

4. **Unified list**: Combines Top 30 Big + Top 50 Global (without duplicates) with commented sections

5. **Additional lists**:
   - **Score >= 2.7**: All companies with a score >= 2.7
   - **Worst 100 performers**: The 100 companies with the lowest scores (score > 0)

6. **Save**: Generates 3 `.txt` files in `data/`:
   - `top_monde_YYYY-MM-DD.txt`: Unified list
   - `top_monde_2_7_YYYY-MM-DD.txt`: Score >= 2.7
   - `top_monde_worst_100_YYYY-MM-DD.txt`: Worst 100

**Output format**:
```
// Top 30 Big
NASDAQ:AAPL
NYSE:MSFT
...
// Top 50 Global
NYSE:JPM
...
```

**Usage**:
```bash
python src/ticker_list.py
```

**Logs**: Displays a detailed summary including skipped tickers during top 15 by score creation.

---

### 3. `cleanup_files.py` - File Organization

**Purpose**: Organizes files between data directories to maintain a clean workflow.

**How it works**:

1. **waiting_room → used_input_files**: Moves all CSV files from `waiting_room` to `used_input_files`
2. **ticker_room → ready_to_use**: Moves all CSV files from `ticker_room` to `ready_to_use`
3. **ready_to_use (non-enhanced) → used_input_files**: Moves non-enhanced CSV files (without `_enhanced` in filename) from `ready_to_use` to `used_input_files`

**Usage**:
```bash
python src/cleanup_files.py
```

**Logs**: Logs each file move operation at INFO level.

---

## Secondary Scripts (Legacy)

### 4. `get_sp500_tickers_history.py`

Retrieves monthly historical data for S&P 500 tickers over the last 13 months via yfinance. Calculates performance metrics (12m, 6m, 3m) and saves to `data/history/sp500_monthly_data.csv`. Checks if data is already up to date before downloading.

*Abandoned because the data was not completely reliable and importing from TradingView screener (TOP MONDE files) was more appropriate.*

### 5. `get_top_tickers.py`

Similar to `get_sp500_tickers_history.py` but uses tickers from the most recent TOP MONDE file instead of S&P 500. Saves to `data/history/top_monde_monthly_data.csv`.

---

## Recommended Workflow

1. **Place raw TOP MONDE files** in `data/waiting_room/`
2. **Run `top_monde_ranking.py`** to enrich the data (outputs to `data/ticker_room/`)
3. **Run `ticker_list.py`** to generate TradingView lists (reads from `data/ticker_room/`, outputs `.txt` files to `data/`)
4. **Copy `.txt` file contents** into TradingView
5. **Run `cleanup_files.py`** to organize processed files:
   - Moves files from `waiting_room` to `used_input_files`
   - Moves files from `ticker_room` to `ready_to_use`
   - Moves non-enhanced files from `ready_to_use` to `used_input_files`

---

## Dependencies

- `pandas`: Data manipulation
- `numpy`: Numerical computations
- `yfinance`: Market data retrieval (secondary scripts)

---

## Technical Notes

- Logs use Python's standard `logging` module
- Files are processed idempotently (skip if already processed)
- Scores are calculated from historical performance and technical indicators
- TradingView format expects `Exchange:Symbol` (e.g., `NASDAQ:AAPL`)
