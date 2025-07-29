# Dagster Sentiment Change Portfolio

This project is forked from the vBase Dagster Sample Portfolio project and implements a portfolio based on StockTwits sentiment data. Long positions are emitted for stocks in the current top decile of 1-day lagged sentiment change, while short positions are emitted for stocks in the bottom decile of 1-day lagged sentiment change. Positions are equal-weighted so that long positions sum to 1 and short positions sum to -1.

## Quickstart Guide

1. Clone the repository:
```bash
git clone <repository-url>
cd dagster-sentiment-change-portfolio
```

2. Create and activate a virtual environment (recommended):
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up environment variables:
Create a `.env` file in the project root with the following variables:
```bash
# StockTwits Configuration
STOCKTWITS_USERNAME=your_username
STOCKTWITS_PASSWORD=your_password

# vBase Configuration
VBASE_API_KEY=your_api_key_here
VBASE_API_URL=your_api_url_here
VBASE_COMMITMENT_SERVICE_PRIVATE_KEY=your_private_key_here

# AWS Configuration (for S3 storage)
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
S3_BUCKET=your_bucket_name
```

For Dagster Cloud deployment, these variables should be set as secrets in your Dagster Cloud organization settings.

5. Run the Dagster development server:
```bash
dagster dev
```

6. Access the Dagster UI at `http://localhost:3000`

## Project Structure

```
dagster-sentiment-change-portfolio/
├── dagster_pipelines/           # Main Dagster pipeline code
│   ├── assets/                  # Dagster assets
│   │   ├── sentiment_change_portfolio_producer.py    # Core portfolio logic
│   │   ├── sentiment_change_portfolio_asset.py       # Portfolio asset definition
│   │   ├── sentiment_dataset_asset.py               # Sentiment data initializer
│   │   ├── sentiment_change_feature.py              # Sentiment feature creation
│   │   ├── dataset_updater.py                       # Sentiment update
│   │   └── etf_holdings_asset.py                    # ETF holdings creator
│   ├── config/                  # Configuration files
│   │   └── constants.py         # Project constants and settings
│   ├── utils/                   # Utility functions
│   ├── resources.py             # Dagster resources (ArcticDB)
│   ├── definitions.py           # Dagster definitions
│   └── workspace.yaml           # Workspace configuration
├── requirements.txt             # Python dependencies
├── requirements-dev.txt         # Development dependencies
├── pyproject.toml              # Project metadata
└── dagster_cloud.yaml          # Dagster Cloud configuration
```

## Key Components

### Portfolio Producer (`sentiment_change_portfolio_producer.py`)
The core logic for generating portfolio positions based on sentiment data:
- Loads sentiment change features from ArcticDB
- Ranks stocks by 1-day lagged sentiment change
- Identifies top and bottom deciles for long/short positions
- Creates equal-weighted market-neutral portfolios

### Sentiment Dataset Asset (`sentiment_dataset_asset.py`)
Manages StockTwits sentiment data:
- Downloads sentiment data for ETF holdings
- Stores data in ArcticDB for efficient access

### Sentiment Dataset Updater (`dataset_updater.py`)
Updates exisiting StockTwits sentiment data in the DB:
- Adds missing tickers to the DB if universe changes
- Handles daily updates for tickers in the DB

### ETF Holdings Asset (`etf_holdings_asset.py`)
Downloads ETF holdings from iShares and stores in DB
- Updates universe if data is stale (>60 days old)
- Checks if there is an existing universe for a given portfolio date, otherwise returns the most recent universe.

### Portfolio Asset (`sentiment_change_portfolio_asset.py`)
Dagster asset that:
- Orchestrates portfolio production
- Commits portfolio data to vBase



## Development

### Code Quality
- Pylint is used for code quality checks (minimum score: 8.0)
- GitHub Actions automatically runs Pylint on all pushes and pull requests
- Pre-commit hooks are configured to run code quality checks before commits

## Configuration

Key configuration options in `dagster_pipelines/config/constants.py`:
- `PORTFOLIO_NAME`: Name of the portfolio set in vBase
- `VBASE_FORWARDER_URL`: URL to forward vBase stamps