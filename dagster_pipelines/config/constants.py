"""
Constants for the project.
"""

import pytz
from dagster import DailyPartitionsDefinition
from arcticdb import LibraryOptions

EASTERN_TZ = pytz.timezone("America/New_York")

ETF_HOLDINGS_URLS = {
    # Russell 2000
    "IWM": (
        "https://www.ishares.com/us/products/239710/ishares-russell-2000-etf/"
        "1467271812596.ajax?fileType=csv&fileName=IWM&dataType=fund"
    ),
    # Russell 3000
    "IWV": (
        "https://www.ishares.com/us/products/239714/ishares-russell-3000-etf/"
        "1467271812596.ajax?fileType=csv&fileName=IWV_holdings&dataType=fund"
    ),
    # Russel 1000
    "IWB": (
        "https://www.ishares.com/us/products/239707/ishares-russell-1000-etf/"
        "1467271812596.ajax?fileType=csv&fileName=IWB_holdings&dataType=fund"
    ),
    # S&P 500
    "SPY": (
        "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    ),
}

# The vBase forwarder URL for making stamps.
VBASE_FORWARDER_URL = "https://dev.api.vbase.com/forwarder-test/"

# The name of the portfolio set (collection).
# This is the vBase set (collection) that receive the object commitments (stamps)
# for the individual portfolios.
PORTFOLIO_NAME = "SentimentChangePortfolio"

# Define a daily partition for portfolio rebalancing.
# The portfolio rebalances daily starting from 2025-01-01.
PORTFOLIO_PARTITIONS_DEF = DailyPartitionsDefinition(start_date="2025-01-01")

# The ticker of the iShares ETF to use for the portfolio.
#ETF_TICKER = "IWM"
ETF_TICKER = "SPY"
DEBUG_ETF_TICKERS = ["IWM", "IWV", "IWB", "SPY"]

BASE_DATASET_SYMBOLS = ["sentimentNormalized", "messageVolumeNormalized"]
FEATURE_LOOKBACK_WINDOW = 2  # Extra days of data needed to get feature value for today
NULL_CHANGE_WINDOW = (
    20  # Number of days to look back for finding low sentiment change stocks
)

# Default library options for ArcticDB.
DEFAULT_LIBRARY_OPTIONS = LibraryOptions(dynamic_schema=True, dedup=True)

# Output directory for sentiment and portfolio datasets.
# The following folder uses the vbase_env_runner folder layout
# for output datasets.
OUTPUT_DIR = "/work/data/output"
