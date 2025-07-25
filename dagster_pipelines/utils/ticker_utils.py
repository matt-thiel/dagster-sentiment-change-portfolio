"""
Utilities for fetching and managing ETF ticker data from iShares.

This module provides functionality to download ETF holdings from iShares,
parse the data, and store it in ArcticDB for efficient retrieval. It includes
data validation, deduplication, and caching mechanisms to minimize API calls.
"""

from datetime import datetime, timedelta
from io import BytesIO
import pandas as pd
import requests
#from arcticdb.exceptions import ArcticException

from dagster_pipelines.utils.datetime_utils import ensure_timezone
from dagster_pipelines.config.constants import EASTERN_TZ

ISHARES_ETF_URLS = {
    "IWM": "https://www.ishares.com/us/products/239710/ishares-russell-2000-etf/1467271812596.ajax?fileType=csv&fileName=IWM&dataType=fund",
}

def get_ishares_etf_tickers(etf_ticker: str, arctic_library: object, logger: object, timeout: int = 10) -> list[str]:
    """
    Fetches and caches ETF holdings from iShares, with intelligent caching and data validation.

    This function downloads ETF holdings from iShares, filters for valid equity tickers,
    removes duplicates, and stores the data in ArcticDB. It implements a caching strategy
    that only re-downloads data if it's older than 60 days.

    Args:
        etf_ticker (str): The ETF ticker symbol (e.g., 'IWM' for iShares Russell 2000 ETF).
        arctic_library (object): ArcticDB library instance for data storage and retrieval.
        logger (object): Logger object for logging messages and warnings.
        timeout (int, optional): Request timeout in seconds. Defaults to 10.

    Returns:
        list[str]: List of valid equity ticker symbols from the ETF holdings.

    Raises:
        ValueError: If no download URL is found for the specified ETF ticker.
        requests.HTTPError: If the HTTP request to iShares fails.
        Exception: If there are issues reading from or writing to ArcticDB.
    """
    download_url = ISHARES_ETF_URLS.get(etf_ticker)
    if download_url is None:
        raise ValueError(f"No download URL found for {etf_ticker}")
    
    try:
        latest_record = arctic_library.read(f"{etf_ticker}_holdings")
        logger.info(f"Found recent data in ArcticDB for {etf_ticker}")
        as_of_date = latest_record.metadata.get('as_of_date')

        as_of_dt = datetime.strptime(as_of_date, "%Y-%m-%d")
        as_of_dt = ensure_timezone(as_of_dt, EASTERN_TZ)

        today = datetime.now(EASTERN_TZ)
        if (today - as_of_dt) >= timedelta(days=60):
            logger.warning(
                f"Data for {etf_ticker} is stale (as_of_date: {as_of_date}). Redownloading..."
            )
        else:
            return latest_record.data.index.tolist()
        
    #except ArcticException:
    except Exception as e:
        logger.warning(f"{etf_ticker}_holdings not found in arctic_library. Downloading...")


    logger.info(f"Downloading {etf_ticker} holdings from iShares...")
    response = requests.get(download_url, timeout=timeout)
    response.raise_for_status()

    # Parse the CSV into a dataframe of holdings and metadata for the ETF
    holdings_df = pd.read_csv(BytesIO(response.content), index_col=0, header=8, skipfooter=2, engine='python')
    holdings_df = holdings_df.dropna(how='all') # drop any all na rows
    holdings_metadata = pd.read_csv(BytesIO(response.content), index_col=0, nrows=7)
    # Get the as-of date for the ETF constituents
    full_etf_name = holdings_metadata.columns[0]
    as_of_date = holdings_metadata.loc['Fund Holdings as of', full_etf_name]
    as_of_date = datetime.strptime(as_of_date, '%b %d, %Y')
    as_of_date = ensure_timezone(as_of_date, EASTERN_TZ)
    # Filter for valid equity tickers
    valid_equity_mask = (holdings_df["Asset Class"] == "Equity") & holdings_df.index.notna() & (holdings_df.index != "-")
    valid_equity_df = holdings_df[valid_equity_mask]
    
    # Log non-equity and undefined holdings
    non_stock_holdings = holdings_df[holdings_df["Asset Class"] != "Equity"]
    undefined_tickers = holdings_df[holdings_df.index.isna() | (holdings_df.index == "-")]
    logger.info(f"Found {len(non_stock_holdings)} non-stock holdings and {len(undefined_tickers)} undefined tickers")
    
    # Check for duplicate tickers using pandas
    ticker_series = valid_equity_df.index
    duplicate_mask = ticker_series.duplicated()
    if duplicate_mask.any():
        duplicate_tickers = ticker_series[duplicate_mask].tolist()
        logger.warning(f"Found {len(duplicate_tickers)} duplicate tickers: {duplicate_tickers}")
        # Remove duplicates while preserving order
        #cleaned_tickers = ticker_series.drop_duplicates().tolist()
        valid_equity_df = valid_equity_df.loc[~valid_equity_df.index.duplicated(keep='first')]
 
    cleaned_tickers = valid_equity_df.index.tolist()

    logger.info(f"Found {len(cleaned_tickers)} unique equity tickers from {len(holdings_df)} total holdings.")

        # Store in ArcticDB
    symbol_name = f"{etf_ticker}_holdings"
    current_time = datetime.now(EASTERN_TZ)
    arctic_library.write(
        symbol_name, 
        holdings_df, 
        metadata={
            'as_of_date': as_of_date.strftime('%Y-%m-%d'),
            'source': 'iShares',
            'etf_ticker': etf_ticker,
            'download_timestamp': current_time.isoformat(),
        }
    )
    logger.info(f"Stored {etf_ticker} holdings in ArcticDB as '{symbol_name}'")
    
    return cleaned_tickers