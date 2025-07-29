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
from arcticdb.exceptions import ArcticNativeException

from dagster_pipelines.utils.datetime_utils import ensure_timezone
from dagster_pipelines.config.constants import EASTERN_TZ, ISHARES_ETF_URLS

# Disable too many locals due to the function being complex
# pylint: disable=too-many-locals
def get_ishares_etf_tickers(
    etf_ticker: str, partition_date: str, arctic_library: object, logger: object, timeout: int = 10
) -> list[str]:
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
        logger.info("Found recent data in ArcticDB for %s", etf_ticker)
        
        last_as_of_dt = latest_record.data.index[-1]
        last_as_of_dt = ensure_timezone(last_as_of_dt, EASTERN_TZ)

        today = datetime.now(EASTERN_TZ)
        if (today - last_as_of_dt) >= timedelta(days=60):
            logger.warning(
                "Data for %s is stale (last_as_of_date: %s). Redownloading...",
                etf_ticker,
                last_as_of_dt,
            )
        else:
            valid_rows = latest_record.data[latest_record.data.index <= partition_date]
            if len(valid_rows) == 0:
                logger.warning("Portfolio date is before first as-of date for universe. Most recent universe will be used.")
                holdings_row = latest_record.data.iloc[-1]
            else:
                holdings_row = valid_rows.iloc[-1]
                logger.info("Using universe as of %s", holdings_row.name)
            return holdings_row[holdings_row.notna()].index.tolist()

    except ArcticNativeException:
        logger.warning(
            "%s_holdings not found in arctic_library. Downloading...",
            etf_ticker,
        )

    logger.info("Downloading %s holdings from iShares...", etf_ticker)
    response = requests.get(download_url, timeout=timeout)
    response.raise_for_status()

    # Parse the CSV into a dataframe of holdings and metadata for the ETF
    holdings_df = pd.read_csv(
        BytesIO(response.content), index_col=0, header=8, skipfooter=2, engine="python"
    )
    holdings_df = holdings_df.dropna(how="all")  # drop any all na rows
    holdings_metadata = pd.read_csv(BytesIO(response.content), index_col=0, nrows=7)
    # Get the as-of date for the ETF constituents
    full_etf_name = holdings_metadata.columns[0]
    as_of_date = holdings_metadata.loc["Fund Holdings as of", full_etf_name]
    as_of_date = datetime.strptime(as_of_date, "%b %d, %Y")
    as_of_date = ensure_timezone(as_of_date, EASTERN_TZ)
    # Filter for valid equity tickers
    valid_equity_mask = (
        (holdings_df["Asset Class"] == "Equity")
        & holdings_df.index.notna()
        & (holdings_df.index != "-")
    )
    valid_equity_df = holdings_df[valid_equity_mask]

    # Log non-equity and undefined holdings
    non_stock_holdings = holdings_df[holdings_df["Asset Class"] != "Equity"]
    undefined_tickers = holdings_df[
        holdings_df.index.isna() | (holdings_df.index == "-")
    ]
    logger.info(
        "Found %s non-stock holdings and %s undefined tickers",
        len(non_stock_holdings),
        len(undefined_tickers),
    )

    # Check for duplicate tickers using pandas
    ticker_series = valid_equity_df.index
    duplicate_mask = ticker_series.duplicated()
    if duplicate_mask.any():
        duplicate_tickers = ticker_series[duplicate_mask].tolist()
        logger.warning(
            "Found %s duplicate tickers: %s",
            len(duplicate_tickers),
            duplicate_tickers,
        )

        valid_equity_df = valid_equity_df.loc[
            ~valid_equity_df.index.duplicated(keep="first")
        ]

    cleaned_tickers = valid_equity_df.index.tolist()
    output_df = valid_equity_df[['Weight (%)']].T
    #output_df.index = [as_of_date.date()]
    output_df.index = [as_of_date]

    logger.info(
        "Found %s unique equity tickers from %s total holdings.",
        len(cleaned_tickers),
        len(holdings_df),
    )

    # Store in ArcticDB
    symbol_name = f"{etf_ticker}_holdings"
    current_time = datetime.now(EASTERN_TZ)

    new_metadata = {"source": "iShares",
                    "etf_ticker": etf_ticker,
                    "last_updated": current_time.isoformat()}

    if not arctic_library.has_symbol(symbol_name):
        arctic_library.write(symbol=symbol_name,
                            data=output_df,
                            metadata=new_metadata,
                            prune_previous_versions=True)
    else:
        arctic_library.append(symbol=symbol_name,
                                data=output_df,
                                metadata=new_metadata,
                                prune_previous_versions=True)

    logger.info("Stored %s holdings in ArcticDB as '%s'", etf_ticker, symbol_name)

    latest_record = arctic_library.read(f"{etf_ticker}_holdings")

    valid_rows = latest_record.data[latest_record.data.index <= partition_date]
    if len(valid_rows) == 0:
        logger.warning("Portfolio date is before first as-of date for universe. Most recent universe will be used.")
        holdings_row = latest_record.data.iloc[-1]
    else:
        holdings_row = valid_rows.iloc[-1]
        logger.info("Using universe as of %s", holdings_row.name)
    return holdings_row[holdings_row.notna()].index.tolist()
