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

from dagster_pipelines.utils.database_utils import arctic_db_write_or_append
from dagster_pipelines.utils.datetime_utils import ensure_timezone
from dagster_pipelines.config.constants import EASTERN_TZ, ISHARES_ETF_URLS


def _download_ishares_etf_holdings(
    etf_ticker: str, logger: object, timeout: int = 10
) -> pd.DataFrame:
    """
    Downloads ETF holdings from iShares and returns a DataFrame of valid tickers by ETF weight.

    Args:
        etf_ticker (str): The ETF ticker symbol (e.g., 'IWM' for iShares Russell 2000 ETF).
        logger (object): Logger object for logging messages and warnings.
        timeout (int, optional): Request timeout in seconds. Defaults to 10.

    Returns:
        pd.DataFrame: A DataFrame of valid tickers by ETF weight.
    """
    download_url = ISHARES_ETF_URLS.get(etf_ticker)

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
    valid_equity_df = holdings_df[
        (holdings_df["Asset Class"] == "Equity")
        & holdings_df.index.notna()
        & (holdings_df.index != "-")
    ]
    # Remove duplicate tickers
    valid_equity_df = valid_equity_df.loc[
        ~valid_equity_df.index.duplicated(keep="first")
    ]

    # Log non-equity and undefined holdings
    num_invalid_holdings = len(holdings_df) - len(valid_equity_df)
    logger.info(
        "Found %s non-stock, undefined, or duplicate tickers", num_invalid_holdings
    )

    cleaned_tickers = valid_equity_df.index.tolist()
    output_df = valid_equity_df[["Weight (%)"]].T

    output_df.index = [as_of_date]

    logger.info(
        "Found %s unique equity tickers from %s total holdings.",
        len(cleaned_tickers),
        len(holdings_df),
    )

    return output_df


# Disable too many locals due to the function being complex
# pylint: disable=too-many-locals
def get_ishares_etf_tickers(
    etf_ticker: str,
    partition_date: str,
    arctic_library: object,
    logger: object,
    timeout: int = 10,
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
    symbol_name = f"{etf_ticker}_holdings"
    download_data = False
    current_time = datetime.now(EASTERN_TZ)

    if download_url is None:
        raise ValueError(f"No download URL found for {etf_ticker}")

    if arctic_library.has_symbol(symbol_name):
        # Use tail to get last as of date for update checking
        latest_record = arctic_library.tail(symbol_name, n=1, columns=[])
        last_as_of_dt = latest_record.data.index[-1]
        last_as_of_dt = ensure_timezone(last_as_of_dt, EASTERN_TZ)

        # Update universe every ~2 months in case of constituent changes
        if (current_time - last_as_of_dt) >= timedelta(days=60):
            logger.warning(
                "Data for %s is stale (last_as_of_date: %s). Redownloading...",
                etf_ticker,
                last_as_of_dt,
            )
            download_data = True
    else:
        logger.warning(
            "%s_holdings not found in arctic_library. Downloading...",
            etf_ticker,
        )
        download_data = True

    if download_data:
        logger.info("Downloading %s holdings from iShares...", etf_ticker)
        output_df = _download_ishares_etf_holdings(etf_ticker, logger, timeout)

        new_metadata = {
            "source": "iShares",
            "etf_ticker": etf_ticker,
            "last_updated": current_time.isoformat(),
        }

        # Store result in ArcticDB
        arctic_db_write_or_append(
            symbol=symbol_name,
            arctic_library=arctic_library,
            data=output_df,
            metadata=new_metadata,
            prune_previous_versions=True,
        )

        logger.info("Stored %s holdings in ArcticDB as '%s'", etf_ticker, symbol_name)

    # Need to load the full universe data to query the correct date for the portfolio
    latest_record = arctic_library.read(f"{etf_ticker}_holdings")

    valid_rows = latest_record.data[latest_record.data.index <= partition_date]
    if len(valid_rows) == 0:
        logger.warning(
            "Portfolio date is before first as-of universe date. Most recent universe will be used."
        )
        holdings_row = latest_record.data.iloc[-1]
    else:
        holdings_row = valid_rows.iloc[-1]
        logger.info("Using universe as of %s", holdings_row.name)

    return holdings_row[holdings_row.notna()].index.tolist()
