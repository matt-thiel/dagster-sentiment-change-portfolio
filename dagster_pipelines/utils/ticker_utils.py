"""
Utilities for fetching and managing ETF ticker data from iShares.

This module provides functionality to download ETF holdings from iShares,
parse the data, and store it in ArcticDB for efficient retrieval. It includes
data validation, deduplication, and caching mechanisms to minimize API calls.
"""

from datetime import datetime, timedelta
from io import BytesIO, StringIO
import pandas as pd
import numpy as np
import requests

from dagster_pipelines.utils.database_utils import arctic_db_write_or_append
from dagster_pipelines.utils.datetime_utils import ensure_timezone
from dagster_pipelines.config.constants import EASTERN_TZ, ETF_HOLDINGS_URLS


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
    download_url = ETF_HOLDINGS_URLS.get(etf_ticker)

    if download_url is None:
        raise ValueError(f"No download URL found for {etf_ticker}")

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


def _download_spy_etf_holdings(
    logger: object,
    timeout: int = 30,
) -> pd.DataFrame:
    """
    Downloads ETF holdings from iShares and returns a DataFrame of valid tickers by ETF weight.
    """
    download_url = ETF_HOLDINGS_URLS.get("SPY")
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }
    response = requests.get(download_url, timeout=timeout, headers=headers)
    if response.status_code != 200:
        logger.error(
            "Failed to fetch S&P 500 constituents: %s %s",
            response.status_code,
            response.reason,
        )
        raise RuntimeError(
            f"Failed to fetch S&P 500 consituents: {response.status_code} {response.reason}"
        )

    # Parse the HTML content using pandas.
    tables = pd.read_html(StringIO(response.text))

    # The table with S&P 500 companies is usually the first table.
    sp_500_table = tables[0]
    symbols = sp_500_table["Symbol"].tolist()
    as_of_date = datetime.now(EASTERN_TZ)
    placeholder_weights = np.full((1, len(symbols)), 1 / len(symbols))
    output_df = pd.DataFrame(placeholder_weights, columns=symbols, index=[as_of_date])
    output_df.columns.name = "Ticker"
    return output_df


def get_most_recent_etf_holdings(
    arctic_library: object,
    etf_ticker: str,
) -> list[str]:
    """
    Gets the most recent ETF holdings from ArcticDB.

    Args:
        arctic_library (object): ArcticDB library instance for data storage and retrieval.
        etf_ticker (str): The ETF ticker symbol (e.g., 'IWM' for iShares Russell 2000 ETF).

    Returns:
        list[str]: List of valid equity ticker symbols from the ETF holdings.
    """
    symbol_name = f"{etf_ticker}_holdings"
    # Ensure data for the symbol exists.
    if not arctic_library.has_symbol(symbol_name):
        raise ValueError(f"No holdings found for {etf_ticker}")

    # Get the most recent universe.
    latest_universe = arctic_library.tail(symbol_name, n=1, columns=None).data
    holdings_row = latest_universe.iloc[-1]

    return holdings_row[holdings_row.notna()].index.tolist()


def initialize_ishares_etf_holdings(
    etf_ticker: str,
    arctic_library: object,
    logger: object,
    timeout: int = 10,
) -> None:
    """
    Initializes the iShares ETF holdings library in ArcticDB.

    Args:
        etf_ticker (str): The ETF ticker symbol (e.g., 'IWM' for iShares Russell 2000 ETF).
        arctic_library (object): ArcticDB library instance for data storage and retrieval.
        logger (object): Logger object for logging messages and warnings.
        timeout (int, optional): Request timeout in seconds. Defaults to 10.
    """
    symbol_name = f"{etf_ticker}_holdings"
    current_time = datetime.now(EASTERN_TZ)

    if arctic_library.has_symbol(symbol_name):
        logger.info("iShares ETF holdings library already exists for %s", etf_ticker)
    else:
        logger.info("Downloading %s holdings from iShares...", etf_ticker)
        if etf_ticker == "SPY":
            latest_record = _download_spy_etf_holdings(logger, timeout)
        else:
            latest_record = _download_ishares_etf_holdings(etf_ticker, logger, timeout)

        new_metadata = {
            "source": "iShares",
            "etf_ticker": etf_ticker,
            "last_updated": current_time.isoformat(),
        }

        # Store result in ArcticDB
        arctic_db_write_or_append(
            symbol=symbol_name,
            arctic_library=arctic_library,
            data=latest_record,
            metadata=new_metadata,
            prune_previous_versions=True,
        )

        logger.info("Stored %s holdings in ArcticDB as '%s'", etf_ticker, symbol_name)


def _check_if_stale_holdings(
    etf_ticker: str,
    arctic_library: object,
    logger: object,
    timeout: int = 10,
) -> None:
    """
    Checks if the ETF holdings are stale and downloads new data if needed.

    Args:
        etf_ticker (str): The ETF ticker symbol (e.g., 'IWM' for iShares Russell 2000 ETF).
        arctic_library (object): ArcticDB library instance for data storage and retrieval.
        logger (object): Logger object for logging messages and warnings.
        timeout (int, optional): Request timeout in seconds. Defaults to 10.

    Raises:
        ValueError: If no download URL is found for the specified ETF ticker.
        requests.HTTPError: If the HTTP request to iShares fails.
        Exception: If there are issues reading from or writing to ArcticDB.
    """
    symbol_name = f"{etf_ticker}_holdings"
    current_time = datetime.now(EASTERN_TZ)
    download_data = False

    if arctic_library.has_symbol(symbol_name):
        # Use tail to get last as of date for update checking
        latest_record = arctic_library.tail(symbol_name, n=1, columns=[])
        last_as_of_dt = latest_record.data.index[-1]
        last_as_of_dt = ensure_timezone(last_as_of_dt, EASTERN_TZ)

        # Update universe every ~2 months in case of constituent changes
        if (current_time - last_as_of_dt) >= timedelta(days=60):
            logger.warning(
                "Data for %s is stale (last_as_of_date: %s). Updating...",
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
        if etf_ticker == "SPY":
            output_df = _download_spy_etf_holdings(logger, timeout)
        else:
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
    else:
        logger.info("No update needed for %s", etf_ticker)


# Disable too many locals due to the function being complex
# pylint: disable=too-many-locals
def get_ishares_etf_tickers(
    etf_ticker: str,
    partition_date: str | None,
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
        partition_date (str | None): The date of the partition to fetch holdings for.
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

    # If no partition date, use most recent universe. Skip stale check.
    if partition_date is None:
        return get_most_recent_etf_holdings(arctic_library, etf_ticker)

    # Ensure partition date is eastern timezone
    partition_date = datetime.strptime(partition_date, "%Y-%m-%d")
    partition_date = ensure_timezone(partition_date, EASTERN_TZ)

    # Check if holdings data is stale
    _check_if_stale_holdings(etf_ticker, arctic_library, logger, timeout)

    # Query for data up to and including the partition date
    date_range = (None, partition_date)
    full_record = arctic_library.read(f"{etf_ticker}_holdings", date_range=date_range)

    # Conservative filter to only use partition data up to partition date
    valid_rows = full_record.data[full_record.data.index < partition_date]
    if valid_rows.empty:
        if not full_record.data.empty:
            # Try to use portfolio date if no as-of date before portfolio date
            logger.warning(
                "No as-of date before portfolio date, using holdings from portfolio date."
            )
            holdings_row = full_record.data.iloc[-1]
        else:
            # Fallback to most recent universe if nothing before or including portfolio date.
            logger.warning(
                "No as-of date before or including portfolio date. Using most recent universe."
            )
            return get_most_recent_etf_holdings(arctic_library, etf_ticker)
    else:
        holdings_row = valid_rows.iloc[-1]
        logger.info("Using universe as of %s", holdings_row.name)

    return holdings_row[holdings_row.notna()].index.tolist()
