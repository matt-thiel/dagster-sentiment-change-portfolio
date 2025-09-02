"""
Utilities for fetching and processing sentiment data from StockTwits API.
"""

import os
import sys
import time
from datetime import datetime
from json import JSONDecodeError
from requests import HTTPError, Timeout
import cloudscraper
from tqdm import tqdm
import pandas as pd

from dagster_pipelines.config.constants import EASTERN_TZ, OUTPUT_DIR
from dagster_pipelines.utils.datetime_utils import get_market_day_from_date
from dagster_pipelines.utils.database_utils import compare_files_to_timestamp

STOCKTWITS_ENDPOINT = (
    "https://api-gw-prd.stocktwits.com/api-middleware/external/sentiment/v2/"
)


# Disable too many arguments due to arguments necessary for api call
# pylint: disable=too-many-arguments
def get_symbol_chart(
    symbol: str,
    zoom: str,
    username: str,
    password: str,
    logger: object,
    timeout: int = 10,
) -> pd.DataFrame:
    """
    Fetches sentiment chart data for a given stock symbol from StockTwits API.

    This function makes an authenticated request to the StockTwits sentiment API
    to retrieve historical sentiment data for a specific symbol. The data is
    returned as a timezone-aware DataFrame with sentiment metrics.

    Args:
        symbol (str): Stock symbol to fetch sentiment data for (e.g., 'AAPL').
        zoom (str): Time zoom level for the chart data (e.g., '1D', '1W', '1M').
        username (str): StockTwits API username for authentication.
        password (str): StockTwits API password for authentication.
        logger (object): Logger object for logging messages and errors.
        timeout (int, optional): Request timeout in seconds. Defaults to 10.

    Returns:
        pd.DataFrame: DataFrame containing the sentiment chart data with timezone-aware
            datetime index and sentiment metrics as columns.

    Raises:
        HTTPError: If the HTTP request fails (4xx or 5xx status codes).
        JSONDecodeError: If the response cannot be parsed as valid JSON.
        ConnectionError: If there is a network connection error.
        Timeout: If the request times out.
        SystemExit: If any of the above errors occur (function calls sys.exit).
    """
    try:
        url = f"{STOCKTWITS_ENDPOINT}{symbol}/chart"
        scraper = cloudscraper.create_scraper()
        resp = scraper.get(
            url,
            auth=(username, password),
            params={"zoom": zoom},
            timeout=timeout,
        )
        resp.raise_for_status()
        result = resp.json()

    except HTTPError as err:
        logger.error("HTTP error occurred: %s", err)
        sys.exit(f"HTTP error occurred: {err}")
    except JSONDecodeError as err:
        logger.error("JSON Decode Error: %s", err)
        sys.exit(f"JSON Decode Error: {err}")
    except ConnectionError as err:
        logger.error("Connection Error: %s", err)
        sys.exit(f"Connection Error: {err}")
    except Timeout as err:
        logger.error("Timeout Error: %s", err)
        sys.exit(f"Timeout Error: {err}")

    response_df = pd.DataFrame.from_dict(result["data"], orient="index")

    response_df.index = pd.to_datetime(response_df.index, utc=True).tz_convert(
        "America/New_York"
    )

    return response_df


# Disable too many arguments due to arguments necessary for api call
# pylint: disable=too-many-arguments
def get_chart_for_symbols(
    symbols: list[str],
    zoom: str,
    username: str,
    password: str,
    logger: object,
    timeout: int = 10,
) -> pd.DataFrame:
    """
    Fetches sentiment data for multiple tickers with progress tracking and error handling.

    This function iterates through a list of symbols, fetching sentiment data for each
    one using the StockTwits API. It includes progress tracking, error handling for
    individual symbols, and rate limiting to avoid API throttling. The results are
    combined into a multi-indexed DataFrame.

    Args:
        symbols (list[str]): List of ticker symbols to fetch sentiment data for.
        zoom (str): Time zoom level for the chart data (e.g., '1D', '1W', '1M').
        username (str): StockTwits API username for authentication.
        password (str): StockTwits API password for authentication.
        logger (object): Logger object for logging messages and errors.
        timeout (int, optional): Request timeout in seconds. Defaults to 10.

    Returns:
        pd.DataFrame: Multi-indexed DataFrame with tickers as the first level and
            sentiment metrics as the second level of column names.

    Raises:
        ValueError: If no data was successfully fetched for any ticker in the list.
        HTTPError: If HTTP requests fail for individual symbols (logged but not raised).
        JSONDecodeError: If JSON parsing fails for individual symbols (logged but not raised).
        ConnectionError: If connection errors occur for individual symbols (logged but not raised).
        Timeout: If requests timeout for individual symbols (logged but not raised).
    """
    dfs = {}

    for symbol in tqdm(symbols, desc="Fetching sentiment data"):
        try:
            df = get_symbol_chart(
                symbol=symbol,
                zoom=zoom,
                username=username,
                password=password,
                logger=logger,
                timeout=timeout,
            )
            df = df.drop(columns=["dateTime"])
            dfs[symbol] = df
        except (HTTPError, JSONDecodeError, ConnectionError, Timeout, ValueError) as e:
            logger.error("Error fetching data for %s: %s", symbol, e)
            continue
        # Add a small delay to avoid rate limiting
        time.sleep(0.1)

    if not dfs:
        raise ValueError("No data was successfully fetched for any ticker")

    combined_df = pd.concat(
        [
            df.set_axis(pd.MultiIndex.from_product([[ticker], df.columns]), axis=1)
            for ticker, df in dfs.items()
        ],
        axis=1,
    )

    dfs.clear()

    return combined_df


def select_zoom(days_to_query: int) -> str:
    """
    Selects the appropriate StockTwits API zoom level based on the number of days to query.

    This function maps the number of days to query to the most appropriate zoom level
    for the StockTwits API. It uses predefined thresholds to optimize data granularity
    and API performance.

    Args:
        days_to_query (int): Number of days of data to query.

    Returns:
        str: Zoom level string appropriate for the query period:
            - '1D' for 1 day or less
            - '1W' for 2-7 days
            - '1M' for 8-30 days
            - '3M' for 31-90 days
            - '6M' for 91-180 days
            - '1Y' for 181-365 days
            - 'ALL' for all available data
    """
    zooms = [
        # (1, "1D"),
        (2, "1D"),
        # (7, "1W"),
        (30, "1M"),
        (90, "3M"),
        (180, "6M"),
        (365, "1Y"),
    ]
    for max_days, zoom in zooms:
        if days_to_query <= max_days:
            return zoom
    return "ALL"


def save_sentiment_data(
    output_dir: str,
    arctic_library: object,
    dataset_date_str: str,
    updated_sentiment_df: pd.DataFrame | None,
    logger: object,
    overwrite: bool = False,
) -> None:
    """
    Saves sentiment data to ArcticDB.
    """
    now = datetime.now(EASTERN_TZ)
    last_market_close = get_market_day_from_date(now)
    # current_timestamp = datetime.now(EASTERN_TZ).strftime("%Y%m%d%H%M%S")
    dataset_timestamp = get_market_day_from_date(dataset_date_str)

    # Check if a file exists up to the hour timestamp
    if not overwrite and compare_files_to_timestamp(
        output_dir,
        dataset_timestamp.strftime("%Y%m%d%H%M%S"),
        "sentiment_features_long_last_",
        10,
    ):
        logger.info("Sentiment csv dump already exists for this hour, skipping save...")
        return

    if output_dir != OUTPUT_DIR:
        logger.warning(
            "Passed output directory is different from OUTPUT_DIR constant. "
            "This may cause data to be saved in multiple locations when running "
            "the portfolio producer. To avoid this, ensure the OUTPUT_DIR "
            "constant in constants.py is the same as the passed output directory."
        )

    if (
        updated_sentiment_df is None
        or last_market_close.date() != dataset_timestamp.date()
    ):
        logger.info(
            "No data passed/current date does not match intended dataset date. "
            "Saving from ArcticDB..."
        )

        # updated_sentiment_df = arctic_library.tail("sentimentNormalized", n=1).data
        sentiment_df = arctic_library.read(
            "sentimentNormalized", date_range=(dataset_timestamp, dataset_timestamp)
        ).data
        message_volume_df = arctic_library.read(
            "messageVolumeNormalized", date_range=(dataset_timestamp, dataset_timestamp)
        ).data

        updated_sentiment_df = pd.concat(
            [sentiment_df, message_volume_df],
            keys=["sentimentNormalized", "messageVolumeNormalized"],
            axis=1,
        )

    os.makedirs(output_dir, exist_ok=True)

    dataset_timestamp = dataset_timestamp.strftime("%Y%m%d%H%M%S")

    sentiment_features_long = updated_sentiment_df.stack(
        level=[0, 1], future_stack=True
    ).reset_index()
    sentiment_features_long.columns = ["t", "sym", "metric", "value"]
    # Subset to the latest timestamp.
    t_max = sentiment_features_long["t"].max()
    sentiment_features_long_last = sentiment_features_long[
        sentiment_features_long["t"] == t_max
    ]
    sentiment_features_long_last.to_csv(
        os.path.join(
            output_dir, f"sentiment_features_{dataset_timestamp}.csv"
        ),
        index=False,
    )

    logger.info(
        "Sentiment csv dump saved to %s",
        os.path.join(
            output_dir, f"sentiment_features_{dataset_timestamp}.csv"
        ),
    )
