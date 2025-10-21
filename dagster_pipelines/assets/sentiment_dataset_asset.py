"""
Ensures sentiment datasets for the given tickers exist in ArcticDB,
  downloading and storing if missing.
"""

from datetime import datetime
import os
import pandas as pd
from dagster import asset, AssetIn, AssetExecutionContext
from arcticdb.version_store.library import Library

from dagster_pipelines.utils.sentiment_utils import get_chart_for_symbols
from dagster_pipelines.config.constants import (
    BASE_DATASET_SYMBOLS,
    EASTERN_TZ,
    ETF_TICKER,
    DEFAULT_LIBRARY_OPTIONS,
    SENTIMENT_TIME_PADDING,
)
from dagster_pipelines.utils.ticker_utils import get_most_recent_etf_holdings

# Disable too many locals due to function complexity
# pylint: disable=too-many-locals


@asset(
    required_resource_keys={"arctic_db"},
    ins={"holdings_library": AssetIn("ishares_etf_holdings_asset")},
)
def sentiment_dataset_asset(
    context: AssetExecutionContext, holdings_library: Library
) -> Library:
    """
    Ensures sentiment datasets for the given tickers exist in ArcticDB,
      downloading and storing if missing.

    Args:
        context: Dagster asset context with ArcticDB resource.
        holdings_library (Library): ArcticDB library containing ETF holdings.

    Returns:
        Library: ArcticDB library containing sentiment datasets.
    """
    logger = context.log
    arctic_store = context.resources.arctic_db
    library_name = "sentiment_features"

    ishares_etf_holdings = get_most_recent_etf_holdings(holdings_library, ETF_TICKER)

    # Create the library if it doesn't exist
    if library_name not in arctic_store.list_libraries():
        arctic_store.create_library(
            library_name, library_options=DEFAULT_LIBRARY_OPTIONS
        )
    # Get the library
    arctic_library = arctic_store[library_name]

    # Get StockTwits credentials
    st_username = os.environ["STOCKTWITS_USERNAME"]
    st_password = os.environ["STOCKTWITS_PASSWORD"]

    # Code similar to sentiment updater, disable warning
    # pylint: disable=duplicate-code
    if not all(arctic_library.has_symbol(symbol) for symbol in BASE_DATASET_SYMBOLS):
        logger.warning("Dataset not found in ArcticDB. Downloading...")
        sentiment_features = get_chart_for_symbols(
            symbols=ishares_etf_holdings,
            zoom="ALL",
            timeout=50,
            username=st_username,
            password=st_password,
            logger=logger,
        ).select_dtypes(include=["float64", "int64"])

        current_time = datetime.now(EASTERN_TZ) + pd.Timedelta(minutes=SENTIMENT_TIME_PADDING)

        # Select only data before the current time
        sentiment_features = sentiment_features[sentiment_features.index < current_time]

        metadata = {
            "date_created": current_time.isoformat(),
            "date_updated": current_time.isoformat(),
            "source": "StockTwits",
            "last_dagster_run_id": getattr(context, "run_id", None),
        }
        for symbol in BASE_DATASET_SYMBOLS:
            dataset = sentiment_features.xs(symbol, axis=1, level=1)
            arctic_library.write(symbol, dataset, metadata=metadata)

    return arctic_library
