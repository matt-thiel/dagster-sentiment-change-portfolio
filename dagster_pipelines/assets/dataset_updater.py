"""
Update and synchronize sentiment data in ArcticDB for specified tickers and dates.
"""

import os
from datetime import datetime
import pandas as pd

from dagster_pipelines.utils.database_utils import check_db_fragmentation
from dagster_pipelines.utils.sentiment_utils import get_chart_for_symbols, select_zoom
from dagster_pipelines.utils.datetime_utils import ensure_timezone
from dagster_pipelines.config.constants import (
    EASTERN_TZ,
    BASE_DATASET_SYMBOLS,
    FEATURE_LOOKBACK_WINDOW,
)

def _update_base_dataset_symbol(
    arctic_library: object,
    symbol: str,
    updated_sentiment_df: pd.DataFrame,
    current_datetime: datetime,
    add_new_columns: bool = False,
) -> None:
    """
    Update a base dataset symbol in ArcticDB with new data.

    Args:
        arctic_library (object): ArcticDB library instance.
        symbol (str): The symbol to update.
        updated_sentiment_df (pd.DataFrame): DataFrame containing updated sentiment data.
        current_datetime (datetime): The current datetime for metadata.
        add_new_columns (bool): Whether new columns will be added to the dataset.
    """
    updated_dataset = updated_sentiment_df.xs(symbol, axis=1, level=1)
    

    if add_new_columns:
        existing_symbol = arctic_library.read(symbol)
        previous_metadata = existing_symbol.metadata
        existing_dataset = existing_symbol.data
        # Add new columns to existing dataset
        updated_dataset = updated_dataset.combine_first(existing_dataset)
        updated_dataset = updated_dataset.sort_index()
        arctic_library.write(
            symbol=symbol,
            data=updated_dataset,
            metadata={
            "date_created": previous_metadata["date_created"],
            "date_updated": current_datetime.isoformat(),
            "source": "StockTwits",
            "last_dagster_run_id": previous_metadata["last_dagster_run_id"],
            },
            prune_previous_versions=True,
        )
    else:
        previous_metadata = arctic_library.read_metadata(symbol).metadata

        #arctic_library.update(
        arctic_library.append(
            symbol=symbol,
            data=updated_dataset,
            metadata={
                "date_created": previous_metadata["date_created"],
                "date_updated": current_datetime.isoformat(),
                "source": "StockTwits",
                "last_dagster_run_id": previous_metadata["last_dagster_run_id"],
            },
            prune_previous_versions=True,
        )

# Complex function needs many locals
# pylint: disable=too-many-locals
def update_sentiment_data(
    arctic_library: object,
    tickers: list,
    logger: object,
    portfolio_date: str,
) -> None:
    """
    Updates the sentiment data in ArcticDB for the specified tickers and date,
    downloading missing or incomplete data.

    Args:
        arctic_library (object): ArcticDB library instance.
        tickers (list): List of ticker symbols to update.
        logger (object): Logger for logging messages.
        portfolio_date (str): Portfolio date in 'YYYY-MM-DD' format.

    Raises:
        ValueError: If sentiment data is missing for the requested date.
    """
    portfolio_datetime = datetime.strptime(portfolio_date, "%Y-%m-%d")
    portfolio_datetime = ensure_timezone(portfolio_datetime, EASTERN_TZ)
    current_datetime = datetime.now(EASTERN_TZ)
    st_username = os.environ["STOCKTWITS_USERNAME"]
    st_password = os.environ["STOCKTWITS_PASSWORD"]

    if not arctic_library.has_symbol("sentimentNormalized"):
        raise ValueError("sentimentNormalized dataset not found in ArcticDB")

    symbol_head = arctic_library.head("sentimentNormalized", n=1, columns=[]).data
    symbol_tail = arctic_library.tail("sentimentNormalized", n=1).data
    earliest_available_date = symbol_head.index.min()
    last_available_date = symbol_tail.index.max()

    dataset_cols = symbol_tail.columns.tolist()

    missing_tickers = [ticker for ticker in tickers if ticker not in dataset_cols]

    if portfolio_datetime < earliest_available_date:
        raise ValueError(
            "Requested portfolio date is before earliest available sentiment date."
        )
    if portfolio_datetime > last_available_date:
        timedelta_to_last = (current_datetime - last_available_date).days
        if timedelta_to_last < 1:
            logger.warning("Cannot update with today's date because it is before market close.")
            return
        logger.warning("No recent sentiment data for %s, updating...", portfolio_datetime)
        zoom_param = select_zoom(timedelta_to_last + FEATURE_LOOKBACK_WINDOW)

        # Only update what is in the dataset because anything missing will be downloaded in the next step
        update_tickers = dataset_cols

        updated_sentiment_df = get_chart_for_symbols(
            symbols=update_tickers,
            zoom=zoom_param,
            timeout=10,
            username=st_username,
            password=st_password,
            logger=logger,
        ).select_dtypes(include=["float64", "int64"])

        # Only pass new data to the updater helper
        updated_sentiment_df = updated_sentiment_df[
            (updated_sentiment_df.index > last_available_date) &
            (updated_sentiment_df.index < current_datetime)
        ]

        for symbol in BASE_DATASET_SYMBOLS:
            _update_base_dataset_symbol(
                arctic_library,
                symbol,
                updated_sentiment_df,
                current_datetime,
                add_new_columns=False,
            )
    else:
        logger.info("Data is current for %s.", portfolio_datetime)

    if missing_tickers:
        logger.warning("Tickers missing from sentiment data: %s", missing_tickers)
        logger.warning("Downloading new tickers...")

        updated_sentiment_df = get_chart_for_symbols(
            symbols=missing_tickers,
            zoom="ALL",
            timeout=10,
            username=st_username,
            password=st_password,
            logger=logger,
        ).select_dtypes(include=["float64", "int64"])



        updated_sentiment_df = updated_sentiment_df[
            (updated_sentiment_df.index < current_datetime)
        ]


        for symbol in BASE_DATASET_SYMBOLS:
            _update_base_dataset_symbol(
                arctic_library,
                symbol,
                updated_sentiment_df,
                current_datetime,
                add_new_columns=True,
            )

    # Check datase fragmentation
    for symbol in BASE_DATASET_SYMBOLS:
        check_db_fragmentation(symbol, arctic_library, logger)
