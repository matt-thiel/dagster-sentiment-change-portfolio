"""
Update and synchronize sentiment data in ArcticDB for specified tickers and dates.
"""

import os
from datetime import datetime
import pandas as pd

from dagster_pipelines.utils.database_utils import (
    arctic_db_write_or_append,
    check_db_fragmentation,
)
from dagster_pipelines.utils.sentiment_utils import get_chart_for_symbols, select_zoom
from dagster_pipelines.utils.datetime_utils import ensure_timezone
from dagster_pipelines.config.constants import (
    EASTERN_TZ,
    BASE_DATASET_SYMBOLS,
    FEATURE_LOOKBACK_WINDOW,
)


# Requires many arguments for update flexibility
# pylint: disable=too-many-arguments
def _download_and_update_sentiment_data(
    arctic_library: object,
    tickers: list,
    zoom: str,
    last_available_datetime: datetime,
    current_datetime: datetime,
    logger: object,
    add_new_columns: bool = False,
) -> None:
    """
    Download and update sentiment data for a list of tickers.

    Args:
        arctic_library (object): ArcticDB library instance.
        tickers (list): List of ticker symbols to update.
        zoom (str): Zoom level for sentiment data.
        last_available_datetime (datetime): The last available datetime for sentiment data.
        current_datetime (datetime): The current datetime.
        logger (object): Logger for logging messages.
        add_new_columns (bool): Whether new columns will be added to the dataset.
    """

    st_username = os.environ["STOCKTWITS_USERNAME"]
    st_password = os.environ["STOCKTWITS_PASSWORD"]

    # Download sentiment data for given tickers
    updated_sentiment_df = get_chart_for_symbols(
        symbols=tickers,
        zoom=zoom,
        timeout=10,
        username=st_username,
        password=st_password,
        logger=logger,
    ).select_dtypes(include=["float64", "int64"])

    if add_new_columns:
        # If adding new tickers, get all data before current time
        updated_sentiment_df = updated_sentiment_df[
            (updated_sentiment_df.index < current_datetime)
        ]
    else:
        # If updating existing tickers, filter out existing data
        updated_sentiment_df = updated_sentiment_df[
            (updated_sentiment_df.index > last_available_datetime)
            & (updated_sentiment_df.index < current_datetime)
        ]

    # Updates data for both sentiment and message volume
    for symbol in BASE_DATASET_SYMBOLS:
        _update_base_dataset_symbol(
            arctic_library,
            symbol,
            updated_sentiment_df,
            current_datetime,
            add_new_columns=add_new_columns,
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

    # If adding new tickers, we need to combine the existing dataset with the new data
    if add_new_columns:
        existing_symbol = arctic_library.read(symbol)
        previous_metadata = existing_symbol.metadata
        existing_dataset = existing_symbol.data
        # Add new columns to existing dataset
        updated_dataset = updated_dataset.combine_first(existing_dataset)
        updated_dataset = updated_dataset.sort_index()
    else:
        previous_metadata = arctic_library.read_metadata(symbol).metadata

    new_metadata = {
        "date_created": previous_metadata["date_created"],
        "date_updated": current_datetime.isoformat(),
        "source": "StockTwits",
        "last_dagster_run_id": previous_metadata["last_dagster_run_id"],
    }

    arctic_db_write_or_append(
        symbol=symbol,
        arctic_library=arctic_library,
        data=updated_dataset,
        metadata=new_metadata,
        prune_previous_versions=True,
        force_write=add_new_columns,  # Overwrite existing with combined dataset
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

    if not arctic_library.has_symbol("sentimentNormalized"):
        raise ValueError("sentimentNormalized dataset not found in ArcticDB")

    # Get head and tail for earliest and latest dates and existing tickers
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
            logger.warning(
                "Cannot update with today's date because it is before market close."
            )
            return
        logger.warning(
            "No recent sentiment data for %s, updating...", portfolio_datetime
        )
        # Select lookback window to only download what is needed
        zoom_param = select_zoom(timedelta_to_last + FEATURE_LOOKBACK_WINDOW)

        # Only update existing, missing tickers are added in next step
        _download_and_update_sentiment_data(
            arctic_library=arctic_library,
            tickers=dataset_cols,
            zoom=zoom_param,
            last_available_datetime=last_available_date,
            current_datetime=current_datetime,
            logger=logger,
            add_new_columns=False,
        )

    if missing_tickers:
        logger.warning("Tickers missing from sentiment data: %s", missing_tickers)
        logger.warning("Downloading new tickers...")

        _download_and_update_sentiment_data(
            arctic_library=arctic_library,
            tickers=missing_tickers,
            zoom="ALL",
            last_available_datetime=last_available_date,
            current_datetime=current_datetime,
            logger=logger,
            add_new_columns=True,
        )

    # Check datase fragmentation
    for symbol in BASE_DATASET_SYMBOLS:
        check_db_fragmentation(symbol, arctic_library, logger)
