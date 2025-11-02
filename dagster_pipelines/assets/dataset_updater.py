"""
Update and synchronize sentiment data in ArcticDB for specified tickers and dates.
"""

import os
from datetime import datetime
import pandas as pd

from dagster_pipelines.utils.database_utils import (
    arctic_db_write_or_append,
    arctic_db_batch_update,
    check_db_fragmentation,
)
from dagster_pipelines.utils.sentiment_utils import (
    get_chart_for_symbols,
    select_zoom,
    save_sentiment_data,
)
from dagster_pipelines.utils.datetime_utils import (
    ensure_timezone,
    get_market_day_from_date,
    get_timedelta_market_days,
)
from dagster_pipelines.config.constants import (
    EASTERN_TZ,
    BASE_DATASET_SYMBOLS,
    OUTPUT_DIR,
    SENTIMENT_TIME_PADDING,
    SENTIMENT_SAVE_MARKET_DAYS_ONLY,
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
        timeout=50,
        username=st_username,
        password=st_password,
        logger=logger,
    ).select_dtypes(include=["float64", "int64"])

    # make sure no partial bars are in the dataset
    updated_sentiment_df = updated_sentiment_df[
        updated_sentiment_df.index
        <= current_datetime + pd.Timedelta(minutes=SENTIMENT_TIME_PADDING)
    ]

    # If updating with 1D zoom, we only want the sentiment at market close
    if zoom == "1D":
        market_close = pd.Timestamp("16:00:00").tz_localize(EASTERN_TZ).time()
        updated_sentiment_df = updated_sentiment_df[
            updated_sentiment_df.index.time == market_close
        ]

    if add_new_columns:
        # If adding new tickers, get all data before current time
        updated_sentiment_df = updated_sentiment_df[
            (
                updated_sentiment_df.index
                <= current_datetime + pd.Timedelta(minutes=SENTIMENT_TIME_PADDING)
            )
        ]
    else:
        # If updating existing tickers, filter out existing data
        updated_sentiment_df = updated_sentiment_df[
            (updated_sentiment_df.index > last_available_datetime)
            & (
                updated_sentiment_df.index
                <= current_datetime + pd.Timedelta(minutes=SENTIMENT_TIME_PADDING)
            )
        ]

    # Updates data for both sentiment and message volume
    for symbol in BASE_DATASET_SYMBOLS:
        _update_base_dataset_symbol(
            arctic_library,
            symbol,
            updated_sentiment_df,
            current_datetime,
            logger,
            add_new_columns=add_new_columns,
        )


# Needs many arguments for flexibility
# pylint: disable=too-many-arguments
def _update_base_dataset_symbol(
    arctic_library: object,
    symbol: str,
    updated_sentiment_df: pd.DataFrame,
    current_datetime: datetime,
    logger: object,
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
    previous_metadata = arctic_library.read_metadata(symbol).metadata

    if previous_metadata is None:
        new_metadata = {
            "date_created": current_datetime.isoformat(),
            "date_updated": current_datetime.isoformat(),
            "source": "StockTwits",
            "last_dagster_run_id": None,
        }
    else:
        new_metadata = {
            "date_created": previous_metadata["date_created"],
            "date_updated": current_datetime.isoformat(),
            "source": "StockTwits",
            "last_dagster_run_id": previous_metadata["last_dagster_run_id"],
        }

    # If adding new tickers, we need to combine the existing dataset with the new data
    if add_new_columns:
        arctic_db_batch_update(
            symbol=symbol,
            arctic_library=arctic_library,
            new_data=updated_dataset,
            new_metadata=new_metadata,
            logger=logger,
            prune_previous_versions=True,
            allow_mismatched_indices=False,  # Don't add mismatched rows to existing data
        )
    else:
        arctic_db_write_or_append(
            symbol=symbol,
            arctic_library=arctic_library,
            data=updated_dataset,
            metadata=new_metadata,
            prune_previous_versions=True,
        )


# Complex function needs many locals
# pylint: disable=too-many-locals
def update_sentiment_data(
    arctic_library: object,
    tickers: list,
    logger: object,
    portfolio_date: str,
    sentiment_dump_dir: str = OUTPUT_DIR,
    save_dataset: bool = False,
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
    # Refer to last market day for sentiment update
    # Use the below line if only want to update relative to passed date
    # current_datetime = get_market_day_from_date(portfolio_date)
    now = datetime.now(EASTERN_TZ)
    current_datetime = get_market_day_from_date(now)
    logger.info(f"update_sentiment_data(): portfolio_date = {portfolio_date}")
    logger.info(f"update_sentiment_data(): current_datetime = {current_datetime}")

    if not arctic_library.has_symbol("sentimentNormalized"):
        raise ValueError("sentimentNormalized dataset not found in ArcticDB")

    # Get head and tail for earliest and latest dates and existing tickers
    symbol_head = arctic_library.head("sentimentNormalized", n=1, columns=[]).data
    symbol_tail = arctic_library.tail("sentimentNormalized", n=1).data
    earliest_available_date = symbol_head.index.min()
    last_available_date = symbol_tail.index.max()
    # Use current_datetime (market day) for timedelta so it only updates missing market days
    if SENTIMENT_SAVE_MARKET_DAYS_ONLY:
        timedelta_to_last = get_timedelta_market_days(
            start_date=last_available_date,
            end_date=current_datetime,
            prune_end_date=False,
        )

    else:
        timedelta_to_last = (current_datetime - last_available_date).days
    logger.info(f"update_sentiment_data(): last_available_date = {last_available_date}")
    logger.info(f"update_sentiment_data(): timedelta_to_last = {timedelta_to_last}")

    dataset_cols = symbol_tail.columns.tolist()

    missing_tickers = [ticker for ticker in tickers if ticker not in dataset_cols]

    if portfolio_datetime < earliest_available_date:
        raise ValueError(
            "Requested portfolio date is before earliest available sentiment date."
        )

    # Update dataset with current data regardless of portfolio date.
    if timedelta_to_last >= 1:
        # If more than 1 day since last update, update all data
        logger.warning("No recent sentiment data for %s, updating...", current_datetime)
        # Select lookback window to only download what is needed
        # Zoom timedelta is different than timedelta to last.
        # This is because ST will query from now and give partial bars if before close.
        # Fixes issue where mkt day td is 1 but now is 2 days ahead
        zoom_timedelta = (now - last_available_date).days
        zoom_param = select_zoom(zoom_timedelta)

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
    else:
        logger.warning(
            "Current time is before market close, no updates to existing ticker sentiment needed."
        )

    if missing_tickers:
        logger.warning("Tickers missing from sentiment data: %s", missing_tickers)
        logger.warning("Downloading new tickers...")

        zoom_timedelta = (now - last_available_date).days
        zoom_param = select_zoom(zoom_timedelta)

        try:
            _download_and_update_sentiment_data(
                arctic_library=arctic_library,
                tickers=missing_tickers,
                zoom=zoom_param,
                last_available_datetime=last_available_date,
                current_datetime=current_datetime,
                logger=logger,
                add_new_columns=True,
            )
        except ValueError as e:
            logger.error(f"Could not download sentiment data for missing tickers. Database will not be updated with these tickers: {e}")

    # Save export sentiment dataset CSV.
    # Convert the dataset to a long CSV with the timestamp index preserved
    # and the columns: t, sym, metric, value
    if save_dataset:
        save_sentiment_data(
            tickers=tickers,
            output_dir=sentiment_dump_dir,
            updated_sentiment_df=None,
            dataset_date_str=portfolio_date,
            arctic_library=arctic_library,
            logger=logger,
            overwrite=False,
        )

    # Check datase fragmentation
    for symbol in BASE_DATASET_SYMBOLS:
        check_db_fragmentation(symbol, arctic_library, logger)