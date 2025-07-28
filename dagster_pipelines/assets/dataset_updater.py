"""
Update and synchronize sentiment data in ArcticDB for specified tickers and dates.
"""
import os
from datetime import datetime
import pandas as pd
import numpy as np
from arcticdb.exceptions import ArcticNativeException

from dagster_pipelines.utils.database_utils import check_db_fragmentation
from dagster_pipelines.utils.sentiment_utils import get_chart_for_symbols, select_zoom
from dagster_pipelines.utils.datetime_utils import ensure_timezone
from dagster_pipelines.config.constants import EASTERN_TZ, BASE_DATASET_SYMBOLS, FEATURE_LOOKBACK_WINDOW

# Disable too many locals/branches/statements due to function complexity
# pylint: disable=too-many-locals, too-many-branches, too-many-statements

def _update_base_dataset_symbol(
    arctic_library: object,
    symbol: str,
    updated_sentiment_df: pd.DataFrame,
    current_datetime: datetime,
    add_new_columns: bool = False
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
        updated_dataset = updated_dataset.combine_first(existing_dataset)
        updated_dataset = updated_dataset.sort_index()
        data_start_date = updated_dataset.index.min()
    else:
        previous_metadata = arctic_library.read_metadata(symbol).metadata
        data_start_date = previous_metadata["data_start_date"]

    arctic_library.update(
        symbol=symbol,
        data=updated_dataset,
        metadata={
            "date_created": previous_metadata["date_created"],
            "date_updated": current_datetime.isoformat(),
            "data_start_date": data_start_date,
            "data_end_date": updated_dataset.index.max(),
            "source": "StockTwits",
            "last_dagster_run_id": previous_metadata["last_dagster_run_id"],
        },
        upsert=True,
        prune_previous_versions=True,
    )

def _update_feature_dataset_symbol(
    arctic_library: object,
    symbol: str,
    updated_sentiment_df: pd.DataFrame,
    current_datetime: datetime
) -> None:
    """
    Update a feature dataset symbol in ArcticDB with new data.

    Args:
        arctic_library (object): ArcticDB library instance.
        symbol (str): The feature symbol to update.
        updated_sentiment_df (pd.DataFrame): DataFrame containing updated sentiment data.
        current_datetime (datetime): The current datetime for metadata.
    """
    if symbol == "sentimentNormalized_1d_change_1d_lag":
        base_symbol = symbol.split("_1d_change_1d_lag", maxsplit=1)[0]
        dataset = updated_sentiment_df.xs(base_symbol, axis=1, level=1)
        dataset = dataset.pct_change(periods=1, fill_method=None).shift(1)
        dataset.replace([np.inf, -np.inf], np.nan, inplace=True)
        existing_symbol = arctic_library.read(symbol)
        existing_dataset = existing_symbol.data
        updated_dataset = dataset.combine_first(existing_dataset)
        updated_dataset = updated_dataset.sort_index()
        arctic_library.write(
            symbol,
            updated_dataset,
            metadata={
                "date_created": existing_symbol.metadata["date_created"],
                "date_updated": current_datetime.isoformat(),
                "data_start_date": updated_dataset.index.min(),
                "data_end_date": updated_dataset.index.max(),
                "source": "StockTwits",
                "last_dagster_run_id": existing_symbol.metadata["last_dagster_run_id"],
            },
        )

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

    # TODO: Use query builder for calculating features
    # TODO: Throw warning for nans when calculating feaures?
    # TODO: SORT COLS BEFORE WRITE

    #last_available_date = df_sentiment.index.max()
    #earliest_available_date = df_sentiment.index.min()

    missing_tickers = [
        ticker for ticker in tickers if ticker not in dataset_cols
    ]

    tickers_to_download = missing_tickers

    if tickers_to_download:
        logger.warning("Tickers missing from sentiment data: %s", missing_tickers)
        logger.warning(
            "Some tickers are not in the sentiment data or contain missing values. Downloading..."
        )
        updated_sentiment_df = get_chart_for_symbols(
            symbols=tickers_to_download,
            zoom="ALL",
            timeout=10,
            username=st_username,
            password=st_password,
            logger=logger,
        ).select_dtypes(include=["float64", "int64"])
        for symbol in BASE_DATASET_SYMBOLS:
            _update_base_dataset_symbol(arctic_library, symbol, updated_sentiment_df, current_datetime, add_new_columns=True)

    updated_sentiment_df = None

    if portfolio_datetime < earliest_available_date:
        raise ValueError(
            "Requested portfolio date is before earliest available sentiment date."
        )
    if portfolio_datetime > last_available_date:
        logger.warning("No exisiting sentiment data for %s.", portfolio_datetime)
        timedelta_to_last = (current_datetime.date() - last_available_date).days
        zoom_param = select_zoom(timedelta_to_last + FEATURE_LOOKBACK_WINDOW)

        update_tickers = list(set(tickers) | set(dataset_cols))

        updated_sentiment_df = get_chart_for_symbols(
            symbols=update_tickers,
            zoom=zoom_param,
            timeout=10,
            username=st_username,
            password=st_password,
            logger=logger,
        ).select_dtypes(include=["float64", "int64"])
    else:
        logger.info("Using exisiting sentiment data for %s.", portfolio_datetime)

    if updated_sentiment_df is not None:
        for symbol in BASE_DATASET_SYMBOLS:
            _update_base_dataset_symbol(arctic_library,
                                        symbol,
                                        updated_sentiment_df,
                                        current_datetime,
                                        add_new_columns=False,
                                        )
    else:
        logger.info("Data is available for %s.", portfolio_datetime)

    # Check datase fragmentation
    for symbol in BASE_DATASET_SYMBOLS:
        check_db_fragmentation(symbol, arctic_library, logger)
