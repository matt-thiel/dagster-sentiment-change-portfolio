import pandas as pd
import numpy as np
import os
from datetime import datetime

from dagster_pipelines.utils.database_utils import check_db_fragmentation
from dagster_pipelines.utils.sentiment_utils import get_chart_for_symbols, select_zoom
from dagster_pipelines.utils.datetime_utils import ensure_timezone
from dagster_pipelines.config.constants import EASTERN_TZ


def update_sentiment_data(arctic_library: object, tickers: list, logger: object, portfolio_date: str, update_existing: bool = True) -> None:
    """
    Updates the sentiment data in ArcticDB for the specified tickers and date, downloading missing or incomplete data.

    Args:
        arctic_library (object): ArcticDB library instance.
        tickers (list): List of ticker symbols to update.
        logger (object): Logger for logging messages.
        portfolio_date (str): Portfolio date in 'YYYY-MM-DD' format.
        update_existing (bool, optional): Whether to also update existing tickers in the database even if not in tickers. Defaults to True.

    Raises:
        ValueError: If sentiment data is missing for the requested date.
    """
    #df_sentiment = df_sentiment.swaplevel(axis=1).sort_index(axis=1)
    #df_sentiment = df_sentiment.select_dtypes(include=["float64", "int64"])
    base_dataset_symbols = ["sentimentNormalized", "messageVolumeNormalized"]
    feature_dataset_symbols = ["sentimentNormalized_1d_change_1d_lag"]
    portfolio_datetime = datetime.strptime(portfolio_date, "%Y-%m-%d")
    portfolio_datetime = ensure_timezone(portfolio_datetime, EASTERN_TZ)
    current_datetime = datetime.now(EASTERN_TZ)
    st_username = os.environ["STOCKTWITS_USERNAME"]
    st_password = os.environ["STOCKTWITS_PASSWORD"]
     
    # TODO: instead of loading sentiment dataset, read from metadata? but this is problem for na replace.

    try: 
        symbol = "sentimentNormalized"
        record = arctic_library.read(symbol)
        df_sentiment = record.data
    # except ValueError as e:
    except Exception as e:
        raise ValueError(f"Error reading {symbol} dataset from ArcticDB: {e}")

    last_available_date = df_sentiment.index.max()
    earliest_available_date = df_sentiment.index.min()

    missing_tickers = [ticker for ticker in tickers if ticker not in df_sentiment.columns]
    if update_existing:
        tickers_with_nan = [ticker for ticker in df_sentiment.columns if df_sentiment[ticker].isna().any()]

    else:
        tickers_with_nan = [ticker for ticker in tickers if ticker in df_sentiment.columns and df_sentiment[ticker].isna().any()]

    tickers_to_download = missing_tickers + tickers_with_nan

    if tickers_to_download:
        logger.warning("Some tickers are not in the sentiment data or contain missing values. Downloading...")
        updated_sentiment_df = get_chart_for_symbols(symbols=tickers_to_download, zoom='ALL', timeout=10, username=st_username, password=st_password, logger=logger).select_dtypes(include=["float64", "int64"])
        for symbol in base_dataset_symbols:
            updated_dataset = updated_sentiment_df.xs(symbol, axis=1, level=1)
            existing_symbol = arctic_library.read(symbol)
            existing_dataset = existing_symbol.data
            updated_dataset = pd.concat([existing_dataset, updated_dataset])
            updated_dataset = updated_dataset[~updated_dataset.index.duplicated(keep='last')]
            updated_dataset = updated_dataset.sort_index()
            arctic_library.write(symbol, updated_dataset, metadata={
                "date_created": existing_symbol.metadata["date_created"],
                "date_updated": current_datetime.isoformat(),
                "data_start_date": updated_dataset.index.min(),
                "data_end_date": updated_dataset.index.max(),
                "source": "StockTwits",
                "last_dagster_run_id": existing_symbol.metadata["last_dagster_run_id"],
            })

        for symbol in feature_dataset_symbols:
            if symbol == "sentimentNormalized_1d_change_1d_lag":
                base_symbol = symbol.split("_1d_change_1d_lag")[0]
                dataset = updated_sentiment_df.xs(base_symbol, axis=1, level=1)
                dataset = dataset.pct_change(periods=1, fill_method=None).shift(1)
                dataset.replace([np.inf, -np.inf], np.nan, inplace=True)
                existing_symbol = arctic_library.read(symbol)
                existing_dataset = existing_symbol.data
                updated_dataset = pd.concat([existing_dataset, updated_dataset])
                updated_dataset = updated_dataset[~updated_dataset.index.duplicated(keep='last')]
                updated_dataset = updated_dataset.sort_index()
                arctic_library.write(symbol, updated_dataset, metadata={
                "date_created": existing_symbol.metadata["date_created"],
                "date_updated": current_datetime.isoformat(),
                "data_start_date": updated_dataset.index.min(),
                "data_end_date": updated_dataset.index.max(),
                "source": "StockTwits",
                "last_dagster_run_id": existing_symbol.metadata["last_dagster_run_id"],
            })
        updated_dataset = None

    updated_sentiment_df = None

    if portfolio_datetime < earliest_available_date:
        raise ValueError("Requested portfolio date is before earliest available sentiment date.")
    elif portfolio_datetime > last_available_date:
        logger.warning("No exisiting sentiment data for %s.", portfolio_datetime)
        timedelta_to_last = (current_datetime.date() - last_available_date).days
        zoom_param = select_zoom(timedelta_to_last)
        if update_existing:
            update_tickers = tickers + df_sentiment.columns
        else:
            update_tickers = tickers
        updated_sentiment_df = get_chart_for_symbols(symbols=update_tickers, zoom=zoom_param, timeout=10, username=st_username, password=st_password, logger=logger).select_dtypes(include=["float64", "int64"])

    else:
        logger.info("Using exisiting sentiment data for %s.", portfolio_datetime)


    if updated_sentiment_df is not None:
        for symbol in base_dataset_symbols:
            updated_dataset = updated_sentiment_df.xs(symbol, axis=1, level=1)
            existing_symbol = arctic_library.read(symbol)
            existing_dataset = existing_symbol.data
            updated_dataset = pd.concat([existing_dataset, updated_dataset])
            updated_dataset = updated_dataset[~updated_dataset.index.duplicated(keep='last')]
            updated_dataset = updated_dataset.sort_index()
            arctic_library.write(symbol, updated_dataset, metadata={
                "date_created": existing_symbol.metadata["date_created"],
                "date_updated": current_datetime.isoformat(),
                "data_start_date": updated_dataset.index.min(),
                "data_end_date": updated_dataset.index.max(),
                "source": "StockTwits",
                "last_dagster_run_id": existing_symbol.metadata["last_dagster_run_id"],
            })
    else:
        logger.info("Data is available for %s.", portfolio_datetime)

    logger.info(arctic_library.list_symbols())

    # Check datase fragmentation
    for symbol in base_dataset_symbols + feature_dataset_symbols:
        check_db_fragmentation(symbol, arctic_library, logger)