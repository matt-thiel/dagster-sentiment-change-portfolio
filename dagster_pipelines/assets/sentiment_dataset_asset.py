from dagster import asset, AssetIn
from arcticdb.version_store.library import Library
from datetime import datetime
import os
import numpy as np
from dagster_pipelines.utils.sentiment_utils import get_chart_for_symbols
from dagster_pipelines.config.constants import EASTERN_TZ


@asset(required_resource_keys={"arctic_db", "s3"}, ins={"ishares_etf_holdings": AssetIn("ishares_etf_holdings")})
def sentiment_dataset_asset(context, ishares_etf_holdings: list[str]) -> Library:
    """
    Ensures sentiment datasets for the given tickers exist in ArcticDB, downloading and storing if missing.

    Args:
        context: Dagster asset context with ArcticDB and S3 resources.
        ishares_etf_holdings (list[str]): List of ETF holding tickers.

    Returns:
        Library: ArcticDB library containing sentiment datasets.
    """
    logger = context.log
    arctic_store = context.resources.arctic_db
    library_name = "sentiment_features"

    # Create the library if it doesn't exist
    if library_name not in arctic_store.list_libraries():
        arctic_store.create_library(library_name)
    # Get the library
    arctic_library = arctic_store[library_name]

    base_dataset_symbols = ["sentimentNormalized", "messageVolumeNormalized"]
    feature_dataset_symbols = ["sentimentNormalized_1d_change_1d_lag"]
    st_username = os.environ["STOCKTWITS_USERNAME"]
    st_password = os.environ["STOCKTWITS_PASSWORD"]
    sentiment_features = None

    try:
        for symbol in base_dataset_symbols:
            # TODO: use arcticdb metadata to ensure all symbols in db
            record = arctic_library.read(symbol)
            dataset = record.data
    except:
        logger.warning(f"Dataset {symbol} not found in ArcticDB. Downloading...")
        sentiment_features = get_chart_for_symbols(symbols=ishares_etf_holdings, 
                                                   zoom='ALL', 
                                                   timeout=10, 
                                                   username=st_username, 
                                                   password=st_password, 
                                                   logger=logger).select_dtypes(include=["float64", "int64"])
        
        current_time = datetime.now(EASTERN_TZ)
        metadata = {
            "date_created": current_time.isoformat(),
            "date_updated": current_time.isoformat(),
            "data_start_date": sentiment_features.index.min(),
            "data_end_date": sentiment_features.index.max(),
            "source": "StockTwits",
            "last_dagster_run_id": getattr(context, "run_id", None),
        }
        for symbol in base_dataset_symbols:
            dataset = sentiment_features.xs(symbol, axis=1, level=1)
            arctic_library.write(symbol, dataset, metadata=metadata)

    for symbol in feature_dataset_symbols:
        if symbol == "sentimentNormalized_1d_change_1d_lag":
            base_symbol = symbol.split("_1d_change_1d_lag")[0]  
            if sentiment_features is None:
                try:
                    arctic_library.read(symbol)
                    logger.info(f"Feature dataset {symbol} found in ArcticDB. Skipping creation...")
                    return arctic_library
                except Exception as e:
                    logger.warning(f"Feature dataset {symbol} not found in ArcticDB. Creating...")

                    dataset = arctic_library.read(base_symbol)
            else:
                dataset = sentiment_features.xs(base_symbol, axis=1, level=1)
                
            dataset = dataset.pct_change(periods=1, fill_method=None).shift(1)
            dataset.replace([np.inf, -np.inf], np.nan, inplace=True)
            arctic_library.write(symbol, dataset, metadata=metadata)

    return arctic_library