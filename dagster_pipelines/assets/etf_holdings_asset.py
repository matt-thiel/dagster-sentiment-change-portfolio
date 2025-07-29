"""
Fetches and stores the holdings for a specified iShares ETF in ArcticDB.
"""

from datetime import datetime

from dagster import asset, AssetExecutionContext
from dagster_pipelines.config.constants import EASTERN_TZ
from dagster_pipelines.utils.datetime_utils import ensure_timezone
from dagster_pipelines.utils.ticker_utils import get_ishares_etf_tickers


@asset(required_resource_keys={"arctic_db"})
def ishares_etf_holdings_asset(context: AssetExecutionContext) -> list[str]:
    """
    Fetches and stores the holdings for a specified iShares ETF in ArcticDB.

    Args:
        context: Dagster asset context with ArcticDB resource.

    Returns:
        list[str]: List of ETF holding tickers.
    """
    arctic_store = context.resources.arctic_db
    partition_date = context.asset_partition_key_for_output()
    partition_date = datetime.strptime(partition_date, "%Y-%m-%d")
    partition_date = ensure_timezone(partition_date, EASTERN_TZ)
    library_name = "holdings"
    etf_ticker = "IWM"
    logger = context.log
    try:
        # Create the library if it doesn't exist
        if library_name not in arctic_store.list_libraries():
            arctic_store.create_library(library_name)
        # Get the library
        arctic_library = arctic_store[library_name]
        holdings = get_ishares_etf_tickers(
            etf_ticker, partition_date, arctic_library, logger
        )
    except Exception as e:
        logger.error(f"Error creating library, check that S3 bucket exists: {e}")
        raise e

    return holdings[:20]
