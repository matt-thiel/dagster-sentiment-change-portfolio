"""
Fetches and stores the holdings for a specified iShares ETF in ArcticDB.
"""

from dagster import asset, AssetExecutionContext
from dagster_pipelines.utils.ticker_utils import get_ishares_etf_tickers


@asset(required_resource_keys={"arctic_db", "s3"})
def ishares_etf_holdings_asset(context: AssetExecutionContext) -> list[str]:
    """
    Fetches and stores the holdings for a specified iShares ETF in ArcticDB.

    Args:
        context: Dagster asset context with ArcticDB and S3 resources.

    Returns:
        list[str]: List of ETF holding tickers.
    """
    arctic_store = context.resources.arctic_db
    library_name = "holdings"
    etf_ticker = "IWM"
    logger = context.log
    try:
        # Create the library if it doesn't exist
        if library_name not in arctic_store.list_libraries():
            arctic_store.create_library(library_name)
        # Get the library
        arctic_library = arctic_store[library_name]
        holdings = get_ishares_etf_tickers(etf_ticker, arctic_library, logger)
    except Exception as e:
        logger.error(f"Error creating library, check that S3 bucket exists: {e}")
        raise e

    return holdings[:10]
