"""
Fetches and stores the holdings for a specified iShares ETF in ArcticDB.
"""

from arcticdb.version_store.library import Library
from dagster import asset, AssetExecutionContext
from dagster_pipelines.utils.ticker_utils import initialize_ishares_etf_holdings
from dagster_pipelines.config.constants import ETF_TICKER, DEFAULT_LIBRARY_OPTIONS


@asset(required_resource_keys={"arctic_db"})
def ishares_etf_holdings_asset(context: AssetExecutionContext) -> Library:
    """
    Fetches and stores the holdings for a specified iShares ETF in ArcticDB.

    Args:
        context: Dagster asset context with ArcticDB resource.

    Returns:
        Library: ArcticDB library containing ETF holdings.
    """
    arctic_store = context.resources.arctic_db
    library_name = "holdings"
    logger = context.log
    try:
        # Create the library if it doesn't exist
        if library_name not in arctic_store.list_libraries():
            arctic_store.create_library(library_name, library_options=DEFAULT_LIBRARY_OPTIONS)

        arctic_library = arctic_store[library_name]
        
        # If running in debug mode, use the ETF ticker override.
        etf_ticker = context.op_config.get("etf_ticker_override", ETF_TICKER)
        # Downloads holdings if needed
        initialize_ishares_etf_holdings(etf_ticker, arctic_library, logger)

    except Exception as e:
        logger.error(f"Error creating library, check that S3 bucket exists: {e}")
        raise e

    return arctic_library
