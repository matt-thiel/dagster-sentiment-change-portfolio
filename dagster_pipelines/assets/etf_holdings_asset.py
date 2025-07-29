"""
Fetches and stores the holdings for a specified iShares ETF in ArcticDB.
"""

from arcticdb.version_store.library import Library

from dagster import asset, AssetExecutionContext
from dagster_pipelines.utils.ticker_utils import initialize_ishares_etf_holdings
from dagster_pipelines.config.constants import ETF_TICKER


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
    #partition_date = context.asset_partition_key_for_output()
    #partition_date = datetime.strptime(partition_date, "%Y-%m-%d")
    #partition_date = ensure_timezone(partition_date, EASTERN_TZ)
    library_name = "holdings"
    logger = context.log
    try:
        # Create the library if it doesn't exist
        if library_name not in arctic_store.list_libraries():
            arctic_store.create_library(library_name)

        arctic_library = arctic_store[library_name]
        # Downloads holdings if needed and returns a list of the current universe
        #holdings = initialize_ishares_etf_holdings(
        #    etf_ticker, arctic_library, logger
        #)
        initialize_ishares_etf_holdings(ETF_TICKER, arctic_library, logger)

    except Exception as e:
        logger.error(f"Error creating library, check that S3 bucket exists: {e}")
        raise e

    #return holdings
    return arctic_library
