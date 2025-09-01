"""
Generate sentiment features and sentiment change portfolio datasets.
"""

from datetime import datetime
from typing import Optional, Tuple, Any
from dagster import build_op_context, build_init_resource_context
from dagster_pipelines.assets.etf_holdings_asset import ishares_etf_holdings_asset
from dagster_pipelines.assets.r3000_sentiment_dataset_asset import (
    r3000_sentiment_dataset_asset,
)
from dagster_pipelines.utils.ticker_utils import get_ishares_etf_tickers
from dagster_pipelines.assets.dataset_updater import update_sentiment_data
from dagster_pipelines.utils.sentiment_utils import save_sentiment_data
from dagster_pipelines.assets.sentiment_change_portfolio_asset import portfolio_asset
from dagster_pipelines.config.constants import EASTERN_TZ, OUTPUT_DIR
from dagster_pipelines.utils.portfolio_utils import save_portfolio_data
from dagster_pipelines.resources import arctic_db_resource


def _initialize_resources(
    partition_key: str | datetime,
    arctic_store: Optional[Any] = None,
    etf_ticker: Optional[str] = None,
) -> Tuple[Any, Any, Any]:
    """
    Initialize Dagster resources and assets for data processing.

    This function sets up the necessary Dagster context, Arctic database store,
    and initializes the ETF holdings and sentiment dataset assets.

    Args:
        partition_key: The partition key for the Dagster context, can be a string
            or datetime object representing the dataset date.
        arctic_store: Optional Arctic database store object. If None, a new store
            will be initialized using the arctic_db_resource.
        etf_ticker: Optional ETF ticker symbol to override the default ticker
            in the operation configuration.

    Returns:
        Tuple containing:
            - context: The initialized Dagster operation context
            - holdings_library: The initialized ETF holdings asset
            - sentiment_library: The initialized sentiment dataset asset

    Raises:
        Exception: If resource initialization fails or assets cannot be created.
    """
    if arctic_store is None:
        arctic_store = arctic_db_resource(build_init_resource_context())

    context = build_op_context(
        partition_key=partition_key,
        resources={"arctic_db": arctic_store},
        op_config={
            "etf_ticker_override": etf_ticker,
        },
    )

    holdings_library = ishares_etf_holdings_asset(context)

    sentiment_library = r3000_sentiment_dataset_asset(
        context, holdings_library=holdings_library
    )

    return context, holdings_library, sentiment_library


def generate_sentiment_features(
    etf_ticker: str,
    sentiment_output_dir: str,
    dataset_date_str: Optional[str] = None,
    arctic_store: Optional[Any] = None,
    overwrite: bool = False,
) -> None:
    """
    Generate sentiment features for ETF holdings.

    This function processes ETF holdings to generate sentiment features by:
    1. Initializing necessary resources and assets
    2. Retrieving ETF holdings for the specified ticker
    3. Updating sentiment data for the holdings
    4. Saving the sentiment data to CSV files

    Args:
        etf_ticker: The ETF ticker symbol (e.g., "SPY", "QQQ").
        sentiment_output_dir: Directory path where sentiment data CSV files
            will be saved.
        dataset_date_str: Optional date string in "YYYY-MM-DD" format for the
            dataset. If None, uses current date in Eastern timezone.
        arctic_store: Optional Arctic database store object for data storage.
            If None, a new store will be initialized.
        overwrite: If True, overwrite existing sentiment data files.
            If False, skip if files already exist.

    Returns:
        None

    Raises:
        Exception: If sentiment data generation or saving fails.
    """
    if dataset_date_str is None:
        dataset_date_str = datetime.now(EASTERN_TZ).strftime("%Y-%m-%d")

    context, holdings_library, sentiment_library = _initialize_resources(
        partition_key=dataset_date_str,
        arctic_store=arctic_store,
        etf_ticker=etf_ticker,
    )

    # Ensure sentiment data is up to date
    ishares_etf_holdings = get_ishares_etf_tickers(
        etf_ticker, dataset_date_str, holdings_library, context.log
    )

    # Update sentiment data if tickers are missing or data is out of date
    # Disable duplicate code warning (similar to dagster approach)
    # pylint: disable=duplicate-code
    update_sentiment_data(
        sentiment_library,
        tickers=ishares_etf_holdings,
        logger=context.log,
        portfolio_date=dataset_date_str,
        sentiment_dump_dir=sentiment_output_dir,
    )

    # Ensure sentiment data is saved to csv
    save_sentiment_data(
        output_dir=sentiment_output_dir,
        arctic_library=sentiment_library,
        dataset_date_str=dataset_date_str,
        updated_sentiment_df=None,
        logger=context.log,
        overwrite=overwrite,
    )


def generate_change_portfolio(
    etf_ticker: str,
    portfolio_output_dir: str,
    dataset_date_str: Optional[str] = None,
    arctic_store: Optional[Any] = None,
    overwrite: bool = False,
) -> None:
    """
    Generate change portfolio based on sentiment data.

    This function creates a portfolio based on sentiment change analysis by:
    1. Initializing necessary resources and assets
    2. Generating the portfolio asset using holdings and sentiment data
    3. Saving the portfolio data to CSV files

    Args:
        etf_ticker: The ETF ticker symbol (e.g., "SPY", "QQQ").
        portfolio_output_dir: Directory path where portfolio data CSV files
            will be saved.
        dataset_date_str: Optional date string in "YYYY-MM-DD" format for the
            dataset. If None, uses current date in Eastern timezone.
        arctic_store: Optional Arctic database store object for data storage.
            If None, a new store will be initialized.
        overwrite: If True, overwrite existing portfolio data files.
            If False, skip if files already exist.

    Returns:
        None

    Raises:
        Exception: If portfolio generation or saving fails.
    """
    if dataset_date_str is None:
        dataset_date_str = datetime.now(EASTERN_TZ).strftime("%Y-%m-%d")

    context, holdings_library, sentiment_library = _initialize_resources(
        partition_key=dataset_date_str,
        arctic_store=arctic_store,
        etf_ticker=etf_ticker,
    )

    portfolio_df = portfolio_asset(
        context,
        holdings_library=holdings_library,
        sentiment_library=sentiment_library,
    )

    save_portfolio_data(
        output_dir=portfolio_output_dir,
        portfolio_df=portfolio_df,
        dataset_timestamp=dataset_date_str,
        logger=context.log,
        overwrite=overwrite,
    )


if __name__ == "__main__":
    # Example usage of the module functions
    date_str = datetime.now(EASTERN_TZ).strftime("%Y-%m-%d")
    OVERWRITE = True

    for ticker in ["IWM", "IWV", "IWB", "SPY"]:
        generate_sentiment_features(
            etf_ticker=ticker,
            sentiment_output_dir=OUTPUT_DIR + f"/{ticker}",
            dataset_date_str=date_str,
            overwrite=OVERWRITE,
        )
        generate_change_portfolio(
            etf_ticker=ticker,
            portfolio_output_dir=OUTPUT_DIR + f"/{ticker}",
            dataset_date_str=date_str,
            overwrite=OVERWRITE,
        )
