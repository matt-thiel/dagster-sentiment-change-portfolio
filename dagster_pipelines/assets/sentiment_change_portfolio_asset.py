"""
Assets and utilities for generating and managing a sentiment-based portfolio for the SPY ETF.

This module includes Dagster assets and helper functions for:
- Fetching ETF holdings
- Managing sentiment datasets
- Updating sentiment data
- Producing and saving portfolios
- Debugging portfolio generation

Assets interact with ArcticDB (on S3), StockTwits, and vBase for data storage and validation.
"""

import os
from pathlib import Path
from datetime import datetime
from arcticdb.version_store.library import Library
from dotenv import load_dotenv
import pandas as pd

from dagster import (
    asset,
    build_op_context,
    AssetIn,
    build_init_resource_context,
    AssetExecutionContext,
)


from dagster_pipelines.utils.database_utils import (
    print_arcticdb_summary,
    print_arcticdb_symbol,
)
from dagster_pipelines.config.constants import EASTERN_TZ, ETF_TICKER, DEBUG_ETF_TICKERS
from dagster_pipelines.assets.sentiment_change_portfolio_producer import (
    produce_portfolio,
)
from dagster_pipelines.resources import arctic_db_resource
from dagster_pipelines.assets.etf_holdings_asset import ishares_etf_holdings_asset
from dagster_pipelines.assets.r3000_sentiment_dataset_asset import (
    r3000_sentiment_dataset_asset,
)
from dagster_pipelines.assets.dataset_updater import update_sentiment_data
from dagster_pipelines.config.constants import PORTFOLIO_PARTITIONS_DEF, OUTPUT_DIR
from dagster_pipelines.utils.ticker_utils import get_ishares_etf_tickers
from dagster_pipelines.utils.sentiment_utils import save_sentiment_data


# pylint: disable=too-many-locals
@asset(
    partitions_def=PORTFOLIO_PARTITIONS_DEF,
    ins={
        "holdings_library": AssetIn("ishares_etf_holdings_asset"),
        "sentiment_library": AssetIn("sentiment_dataset_asset"),
    },
    required_resource_keys={"arctic_db"},
)
def portfolio_asset(
    context: AssetExecutionContext,
    holdings_library: Library,
    sentiment_library: Library,
) -> pd.DataFrame:
    """
    Generates and saves a portfolio for the SPY ETF for a given partition date,
      stamps it in vBase, and uploads to S3.

    Args:
        context: Dagster asset context with partition key and resources.
        holdings_library (Library): ArcticDB library containing ETF holdings.
        sentiment_library (Library): ArcticDB library with sentiment data.

    Returns:
        pd.DataFrame: DataFrame containing portfolio positions with columns:
            - 'sym': Ticker symbol
            - 'wt': Position weight (positive for long, negative for short, 0 for no position)

    Raises:
        ValueError: If required environment variables are missing or data is unavailable.
    """

    # Load the environment variables and check that settings are defined.
    load_dotenv()
    # Check that all the required settings are defined:
    required_settings = [
        "VBASE_API_KEY",
        "VBASE_COMMITMENT_SERVICE_PRIVATE_KEY",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "STOCKTWITS_USERNAME",
        "STOCKTWITS_PASSWORD",
    ]
    for setting in required_settings:
        if setting not in os.environ:
            raise ValueError(f"{setting} environment variable is not set.")

    # Get the current partition date.
    partition_date = context.asset_partition_key_for_output()
    context.log.info("Starting portfolio generation for %s", partition_date)

    # If running in debug mode, use the ETF ticker override.
    etf_ticker = context.op_config.get("etf_ticker_override", ETF_TICKER)

    try:
        # Get the tickers for the partition date.
        ishares_etf_holdings = get_ishares_etf_tickers(
            etf_ticker, partition_date, holdings_library, context.log
        )

        # If debugging, run with a subset of the holdings
        debug_mode = context.op_config.get("debug_mode", False)
        if debug_mode:
            ishares_etf_holdings = ishares_etf_holdings[:10]

        # Update sentiment data if tickers are missing or data is out of date
        update_sentiment_data(
            sentiment_library,
            tickers=ishares_etf_holdings,
            logger=context.log,
            portfolio_date=partition_date,
        )
        # Produce the portfolio for the partition date.
        df_portfolio = produce_portfolio(
            partition_date,
            arctic_library=sentiment_library,
            tickers=ishares_etf_holdings,
            logger=context.log,
        )
        context.log.info(f"{partition_date}: position_df = \n{df_portfolio}")

        # TODO: Save portfolio to S3 using vbase API
        context.log.warning(
            "Saving portfolio to S3 and stamping with vBase is not yet implemented."
        )
        save_path = Path(OUTPUT_DIR + f"/{etf_ticker}")
        save_path.mkdir(parents=True, exist_ok=True)
        df_portfolio.to_csv(
            save_path / f"{etf_ticker}_smt_chg_pf_{partition_date}.csv", index=True
        )

        return df_portfolio

    except ValueError as e:
        context.log.error(str(e))
        return pd.DataFrame()


def debug_portfolio(date_str: str | None = None) -> None:
    """
    Materializes the portfolio asset for a specific date or today's date.

    Args:
        date_str: Optional date string in YYYY-MM-DD format. If None, uses today's date.
    """

    # Use provided date or today's date.
    partition_date = date_str or datetime.now(EASTERN_TZ).strftime("%Y-%m-%d")

    # Instantiate arctic_db resource
    arctic_db = arctic_db_resource(build_init_resource_context())

    for ticker in DEBUG_ETF_TICKERS:
        # Create a context for debugging.
        context = build_op_context(
            partition_key=partition_date,
            resources={"arctic_db": arctic_db},
            op_config={
                "etf_ticker_override": ticker,
                "debug_mode": True,
            },
        )
        # Get the holdings (call the asset function directly)
        holdings_library = ishares_etf_holdings_asset(context)

        # Use the R3000 sentiment dataset asset.
        sentiment_library = r3000_sentiment_dataset_asset(
            context, holdings_library=holdings_library
        )

        # Materialize the portfolio asset, passing the holdings
        portfolio_asset(
            context,
            holdings_library=holdings_library,
            sentiment_library=sentiment_library,
        )

        # Save the sentiment data to a csv file
        save_sentiment_data(
            output_dir=OUTPUT_DIR + f"/{ticker}",
            arctic_library=sentiment_library,
            dataset_date_str=partition_date,
            updated_sentiment_df=None,
            logger=context.log,
        )

        print_arcticdb_summary(arctic_db, context.log)
        print_arcticdb_symbol(
            symbol="sentimentNormalized",
            library="sentiment_features",
            arctic_object=arctic_db,
            logger=context.log,
        )


if __name__ == "__main__":
    # Run for today's date.
    debug_portfolio()

    # Run for a specific past date.
    debug_portfolio("2025-08-15")
    debug_portfolio("2025-08-14")
    debug_portfolio("2025-08-13")
