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
import pprint
from datetime import datetime
from arcticdb.version_store.library import Library

from dagster import (
    DailyPartitionsDefinition,
    asset,
    build_op_context,
    AssetIn,
    build_init_resource_context,
    AssetExecutionContext,
)
from dotenv import load_dotenv
from vbase import (
    ForwarderCommitmentService,
    VBaseClient,
    VBaseDataset,
    VBaseStringObject,
)

from dagster_pipelines.utils.database_utils import print_arcticdb_summary
from dagster_pipelines.config.constants import EASTERN_TZ, VBASE_FORWARDER_URL, PORTFOLIO_NAME
from dagster_pipelines.assets.sentiment_change_portfolio_producer import (
    produce_portfolio,
)
from dagster_pipelines.resources import s3_resource, arctic_db_resource
from dagster_pipelines.assets.etf_holdings_asset import ishares_etf_holdings_asset
from dagster_pipelines.assets.sentiment_dataset_asset import sentiment_dataset_asset
from dagster_pipelines.assets.dataset_updater import update_sentiment_data


# Define a daily partition for portfolio rebalancing.
# The portfolio rebalances daily starting from 2025-01-01.
partitions_def = DailyPartitionsDefinition(start_date="2025-01-01")



# pylint: disable=too-many-locals
@asset(
    partitions_def=partitions_def,
    ins={
        "ishares_etf_holdings": AssetIn("ishares_etf_holdings_asset"),
        "sentiment_library": AssetIn("sentiment_dataset_asset"),
    },
    required_resource_keys={"arctic_db", "s3"},
)
def portfolio_asset(
    context: AssetExecutionContext,
    ishares_etf_holdings: list,
    sentiment_library: Library,
) -> None:
    """
    Generates and saves a portfolio for the SPY ETF for a given partition date, 
      stamps it in vBase, and uploads to S3.

    Args:
        context: Dagster asset context with partition key and resources.
        ishares_etf_holdings (list): List of ETF holding tickers.
        sentiment_library (Library): ArcticDB library with sentiment data.

    Returns:
        None

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
        "S3_BUCKET",
        "S3_FOLDER",
    ]
    for setting in required_settings:
        if setting not in os.environ:
            raise ValueError(f"{setting} environment variable is not set.")

    # Get the current partition date.
    partition_date = context.asset_partition_key_for_output()
    context.log.info("Starting portfolio generation for %s", partition_date)

    try:
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
        # return

        # pylint: disable=fixme
        # TODO: Saving should be idempotent within some time window.
        # TODO: Do not save if the portfolio is the same as the previous portfolio within a window.
        # Save the position to a CSV file in an S3 bucket.
        # Use the boto3 library to save the file to the bucket
        # and folder specified in the environment variables.
        # Create the filename using a format: portfolio--2024-12-11_07-58-46.csv
        # recognized by vBase validation tools and the current timestamp.
        bucket = os.environ["S3_BUCKET"]
        folder = os.environ["S3_FOLDER"]
        current_time = datetime.now(EASTERN_TZ)
        filename = f"portfolio--{current_time.strftime('%Y-%m-%d_%H-%M-%S')}.csv"
        s3_client = context.resources.s3
        context.log.info(f"Saving portfolio to s3://{bucket}/{folder}/{filename}")
        body = df_portfolio.to_csv(index=False)
        context.log.info(f"{partition_date}: body = \n{body}")
        s3_client.put_object(
            Bucket=bucket,
            Key=f"{folder}/{filename}",
            Body=body,
        )

        # Make a vBase stamp of the portfolio.
        vbc = VBaseClient(
            ForwarderCommitmentService(
                forwarder_url=VBASE_FORWARDER_URL,
                api_key=os.environ["VBASE_API_KEY"],
                private_key=os.environ["VBASE_COMMITMENT_SERVICE_PRIVATE_KEY"],
            )
        )
        # Create the dataset object, if necessary.
        # This operation is idempotent.
        ds = VBaseDataset(vbc, PORTFOLIO_NAME, VBaseStringObject)
        # Add a record to the dataset.
        # TODO: Stamping should be idempotent within some time window.
        # TODO: Do not stamp if the portfolio is the same as the previous portfolio within a window.
        receipt = ds.add_record("body")
        context.log.info(f"ds.add_record() receipt:\n{pprint.pformat(receipt)}")

    except ValueError as e:
        context.log.error(str(e))


def debug_portfolio(date_str: str | None = None) -> None:
    """
    Materializes the portfolio asset for a specific date or today's date.

    Args:
        date_str: Optional date string in YYYY-MM-DD format. If None, uses today's date.
    """
    # Use provided date or today's date.
    partition_date = date_str or datetime.now(EASTERN_TZ).strftime("%Y-%m-%d")

    # Instantiate s3 resource
    s3 = s3_resource(build_init_resource_context())

    # Instantiate arctic_db resource, passing s3 as a required resource
    arctic_db = arctic_db_resource(build_init_resource_context(resources={"s3": s3}))

    # Create a context for debugging.
    context = build_op_context(
        partition_key=partition_date, resources={"s3": s3, "arctic_db": arctic_db}
    )

    # Get the holdings (call the asset function directly)
    ishares_etf_holdings = ishares_etf_holdings_asset(context)
    sentiment_library = sentiment_dataset_asset(
        context, ishares_etf_holdings=ishares_etf_holdings
    )

    # Materialize the portfolio asset, passing the holdings
    portfolio_asset(
        context,
        ishares_etf_holdings=ishares_etf_holdings,
        sentiment_library=sentiment_library,
    )

    print_arcticdb_summary(arctic_db, context.log)


if __name__ == "__main__":
    # Run for today's date.
    debug_portfolio()

    # Run for a specific past date.
    debug_portfolio("2025-04-04")
