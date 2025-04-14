"""
This asset is used to generate a position for the SPY ETF.
"""

import os
import pprint
from datetime import datetime

import boto3
from dagster import DailyPartitionsDefinition, asset, build_op_context
from dotenv import load_dotenv
from vbase import (
    ForwarderCommitmentService,
    VBaseClient,
    VBaseDataset,
    VBaseStringObject,
)

from .portfolio_producer import produce_portfolio

# The name of the portfolio set (collection).
# This is the vBase set (collection) that receive the object commitments (stamps)
# for the individual portfolios.
PORTFOLIO_NAME = "TestPortfolio"

# Define a daily partition for portfolio rebalancing.
# The portfolio rebalances daily starting from 2025-01-01.
partitions_def = DailyPartitionsDefinition(start_date="2025-01-01")

# The vBase forwarder URL for making commitments via the vBase forwarder.
VBASE_FORWARDER_URL = "https://dev.api.vbase.com/forwarder-test/"


@asset(partitions_def=partitions_def)
def portfolio_asset(context):
    """
    This asset is used to generate a position for the SPY ETF.
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
        # Produce the portfolio for the partition date.
        df_portfolio = produce_portfolio(partition_date, logger=context.log)
        context.log.info(f"{partition_date}: position_df = \n{df_portfolio}")

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
        filename = f"portfolio--{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
        s3_client = boto3.client("s3")
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


def debug_portfolio(date_str: str = None) -> None:
    """
    Materialize the portfolio asset for a specific date or today's date.

    Args:
        date_str: Optional date string in YYYY-MM-DD format. If None, uses today's date.
    """
    # Use provided date or today's date.
    partition_date = date_str or datetime.now().strftime("%Y-%m-%d")

    # Create a context for debugging.
    context = build_op_context(partition_key=partition_date)

    # Materialize the asset.
    portfolio_asset(context)


if __name__ == "__main__":
    # Run for today's date.
    debug_portfolio()

    # Run for a specific past date.
    debug_portfolio("2025-04-04")
