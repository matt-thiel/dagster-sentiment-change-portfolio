"""
This asset is used to generate a position for the SPY ETF.
"""

import os
import pprint
from datetime import datetime, timedelta

import boto3
import pandas as pd
import pandas_market_calendars as mcal
import yfinance as yf
from dagster import DailyPartitionsDefinition, asset, build_op_context
from dotenv import load_dotenv
from vbase import VBaseClient, VBaseDataset, VBaseStringObject

# Define the name of the portfolio set (collection).
# This is the vBase set (collection) that receive the object commitments (stamps)
# for the individual portfolios.
PORTFOLIO_NAME = "TestPortfolio"

# Define a daily partition for portfolio rebalancing.
partitions_def = DailyPartitionsDefinition(start_date="2025-01-01")


@asset(partitions_def=partitions_def)
def portfolio_asset(context):
    """
    This asset is used to generate a position for the SPY ETF.
    """

    # Load the environment variables and check that settings are defined.
    load_dotenv()
    # Check that all the required settings are defined:
    required_settings = [
        "VBASE_COMMITMENT_SERVICE_CLASS",
        "VBASE_FORWARDER_URL",
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
    bucket = os.environ["S3_BUCKET"]
    folder = os.environ["S3_FOLDER"]

    # Get the current partition date.
    partition_date = context.asset_partition_key_for_output()
    context.log.info("partition_date = %s", partition_date)

    # Define the NYSE calendar.
    nyse = mcal.get_calendar("NYSE")

    # Get the schedule for the partition date.
    schedule = nyse.schedule(start_date=partition_date, end_date=partition_date)

    if schedule.empty:
        context.log.info(f"No trading on {partition_date}. Skipping materialization.")
        return

    # Get the market close time for the partition date.
    market_close = schedule.iloc[0]["market_close"]

    # Calculate the time 15 minutes before market close.
    target_time = market_close - timedelta(minutes=15)

    # Format the partition date and target time for yfinance.
    partition_datetime = datetime.strptime(partition_date, "%Y-%m-%d")
    target_datetime = datetime.combine(partition_datetime.date(), target_time.time())

    # For current date, we need to check if we're past the target time.
    current_datetime = datetime.now()
    is_current_date = partition_datetime.date() == current_datetime.date()

    if is_current_date and current_datetime < target_datetime:
        context.log.info(
            f"Current time is before target time ({target_time.strftime('%H:%M')}). Waiting for data."
        )
        return

    # Fetch intraday data for the partition date.
    spy_ticker = yf.Ticker("SPY")

    if is_current_date:
        # For current date, get data up to now.
        spy_data = spy_ticker.history(
            start=partition_date, end=current_datetime, interval="15m"
        )
    else:
        # For past dates, get data for the entire day.
        spy_data = spy_ticker.history(
            start=partition_date, end=target_datetime, interval="15m"
        )

    if spy_data.empty:
        context.log.error(f"No data available for SPY on {partition_date}.")
        return

    current_price = spy_data["Close"].iloc[-1]
    context.log.info(
        f"current_datetime = {target_datetime}, current_price = {current_price}"
    )

    # Get the prior day's close before the partition date.
    # This is the price at the close of the day before the partition date.
    prior_record = spy_ticker.history(
        start=datetime.strptime(partition_date, "%Y-%m-%d") - timedelta(days=10),
        end=partition_date,
        interval="1d",
    ).iloc[-1]
    prior_price = prior_record["Close"]
    context.log.info(
        f"prior_datetime = {prior_record.name}, prior_price = {prior_price}"
    )

    # Calculate the return from prior close to current price.
    price_return = current_price / prior_price - 1

    # Determine position weight.
    weight = 1 if price_return > 0 else -1

    # Create a DataFrame for the position.
    position_df = pd.DataFrame({"sym": ["SPY"], "wt": [weight]})
    context.log.info(f"{partition_date}: position_df = \n{position_df}")

    # Save the position to a CSV file in an S3 bucket.
    # Use the boto3 library to save the file to the bucket
    # and folder specified in the environment variables.
    # Create the filename using a format: portfolio--2024-12-11_07-58-46.csv
    # recognized by vBase validation tools and the current timestamp.
    filename = f"portfolio--{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
    s3_client = boto3.client("s3")
    context.log.info(f"Saving portfolio to s3://{bucket}/{folder}/{filename}")
    body = position_df.to_csv(index=False)
    context.log.info(f"{partition_date}: body = \n{body}")
    s3_client.put_object(
        Bucket=bucket,
        Key=f"{folder}/{filename}",
        Body=body,
    )

    # Make a vBase stamp of the portfolio.
    vbc = VBaseClient.create_instance_from_env()
    # Create the dataset object, if necessary.
    # This operation is idempotent.
    ds = VBaseDataset(vbc, PORTFOLIO_NAME, VBaseStringObject)
    # Add a record to the dataset.
    receipt = ds.add_record("body")
    context.log.info(f"ds.add_record() receipt:\n{pprint.pformat(receipt)}")


if __name__ == "__main__":
    """Main entry point for debugging."""

    # Create a context for debugging with today's partition and run the asset.
    context = build_op_context(partition_key=datetime.now().strftime("%Y-%m-%d"))
    portfolio_asset(context)

    # Create a context for debugging with a past partition and run the asset.
    context = build_op_context(partition_key="2025-04-04")
    portfolio_asset(context)
