"""
This asset is used to generate a position for the SPY ETF.
"""

from datetime import datetime, timedelta

import pandas as pd
import pandas_market_calendars as mcal
import yfinance as yf
from dagster import DailyPartitionsDefinition, asset, build_op_context

# Define a daily partition for portfolio rebalancing.
partitions_def = DailyPartitionsDefinition(start_date="2025-01-01")


@asset(partitions_def=partitions_def)
def portfolio_asset(context):
    # Get the current partition date.
    partition_date = context.asset_partition_key_for_output()

    # Define the NYSE calendar.
    nyse = mcal.get_calendar("NYSE")

    # Get the schedule for the partition date.
    schedule = nyse.schedule(start_date=partition_date, end_date=partition_date)

    if schedule.empty:
        context.log.info(f"No trading on {partition_date}. Skipping materialization.")
        return

    # Get the latest intraday price.
    spy_ticker = yf.Ticker("SPY")
    spy_data = spy_ticker.history(period="1d", interval="15m")
    current_price = spy_data["Close"].iloc[-1]

    # Get the prior day's close.
    spy_data = spy_ticker.history(period="2d", interval="1d")
    prior_price = spy_data["Close"].iloc[-2]

    # Calculate the return from prior close to current price.
    price_return = current_price / prior_price - 1

    # Determine position weight.
    weight = 1 if price_return > 0 else -1

    # Create a DataFrame for the position.
    position_df = pd.DataFrame({"sym": ["SPY"], "wt": [weight]})
    context.log.info(f"Position for {partition_date}:\n{position_df}")

    # Save the position to a CSV file.
    """
    output_path = f'spy_position_{partition_date}.csv'
    position_df.to_csv(output_path, index=False)
    context.log.info(f"Position for {partition_date} saved to {output_path}.")
    """


if __name__ == "__main__":
    """Main entry point for debugging."""

    # Create a context for debugging with today's partition.
    context = build_op_context(partition_key=datetime.now().strftime("%Y-%m-%d"))

    # Run the asset.
    portfolio_asset(context)
