"""
This module contains the logic for producing portfolio positions.
"""

from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import pandas_market_calendars as mcal
import yfinance as yf


def produce_portfolio(
    portfolio_date: str, logger: Optional[object] = None
) -> pd.DataFrame:
    """
    Produce the portfolio position for SPY based on the partition date.

    Args:
        portfolio_date: The date for which to produce the portfolio in YYYY-MM-DD format.
        logger: Optional logger object for logging messages. If None, no logging is performed.

    Returns:
        A DataFrame containing the position with columns ['sym', 'wt'].

    Raises:
        ValueError: If the partition date is not a trading day or if data is not available.
    """

    # Define the NYSE calendar.
    nyse = mcal.get_calendar("NYSE")
    if logger:
        logger.info(f"Using NYSE calendar for {portfolio_date}")

    # Get the schedule for the partition date.
    schedule = nyse.schedule(start_date=portfolio_date, end_date=portfolio_date)

    if schedule.empty:
        if logger:
            logger.warning(f"No trading on {portfolio_date}.")
        raise ValueError(f"No trading on {portfolio_date}.")

    # Get the market close time for the partition date.
    market_close = schedule.iloc[0]["market_close"]
    if logger:
        logger.info(
            f"Market close time for {portfolio_date}: {market_close.strftime('%H:%M')}"
        )

    # Calculate the time 15 minutes before market close.
    target_time = market_close - timedelta(minutes=15)
    if logger:
        logger.info(f"Target time for price: {target_time.strftime('%H:%M')}")

    # Format the partition date and target time for yfinance.
    partition_datetime = datetime.strptime(portfolio_date, "%Y-%m-%d")
    target_datetime = datetime.combine(partition_datetime.date(), target_time.time())

    # For current date, we need to check if we're past the target time.
    current_datetime = datetime.now()
    is_current_date = partition_datetime.date() == current_datetime.date()
    if logger:
        logger.info(f"Current time: {current_datetime.strftime('%Y-%m-%d %H:%M')}")
        logger.info(f"Is current date: {is_current_date}")

    if is_current_date and current_datetime < target_datetime:
        if logger:
            logger.warning(
                f"Current time is before target time ({target_time.strftime('%H:%M')})."
            )
        raise ValueError(
            f"Current time is before target time ({target_time.strftime('%H:%M')})."
        )

    # Fetch intraday data for the partition date.
    spy_ticker = yf.Ticker("SPY")
    if logger:
        logger.info("Fetching SPY data...")

    if is_current_date:
        # For current date, get data up to now.
        spy_data = spy_ticker.history(
            start=portfolio_date, end=current_datetime, interval="15m"
        )
        if logger:
            logger.info(f"Fetched {len(spy_data)} intraday records up to current time")
    else:
        # For past dates, get data for the entire day.
        spy_data = spy_ticker.history(
            start=portfolio_date, end=target_datetime, interval="15m"
        )
        if logger:
            logger.info(
                f"Fetched {len(spy_data)} intraday records for {portfolio_date}"
            )

    if spy_data.empty:
        if logger:
            logger.error(f"No data available for SPY on {portfolio_date}.")
        raise ValueError(f"No data available for SPY on {portfolio_date}.")

    current_price = spy_data["Close"].iloc[-1]
    if logger:
        logger.info(f"Current price: ${current_price:.2f}")

    # Get the prior day's close before the partition date.
    # This is the price at the close of the day before the partition date.
    prior_record = spy_ticker.history(
        start=datetime.strptime(portfolio_date, "%Y-%m-%d") - timedelta(days=10),
        end=portfolio_date,
        interval="1d",
    ).iloc[-1]
    prior_price = prior_record["Close"]
    if logger:
        logger.info(
            f"Prior day close: ${prior_price:.2f} (from {prior_record.name.strftime('%Y-%m-%d')})"
        )

    # Calculate the return from prior close to current price.
    price_return = current_price / prior_price - 1
    if logger:
        logger.info(f"Price return: {price_return:.2%}")

    # Determine position weight.
    weight = 1 if price_return > 0 else -1
    if logger:
        logger.info(f"Position weight: {weight}")

    # Create a DataFrame for the position.
    df_portfolio = pd.DataFrame({"sym": ["SPY"], "wt": [weight]})
    if logger:
        logger.info(f"Generated portfolio:\n{df_portfolio}")

    return df_portfolio
