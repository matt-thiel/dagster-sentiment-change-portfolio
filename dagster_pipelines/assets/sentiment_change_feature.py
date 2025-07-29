"""
Module with a function to get the sentiment change feature for a given partition date.
"""

from datetime import timedelta, datetime
import pandas as pd
import numpy as np
import pandas_market_calendars as mcal

from dagster_pipelines.config.constants import NULL_CHANGE_WINDOW

# Disable too many arguments and locals to allow for generality
# pylint: disable=too-many-arguments, disable=too-many-locals


def get_sentiment_change_feature(
    arctic_library: object,
    sentiment_symbol: str,
    partition_date: datetime,
    tickers: list[str],
    change_period: int = 1,
    lag_periods: int = 1,
    remove_null_changes: bool = True,
    null_change_threshold: float = 0.01,
) -> pd.DataFrame:
    """
    Get the sentiment change feature for a given symbol and partition date.

    Args:
        arctic_library: ArcticDB library instance.
        sentiment_symbol: The symbol to get the sentiment change feature for.
        partition_date: The partition date to get the sentiment change feature for.
        tickers: The tickers to get the sentiment change feature for.
        change_period: The number of periods to change the sentiment feature by.
        lag_periods: The number of periods to lag the sentiment change feature by.

    Returns:
        pd.DataFrame: The sentiment change feature for the given symbol and partition date.
    """
    nyse = mcal.get_calendar("NYSE")

    # Number of rows to read from database
    periods = change_period + lag_periods + 3
    # Conservative lookback for NYSE schedule dates
    lookback_window = max(periods + 7, NULL_CHANGE_WINDOW)

    # Get enough days to ensure we have periods trading days in schedule
    start_date = partition_date - timedelta(days=lookback_window)
    schedule = nyse.schedule(start_date=start_date, end_date=partition_date)

    # Only get dates before the current partition date
    schedule = schedule[schedule["market_close"] < partition_date]

    if remove_null_changes:
        date_range = schedule["market_close"][-lookback_window:].to_list()
    else:
        date_range = schedule["market_close"][-periods:].to_list()

    if len(date_range) < periods:
        raise ValueError(
            f"Not enough trading days in the date range for {sentiment_symbol} and {partition_date}"
        )

    data_read = arctic_library.read(
        symbol=sentiment_symbol,
        date_range=date_range,
        columns=tickers,
    ).data

    # Calculate sentiment change removing infinite changes
    change_feature = data_read.pct_change(periods=change_period, fill_method=None)
    change_feature = change_feature.shift(lag_periods).replace(
        [np.inf, -np.inf], np.nan
    )

    # Remove tickers from universe if sentiment doesn't change over the lookback window
    if remove_null_changes:
        change_feature = change_feature.loc[
            :, change_feature.abs().sum(axis=0) > null_change_threshold
        ]

    # Only care about the most recent row for the current portfolio
    return change_feature.iloc[[-1]]
