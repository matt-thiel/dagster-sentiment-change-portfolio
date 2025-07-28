"""
Module with a function to get the sentiment change feature for a given partition date.
"""

from datetime import timedelta
import pandas as pd
import numpy as np
import pandas_market_calendars as mcal

# Disable too many arguments to allow for generality
# pylint: disable=too-many-arguments


def get_sentiment_change_feature(
    arctic_library: object,
    sentiment_symbol: str,
    partition_date: str,
    tickers: list[str],
    change_period: int = 1,
    lag_periods: int = 1,
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

    periods = change_period + lag_periods + 2

    # Get enough days to ensure we have n trading days
    start_date = partition_date - timedelta(
        days=periods + 7
    )  # Buffer to ensure enough trading days
    schedule = nyse.schedule(start_date=start_date, end_date=partition_date)

    date_range = schedule.index[-periods:].to_list()

    if len(date_range) < periods:
        raise ValueError(
            f"Not enough trading days in the date range for {sentiment_symbol} and {partition_date}"
        )

    data_read = arctic_library.read(
        symbol=sentiment_symbol,
        date_range=date_range,
        columns=tickers,
    ).data

    change_feature = data_read.pct_change(periods=change_period, fill_method=None)
    change_feature = change_feature.shift(lag_periods).replace(
        [np.inf, -np.inf], np.nan
    )

    return change_feature.iloc[[-1]]
