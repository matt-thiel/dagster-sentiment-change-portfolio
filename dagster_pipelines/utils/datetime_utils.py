"""
Datetime utilities for timezone handling and conversions.
"""

from datetime import datetime, timedelta
import pytz
from pandas.tseries.offsets import CustomBusinessDay
import pandas_market_calendars as mcal
import pandas as pd
from dagster_pipelines.config.constants import EASTERN_TZ, SENTIMENT_TIME_PADDING


def ensure_timezone(dt: datetime, timezone: pytz.timezone) -> datetime:
    """
    Ensures a datetime object is timezone-aware and in the specified timezone.

    Args:
        dt (datetime): The datetime object to check or convert.
        timezone (pytz.timezone): The target timezone.

    Returns:
        datetime: A timezone-aware datetime object in the specified timezone.
    """
    if dt.tzinfo is None:
        # If the datetime is naive, localize it to the specified timezone
        return timezone.localize(dt)
    if dt.tzinfo != timezone:
        # If the datetime is aware but not in the specified timezone, convert it
        return dt.astimezone(timezone)
    # Already in the specified timezone
    return dt


def get_market_day_from_date(date_str: str) -> datetime:
    """
    Gets the most recent market day relative to a date string.

    If the input date is a market day, returns that date.
    If the input date is a weekend or holiday, returns the previous closest market day.

    Args:
        date_str (str): Date string in 'YYYY-MM-DD' format.

    Returns:
        datetime: The most recent market day as a timezone-aware datetime object.
    """
    nyse = mcal.get_calendar("NYSE")
    input_date = pd.to_datetime(date_str)

    # Get the recent market days.
    schedule = nyse.schedule(
        start_date=input_date - pd.Timedelta(days=10),
        end_date=input_date,  # Don't subtract 1 day - include the input date
    )
    if len(schedule) == 0:
        raise ValueError(f"No market days found in the past 10 days from {date_str}")

    # If the last market day is after the current time, drop it.
    # We want to get the last completed market day
    # prior to the current time.
    current_datetime = datetime.now(EASTERN_TZ)
    if current_datetime < schedule["market_close"].iloc[-1]:
        schedule = schedule.iloc[:-1]

    # Return the last market close prior to the input date.
    return ensure_timezone(schedule["market_close"].iloc[-1], EASTERN_TZ)


def get_market_offset(days: int) -> pd.offsets.CustomBusinessDay:
    """
    Creates a market offset using the NYSE calendar.
    """
    nyse = mcal.get_calendar("NYSE")
    return CustomBusinessDay(n=days, calendar=nyse)


def get_timedelta_market_days(
    start_date: datetime, end_date: datetime, prune_end_date: bool = False
) -> int:
    """
    Gets the number of market days between two dates.
    """
    nyse = mcal.get_calendar("NYSE")
    schedule = nyse.schedule(start_date=start_date, end_date=end_date)
    schedule = schedule.loc[schedule.index.date != start_date.date()]

    if prune_end_date:
        schedule = schedule[
            schedule["market_close"]
            <= end_date + pd.Timedelta(minutes=SENTIMENT_TIME_PADDING)
        ]

    return len(schedule)
