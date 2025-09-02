"""
Datetime utilities for timezone handling and conversions.
"""

from datetime import datetime, timedelta
import pytz
import pandas_market_calendars as mcal
import pandas as pd
from dagster_pipelines.config.constants import EASTERN_TZ


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

    # Check if the input date is a market day
    schedule = nyse.schedule(
        start_date=input_date - pd.Timedelta(days=5),
        end_date=input_date,  # Don't subtract 1 day - include the input date
    )

    if len(schedule) == 0:
        raise ValueError(f"No market days found in the past 5 days from {date_str}")

    # Make sure that if called today that we are before market close
    current_datetime = datetime.now(EASTERN_TZ)
    if input_date.date() == current_datetime.date():
        target_mkt_close = schedule["market_close"].iloc[-1] - timedelta(minutes=10)
        if current_datetime < ensure_timezone(
            datetime.combine(input_date.date(), target_mkt_close.time()), EASTERN_TZ
        ):
            return schedule["market_close"].iloc[-2]

    return schedule["market_close"].iloc[-1]
