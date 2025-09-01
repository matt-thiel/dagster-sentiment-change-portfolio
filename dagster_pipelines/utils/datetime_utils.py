"""
Datetime utilities for timezone handling and conversions.
"""

from datetime import datetime
import pytz
import pandas_market_calendars as mcal
import pandas as pd


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
    input_date = pd.to_datetime(date_str).date()

    # Check if the input date is a market day
    schedule = nyse.schedule(
        start_date=input_date - pd.Timedelta(days=5),
        end_date=input_date,  # Don't subtract 1 day - include the input date
    )

    if len(schedule) == 0:
        raise ValueError(f"No market days found in the past 5 days from {date_str}")

    return schedule["market_close"].iloc[-1]
