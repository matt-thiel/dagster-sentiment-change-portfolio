from datetime import datetime
import pytz

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
    elif dt.tzinfo != timezone:
        # If the datetime is aware but not in the specified timezone, convert it
        return dt.astimezone(timezone)
    else:
        # Already in the specified timezone
        return dt