"""
Portfolio utilities for sentiment change portfolio dataset.
"""

from pathlib import Path
import pandas as pd
import pandas_market_calendars as mcal
from dagster_pipelines.utils.datetime_utils import ensure_timezone
from dagster_pipelines.config.constants import EASTERN_TZ
from dagster_pipelines.utils.database_utils import compare_files_to_timestamp


def save_portfolio_data(
    output_dir: str,
    portfolio_df: pd.DataFrame,
    dataset_timestamp: str,
    logger: object,
    overwrite: bool = False,
) -> None:
    """
    Save portfolio data to a CSV file with proper file management.

    This function handles the saving of sentiment change portfolio data to CSV
    files. It includes checks for existing files, directory creation, and
    proper logging of the save operation.

    The function performs the following operations:
    1. Validates that portfolio data exists
    2. Checks for existing files if overwrite is disabled
    3. Creates the output directory if it doesn't exist
    4. Saves the portfolio data to a timestamped CSV file
    5. Logs the save operation

    Args:
        output_dir: The directory path where the portfolio data CSV file
            will be saved. The directory will be created if it doesn't exist.
        portfolio_df: The portfolio DataFrame containing sentiment change
            analysis results to be saved.
        dataset_timestamp: The timestamp string in format "YYYYMMDDHHMMSS"
            that will be used in the filename.
        logger: The logger object for recording information, warnings, and
            errors during the save operation.
        overwrite: If True, overwrite existing portfolio data files.
            If False, skip saving if a file already exists for the current
            timestamp (within 10 minutes).

    Returns:
        None

    Raises:
        OSError: If there are issues creating the output directory or
            writing the CSV file.
        Exception: If the portfolio DataFrame cannot be saved to CSV format.

    Example:
        >>> save_portfolio_data(
        ...     output_dir="/path/to/output",
        ...     portfolio_df=my_portfolio_df,
        ...     dataset_timestamp="2025-01-01",
        ...     logger=my_logger,
        ...     overwrite=False
        ... )
    """
    if portfolio_df.empty:
        logger.warning("No portfolio data to save, skipping save...")
        return

    if not overwrite and compare_files_to_timestamp(
        output_dir, dataset_timestamp, "sentiment_change_portfolio_", 10
    ):
        logger.info(
            "Sentiment change portfolio already exists for this hour, "
            "skipping save..."
        )
        return

    nyse = mcal.get_calendar("NYSE")
    schedule = nyse.schedule(
        start_date=dataset_timestamp,
        end_date=dataset_timestamp,  # Don't subtract 1 day - include the input date
    )

    #if schedule.empty:
    market_close_for_date = ensure_timezone(schedule.iloc[0]["market_close"], EASTERN_TZ)
    market_close_for_date = market_close_for_date.strftime("%Y%m%d%H%M%S")

    save_path = Path(output_dir)
    save_path.mkdir(parents=True, exist_ok=True)
    portfolio_df.to_csv(
        save_path / f"sentiment_change_portfolio_{market_close_for_date}.csv", index=True
    )
    logger.info(
        "Sentiment change portfolio saved to %s",
        save_path / f"sentiment_change_portfolio_{market_close_for_date}.csv",
    )
