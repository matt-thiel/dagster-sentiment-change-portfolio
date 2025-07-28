"""
Portfolio production logic for sentiment-based trading strategies.

This module contains the core logic for generating portfolio positions based on
sentiment data from ArcticDB. The strategy uses sentiment change features to
identify long and short positions, implementing a market-neutral approach with
equal weighting within each direction.
"""

from datetime import datetime, timedelta
import pandas as pd
import pandas_market_calendars as mcal
import numpy as np

from dagster_pipelines.utils.datetime_utils import ensure_timezone
from dagster_pipelines.config.constants import EASTERN_TZ
from dagster_pipelines.assets.sentiment_change_feature import (
    get_sentiment_change_feature,
)

# Disable too many locals due to function complexity
# pylint: disable=too-many-locals


def produce_portfolio(
    portfolio_date: str,
    arctic_library: object,
    tickers: list,
    logger: object,
) -> pd.DataFrame:
    """
    Produces a sentiment-based portfolio with long/short positions
      based on sentiment change features.

    This function implements a market-neutral strategy that:
    1. Loads sentiment change data from ArcticDB
    2. Ranks tickers by sentiment change features
    3. Identifies top (long) and bottom (short) deciles
    4. Creates equal-weighted positions within each direction

    Args:
        portfolio_date (str): The date for which to produce the portfolio in 'YYYY-MM-DD' format.
        arctic_library (object): ArcticDB library instance containing sentiment datasets.
        tickers (list): List of ticker symbols to consider for portfolio construction.
        logger (object): Logger object for logging messages and warnings.

    Returns:
        pd.DataFrame: DataFrame containing portfolio positions with columns:
            - 'sym': Ticker symbol
            - 'wt': Position weight (positive for long, negative for short, 0 for no position)

    Raises:
        ValueError: If the portfolio date is not a trading day, if current time is before
                   target time (10 minutes before market close), or if required data is unavailable.
    """

    # Get the NYSE schedule for the portfolio date.
    schedule = mcal.get_calendar("NYSE").schedule(
        start_date=portfolio_date, end_date=portfolio_date
    )
    if schedule.empty:
        logger.warning(f"No trading on {portfolio_date}.")
        raise ValueError(f"No trading on {portfolio_date}.")

    # Calculate the target time (10 minutes before market close).
    target_time = schedule.iloc[0]["market_close"] - timedelta(minutes=10)
    logger.info(
        f"Market close time for {portfolio_date}: "
        f"{schedule.iloc[0]['market_close'].strftime('%H:%M')}"
    )
    logger.info(f"Target time for price: {target_time.strftime('%H:%M')}")

    # Check if we're dealing with current date and if we're past the target time.
    portfolio_datetime = datetime.strptime(portfolio_date, "%Y-%m-%d")
    portfolio_datetime = ensure_timezone(portfolio_datetime, EASTERN_TZ)
    current_datetime = datetime.now(EASTERN_TZ)
    is_current_date = portfolio_datetime.date() == current_datetime.date()
    logger.info(
        f"Current time (Eastern): {current_datetime.strftime('%Y-%m-%d %H:%M %Z')}"
    )
    logger.info(f"Is current date: {is_current_date}")

    if is_current_date and current_datetime < ensure_timezone(
        datetime.combine(portfolio_datetime.date(), target_time.time()), EASTERN_TZ
    ):
        error_message = (
            f"Current time is before target time ({target_time.strftime('%H:%M')})."
        )
        logger.error(error_message)
        raise ValueError(error_message)

    selected_features = [
        "sentimentNormalized_1d_change_1d_lag",
    ]

    feature_dfs = {}
    signals = []

    for feature_name in selected_features:
        df_sentiment_feature = get_sentiment_change_feature(
            arctic_library=arctic_library,
            sentiment_symbol="sentimentNormalized",
            partition_date=portfolio_datetime,
            tickers=tickers,
            lag_periods=1,
            change_period=1,
        )

        closest_date = df_sentiment_feature.index.asof(portfolio_datetime)
        logger.info(
            "Portfolio date: %s \nFeature date: %s", portfolio_datetime, closest_date
        )
        todays_sentiment = df_sentiment_feature.loc[closest_date]

        if todays_sentiment.isna().any():
            logger.warning(
                "NaN values in sentiment feature for %s on %s",
                feature_name,
                portfolio_datetime,
            )

        df_feature = todays_sentiment.rank(method="first")
        feature_dfs[feature_name] = (
            pd.qcut(df_feature, 10, labels=False, duplicates="raise") + 1
        )

    for ticker in tickers:
        long_mask = []
        short_mask = []
        for _, feature_df in feature_dfs.items():
            long_mask.append(feature_df.loc[ticker] == 10)
            short_mask.append(feature_df.loc[ticker] == 1)
        if all(long_mask):
            signals.append((ticker, 1))
        elif all(short_mask):
            signals.append((ticker, -1))
        else:
            signals.append((ticker, np.nan))

    # equal weight the signals
    position_df = pd.DataFrame(signals, columns=["sym", "wt"])

    # Count valid positions
    long_positions = position_df["wt"] == 1
    short_positions = position_df["wt"] == -1

    # Equal weight Market-neutral: 1/number_of_positions for each direction
    if long_positions.sum() > 0:
        position_df.loc[long_positions, "wt"] = 1.0 / long_positions.sum()

    if short_positions.sum() > 0:
        position_df.loc[short_positions, "wt"] = -1.0 / short_positions.sum()

    # Set NaN (no position) to 0
    position_df["wt"] = position_df["wt"].fillna(0)

    return position_df
