"""
The definitions for the Dagster Cloud pipeline.
"""

from dagster import Definitions

from dagster_pipelines.assets.sentiment_change_portfolio_asset import portfolio_asset
from dagster_pipelines.assets.etf_holdings_asset import ishares_etf_holdings_asset
from dagster_pipelines.assets.sentiment_dataset_asset import sentiment_dataset_asset
from dagster_pipelines.schedules import portfolio_schedule

defs = Definitions(
    assets=[portfolio_asset, ishares_etf_holdings_asset, sentiment_dataset_asset],
    schedules=[portfolio_schedule],
)
