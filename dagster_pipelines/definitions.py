"""
The definitions for the Dagster Cloud pipeline.
"""

from dagster import Definitions

from dagster_pipelines.resources import arctic_db_resource, s3_resource
from dagster_pipelines.assets.sentiment_change_portfolio_asset import portfolio_asset
from dagster_pipelines.assets.etf_holdings_asset import ishares_etf_holdings_asset
from dagster_pipelines.assets.sentiment_dataset_asset import sentiment_dataset_asset
from dagster_pipelines.schedules.portfolio_schedule import portfolio_schedule

defs = Definitions(
    assets=[portfolio_asset, ishares_etf_holdings_asset, sentiment_dataset_asset],
    schedules=[portfolio_schedule],
    resources={
        "arctic_db": arctic_db_resource,
        "s3": s3_resource,
    },
)
