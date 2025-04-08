from dagster import Definitions

from dagster_pipelines.assets.portfolio_asset import portfolio_asset
from dagster_pipelines.schedules.portfolio_schedule import portfolio_schedule

defs = Definitions(
    assets=[portfolio_asset],
    schedules=[portfolio_schedule],
)
