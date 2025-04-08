from dagster import Definitions

from portfolio_asset import portfolio_asset
from schedules import portfolio_schedule

defs = Definitions(
    assets=[portfolio_asset],
    schedules=[portfolio_schedule],
)
