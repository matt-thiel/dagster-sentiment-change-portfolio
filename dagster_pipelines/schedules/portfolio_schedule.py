"""
The schedule for the portfolio asset.
"""

from dagster import ScheduleDefinition, define_asset_job

#from dagster_pipelines.assets.portfolio_asset import portfolio_asset
from dagster_pipelines.assets.sentiment_change_portfolio_asset import portfolio_asset

# Define a schedule to run the job at 3:50 PM ET on NYSE trading days
portfolio_schedule = ScheduleDefinition(
    job=define_asset_job("portfolio_job", selection=[portfolio_asset]),
    # Set the schedule to run at 3:50 PM ET on NYSE trading days.
    cron_schedule="50 15 * * 1-5",
    execution_timezone="America/New_York",
)
