from dagster import ScheduleDefinition, define_asset_job

from portfolio_asset import portfolio_asset

# Define a job to materialize the spy_position asset
portfolio_job = define_asset_job("portfolio_job", selection=[portfolio_asset])

# Define a schedule to run the job at 3:50 PM ET on NYSE trading days
portfolio_schedule = ScheduleDefinition(
    job=portfolio_job,
    # Set the schedule to run at 3:50 PM ET on NYSE trading days.
    cron_schedule="50 15 * * 1-5",
    execution_timezone="America/New_York",
)
