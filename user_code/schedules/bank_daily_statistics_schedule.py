from dagster import ScheduleDefinition
from jobs.bank_daily_statistics.bank_daily_statistics_job import bank_daily_statistics_job

bank_daily_statistics_schedule = ScheduleDefinition(
    name="bank_daily_statistics_schedule",
    job=bank_daily_statistics_job,
    cron_schedule="30 8 * * *",
    execution_timezone="Asia/Bishkek",
)