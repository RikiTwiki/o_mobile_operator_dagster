from dagster import ScheduleDefinition
from jobs.saima_daily_statistics.saima_daily_statistics_job import saima_daily_statistics_job

saima_daily_statistics_schedule = ScheduleDefinition(
    name="saima_daily_statistics_schedule",
    job=saima_daily_statistics_job,
    cron_schedule="15 8 * * *",
    execution_timezone="Asia/Bishkek",
)