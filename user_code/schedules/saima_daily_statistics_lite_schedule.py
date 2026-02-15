from dagster import ScheduleDefinition
from jobs.saima_daily_statistics_lite.saima_daily_statistics_lite_job import saima_daily_statistics_lite_job

saima_daily_statistics_lite_schedule = ScheduleDefinition(
    name="saima_daily_statistics_lite_schedule",
    job=saima_daily_statistics_lite_job,
    cron_schedule="20 8 * * *",
    execution_timezone="Asia/Bishkek",
)