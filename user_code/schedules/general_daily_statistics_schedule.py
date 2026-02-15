from dagster import ScheduleDefinition
from jobs.general_daily_statistics.general_daily_statistics_job import general_daily_statistics_job

general_daily_statistics_schedule = ScheduleDefinition(
    name="general_daily_statistics_schedule",
    job=general_daily_statistics_job,
    cron_schedule="45 7 * * *",
    execution_timezone="Asia/Bishkek",
)