from dagster import ScheduleDefinition
from jobs.csf_daily_statistics.csf_daily_statistics_job import csf_daily_statistics_job

csf_daily_statistics_schedule = ScheduleDefinition(
    name="csf_daily_statistics_schedule",
    job=csf_daily_statistics_job,
    cron_schedule="20 8 * * *",
    execution_timezone="Asia/Bishkek",
)