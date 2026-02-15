from dagster import ScheduleDefinition
from jobs.text_projects_daily_statistics.text_projects_daily_statistics_job import text_projects_daily_statistics_job

text_projects_daily_statistics_schedule = ScheduleDefinition(
    name="text_projects_daily_statistics_schedule",
    job=text_projects_daily_statistics_job,
    cron_schedule="00 8 * * *",
    execution_timezone="Asia/Bishkek",
)