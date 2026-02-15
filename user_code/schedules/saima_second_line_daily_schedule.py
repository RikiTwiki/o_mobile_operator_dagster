from dagster import ScheduleDefinition
from jobs.saima_second_line_daily_statistics.saima_second_line_daily_statistics_job import saima_second_line_daily_statistics_job

saima_second_line_daily_statistics_schedule = ScheduleDefinition(
    name="saima_second_line_daily_statistics_schedule",
    job=saima_second_line_daily_statistics_job,
    cron_schedule="10 8 * * *",
    execution_timezone="Asia/Bishkek",
)