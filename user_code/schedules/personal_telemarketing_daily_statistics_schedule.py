from dagster import ScheduleDefinition
from jobs.personal_telemarketing_daily_statistics.personal_telemarketing_daily_statistics_job import personal_telemarketing_daily_statistics_job

personal_telemarketing_daily_statistics_schedule = ScheduleDefinition(
    name="personal_telemarketing_daily_statistics_schedule",
    job=personal_telemarketing_daily_statistics_job,
    cron_schedule="11 8 * * *",
    execution_timezone="Asia/Bishkek",
)