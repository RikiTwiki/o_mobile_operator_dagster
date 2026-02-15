from dagster import ScheduleDefinition
from jobs.chat_bot_statistics.chat_bot_statistics_job import chat_bot_statistics_job

chat_bot_statistics_schedule = ScheduleDefinition(
    name="chat_bot_statistics_schedule",
    job=chat_bot_statistics_job,
    cron_schedule="25 8 * * *",
    execution_timezone="Asia/Bishkek",
)