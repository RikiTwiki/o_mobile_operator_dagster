from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from dagster import ScheduleDefinition
from jobs.telemarketing_daily_statistics.telemarketing_daily_statistics_job import telemarketing_daily_statistics_job

def _prev_day_run_config(context):
    tz = ZoneInfo("Asia/Bishkek")
    dt = (context.scheduled_execution_time or datetime.now(tz)).astimezone(tz)
    start = (dt - timedelta(days=1)).date().isoformat()
    end = dt.date().isoformat()
    return {
        "ops": {
            "telemarketing_daily_statistics": {
                "config": {
                    "start": start,
                    "end": end,
                }
            }
        }
    }

telemarketing_daily_statistics_schedule = ScheduleDefinition(
    name="telemarketing_daily_statistics_schedule",
    job=telemarketing_daily_statistics_job,
    cron_schedule="05 08 * * *",
    execution_timezone="Asia/Bishkek",
    run_config_fn=_prev_day_run_config,
)