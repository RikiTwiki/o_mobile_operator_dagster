from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from dagster import ScheduleDefinition, Definitions
from resources import resources
from jobs.aggregate.naumen_handling_time_data_command import naumen_handling_time_data_command
from dagster import job

def _prev_day_run_config(context):
    tz = ZoneInfo("Asia/Bishkek")
    dt = (context.scheduled_execution_time or datetime.now(tz)).astimezone(tz)
    start = (dt - timedelta(days=1)).date().isoformat()  # вчера
    end = dt.date().isoformat()                          # сегодня (exclusive)
    return {
        "ops": {
            "naumen_handling_time_data": {
                "config": {
                    "start": start,
                    "end": end,
                }
            }
        }
    }

naumen_handling_time_data_schedule = ScheduleDefinition(
    name="replicate_naumen_handling_time_data_daily",
    job=naumen_handling_time_data_command,
    cron_schedule="00 03 * * *",                # каждый день в 09:10
    execution_timezone="Asia/Bishkek",
    run_config_fn=_prev_day_run_config,
)