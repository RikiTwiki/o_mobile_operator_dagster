from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from dagster import ScheduleDefinition, Definitions
from resources import resources
from jobs.aggregate.aggregate_mp_handing_time_data_command import aggregate_mp_handing_time_data_command
from dagster import job

def _prev_day_run_config(context):
    tz = ZoneInfo("Asia/Bishkek")
    dt = (context.scheduled_execution_time or datetime.now(tz)).astimezone(tz)
    start = (dt - timedelta(days=1)).date().isoformat()  # вчера
    end = dt.date().isoformat()                          # сегодня (exclusive)
    return {
        "ops": {
            "aggregate_mp_handling_time_data": {  # убедись, что ключ совпадает с именем op внутри джоба
                "config": {
                    "start": start,
                    "end": end,
                }
            }
        }
    }

aggregate_mp_handling_time_data_schedule = ScheduleDefinition(
    name="replicate_aggregate_mp_handling_time_data_daily",
    job=aggregate_mp_handing_time_data_command,
    cron_schedule="00 03 * * *",                # каждый день в 09:10
    execution_timezone="Asia/Bishkek",
    run_config_fn=_prev_day_run_config,
)