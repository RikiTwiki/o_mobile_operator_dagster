# schedules/aggregate/bank_daily_statistics_backfill_schedule.py
from __future__ import annotations
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from dagster import ScheduleDefinition
from jobs.bank_daily_statistics.bank_daily_statistics_job import bank_daily_statistics_job

TZ = ZoneInfo("Asia/Bishkek")
STOP_DATE = date(2025, 11, 12)  # включительно

def _reports_base() -> Path:
    # В Docker по умолчанию: ./reports → /opt/dagster/reports
    return Path(os.getenv("REPORTS_BASE_DIR", "/opt/dagster/reports"))

def _has_any_files(p: Path) -> bool:
    return p.exists() and p.is_dir() and any(p.iterdir())

def _next_date_to_run(today_local: date) -> date | None:
    base = _reports_base()
    d = today_local - timedelta(days=1)  # стартуем со вчера
    while d >= STOP_DATE:
        if not _has_any_files(base / d.isoformat()):
            return d
        d -= timedelta(days=1)
    return None

def _choose_target_day() -> date | None:
    return _next_date_to_run(datetime.now(TZ).date())

def _run_config_fn(context):
    target_day = _choose_target_day()
    if target_day is None:
        return {}
    day_dir = (_reports_base() / target_day.isoformat()).resolve()
    day_dir.mkdir(parents=True, exist_ok=True)
    return {
        "resources": {
            "report_utils": {
                "config": {"tz": "Asia/Bishkek", "report_date_str": target_day.isoformat()}
            },
            # все артефакты этого запуска пишутся в папку дня
            "io_manager": {"config": {"base_dir": str(day_dir)}},
        }
    }

def _should_execute(context) -> bool:
    # если подходящей даты нет — пропускаем тик
    return _choose_target_day() is not None

def _tags_fn(context):
    d = _choose_target_day()
    return {} if d is None else {
        "report_date": d.isoformat(),
        "backfill": "bank_daily_statistics",
        "output_dir": str((_reports_base() / d.isoformat()).resolve()),
    }

bank_daily_statistics_backfill = ScheduleDefinition(
    name="bank_daily_statistics_backfill",
    job=bank_daily_statistics_job,
    cron_schedule="*/11 * * * *",
    execution_timezone="Asia/Bishkek",
    run_config_fn=_run_config_fn,
    should_execute=_should_execute,
    tags_fn=_tags_fn,
)