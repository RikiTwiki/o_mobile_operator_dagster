# schedules/aggregate/general_daily_statistics_backfill_schedule.py
from __future__ import annotations
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from dagster import ScheduleDefinition
from jobs.general_daily_statistics.general_daily_statistics_job import general_daily_statistics_job

TZ = ZoneInfo("Asia/Bishkek")
STOP_DATE = date(2025, 11, 12)  # включительно

# имя финального отчёта можно переопределить через ENV
FINAL_FILE = os.getenv("CHAT_REPORT_FILE", "general_daily_statistics.pdf")

def _reports_base() -> Path:
    # свой ENV + отдельная папка по умолчанию
    return Path(os.getenv("CHAT_REPORTS_BASE_DIR", "/opt/dagster/general_reports"))

def _is_day_done(day_dir: Path) -> bool:
    """День считается готовым, если есть целевой файл или любой *.pdf в корне дня."""
    target = day_dir / FINAL_FILE
    if target.exists() and target.is_file() and target.stat().st_size > 0:
        return True
    return any(p.suffix.lower() == ".pdf" for p in day_dir.glob("*.pdf"))

def _next_date_to_run(today_local: date) -> date | None:
    base = _reports_base()
    d = today_local - timedelta(days=1)  # старт со «вчера»
    while d >= STOP_DATE:
        if not _is_day_done(base / d.isoformat()):
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
            "report_utils": {"config": {"tz": "Asia/Bishkek", "report_date_str": target_day.isoformat()}},
            # всё, что пишет fs_io_manager, улетит сюда
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
        "backfill": "general_daily_statistics",
        "output_dir": str((_reports_base() / d.isoformat()).resolve()),
    }

general_daily_statistics_backfill = ScheduleDefinition(
    name="general_daily_statistics_backfill",
    job=general_daily_statistics_job,
    cron_schedule="*/10 * * * *",     # каждые 15 минут
    execution_timezone="Asia/Bishkek",
    run_config_fn=_run_config_fn,
    should_execute=_should_execute,
    tags_fn=_tags_fn,
)