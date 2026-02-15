# sensors/bank_daily_statistics_backfill_sensor.py
from __future__ import annotations
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from dagster import sensor, RunRequest, SkipReason
from jobs.bank_daily_statistics.bank_daily_statistics_job import bank_daily_statistics_job

TZ = ZoneInfo("Asia/Bishkek")
STOP_DATE = date(2025, 11, 12)  # включительно

def _reports_base() -> Path:
    return Path(os.getenv("REPORTS_BASE_DIR", "/opt/dagster/reports"))

def _has_any_files(p: Path) -> bool:
    return p.exists() and p.is_dir() and any(p.iterdir())

def _next_date_to_run(today_local: date) -> date | None:
    d = today_local - timedelta(days=1)
    base = _reports_base()
    while d >= STOP_DATE:
        if not _has_any_files(base / d.isoformat()):
            return d
        d -= timedelta(days=1)
    return None

def _build_run_config(target_day: date):
    day_dir = (_reports_base() / target_day.isoformat()).resolve()
    day_dir.mkdir(parents=True, exist_ok=True)
    return {
        "resources": {
            "report_utils": {
                "config": {
                    "tz": "Asia/Bishkek",
                    "report_date_str": target_day.isoformat(),
                }
            },
            "io_manager": {"config": {"base_dir": str(day_dir)}},
        }
    }

@sensor(job=bank_daily_statistics_job, minimum_interval_seconds=60)  # тикаем раз в минуту
def bank_daily_statistics_backfill_sensor(context):
    target_day = _next_date_to_run(datetime.now(TZ).date())
    if target_day is None:
        yield SkipReason(f"Backfill complete up to {STOP_DATE.isoformat()}.")
        return

    # Не плодим параллельные запуски: если есть активный run для этой job — подождём следующего тика
    active = [
        r for r in context.instance.get_runs()
        if r.job_name == bank_daily_statistics_job.name and r.status.value in {"QUEUED","STARTED"}
    ]
    if active:
        yield SkipReason("Run in progress, waiting for completion.")
        return

    run_config = _build_run_config(target_day)
    yield RunRequest(
        run_key=f"bank-daily-{target_day.isoformat()}",
        run_config=run_config,
        tags={
            "report_date": target_day.isoformat(),
            "backfill": "bank_daily_statistics",
            "output_dir": str((_reports_base() / target_day.isoformat()).resolve()),
        },
    )