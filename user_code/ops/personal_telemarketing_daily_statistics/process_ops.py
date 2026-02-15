# user_code/ops/personal_telemarketing_daily_statistics/process_ops.py
from __future__ import annotations

from typing import Dict, Any, List, Optional
from datetime import datetime, date, timedelta

from dagster import op, Out, Output, get_dagster_logger, In

from zoneinfo import ZoneInfo

# === Getters (как и в других твоих опах) ===
try:
    from utils.getters.sales_data_getter import SalesDataGetter
except Exception:
    SalesDataGetter = None

try:
    from utils.getters.calls_data_getter import CallsDataGetter
except Exception:
    CallsDataGetter = None

# === Утилиты ===
from utils.transformers.mp_report import _to_dt

from zoneinfo import ZoneInfo

from utils.mail.mail_sender import ReportsApiConfig, ReportInstanceUploader

import os


def _to_iso_date(x) -> str:
    """Привести вход к 'YYYY-MM-DD'."""
    dt = _to_dt(x)
    if isinstance(dt, datetime):
        return dt.date().isoformat()
    if isinstance(dt, date):
        return dt.isoformat()
    return str(dt)[:10] if dt is not None else None


# -----------------------------------------------------------------------------
# ДАТЫ: берём окно «вчера..сегодня (exclusive)» + отчётный день (вчера),
# как это делалось в PHP-классе через StatisticDates.
# -----------------------------------------------------------------------------
@op(
    out={
        "sd": Out(str, description="start_date (YYYY-MM-DD)"),
        "ed": Out(str, description="end_date (exclusive) (YYYY-MM-DD)"),
        "rp": Out(str, description="report_date (YYYY-MM-DD)"),
    },
)
def parse_dates_personal_telemarketing(context):
    tz = ZoneInfo("Asia/Bishkek")  # единая TZ
    now = (context.scheduled_execution_time or datetime.now(tz)).astimezone(tz)
    sd = (now.date() - timedelta(days=1)).isoformat()
    ed = now.date().isoformat()
    rp = sd  # отчётный день = вчера
    context.log.info(f"[personal] sd={sd} ed={ed} rp={rp}")
    yield Output(sd, "sd")
    yield Output(ed, "ed")
    yield Output(rp, "rp")


# -----------------------------------------------------------------------------
# БЛОК ПРОДАЖ (персональные разрезы)
# Соответствует кускам:
#  - salesPersonalGTM           (Персональная статистика продаж, таблица)
#  - salesA                     (Динамика продаж за 30 дней, график)
#  - personalActivationData     (Динамика активаций в %, таблица)
# -----------------------------------------------------------------------------
@op(
    required_resource_keys={"source_naumen_1", "source_dwh_oracle"},
    out={
        "general": Out(Dict[str, Any], description="Сводный словарь для блока продаж"),
        "salesPersonalGTM": Out(List[Dict[str, Any]]),
        "salesA": Out(List[Dict[str, Any]]),
        "personalActivationData": Out(List[Dict[str, Any]]),
    },
)
def personal_sales_data_op(
    context,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    report_date: Optional[str] = None,
):
    if SalesDataGetter is None:
        raise RuntimeError("SalesDataGetter недоступен (импорт не удался).")

    log = get_dagster_logger()
    ora_conn = context.resources.source_dwh_oracle._conn()

    try:
        getter = SalesDataGetter(
            start_date=start_date,
            end_date=end_date,
            report_date=report_date,
            dwh_conn=ora_conn,
            naumen_conn=context.resources.source_naumen_1,
        )

        sales_personal = getter.get_personal_aggregated_sales_data()     # salesPersonalGTM
        sales_dynamic30 = getter.get_aggregated_count_sales_last_30_days()  # salesA
        personal_activation = getter.get_personal_activation_data()      # personalActivationData

        general = {
            "salesPersonalGTM": sales_personal,
            "salesA": sales_dynamic30,
            "personalActivationData": personal_activation,
        }

        log.info(f"[personal.sales] salesPersonalGTM={len(sales_personal or [])}")
        log.info(f"[personal.sales] salesA={len(sales_dynamic30 or [])}")
        log.info(f"[personal.sales] personalActivationData={len(personal_activation or [])}")

        yield Output(general, "general")
        yield Output(sales_personal, "salesPersonalGTM")
        yield Output(sales_dynamic30, "salesA")
        yield Output(personal_activation, "personalActivationData")

    finally:
        try:
            ora_conn.close()
        except Exception:
            pass


# -----------------------------------------------------------------------------
# БЛОК СТАТУСОВ и ЭФФЕКТИВНОСТИ
# Соответствует кускам:
#  - coffee, dinner, dnd, workTime, request, summaryRequest
#  - occupancy, callsDuration
#  - personalAHT
# -----------------------------------------------------------------------------
@op(
    required_resource_keys={"source_naumen_1", "source_dwh"},
    out={
        "general": Out(Dict[str, Any], description="Сводный словарь для статусов/эффективности"),
        "coffee": Out(List[Dict[str, Any]]),
        "dinner": Out(List[Dict[str, Any]]),
        "dnd": Out(List[Dict[str, Any]]),
        "workTime": Out(List[Dict[str, Any]]),
        "request": Out(List[Dict[str, Any]]),
        "summaryRequest": Out(List[Dict[str, Any]]),
        "occupancy": Out(List[Dict[str, Any]]),
        "callsDuration": Out(List[Dict[str, Any]]),
        "personalAHT": Out(List[Dict[str, Any]]),
    },
)
def personal_calls_and_kpi_op(
    context,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    if CallsDataGetter is None:
        raise RuntimeError("CallsDataGetter недоступен (импорт не удался).")

    log = get_dagster_logger()
    getter = CallsDataGetter(
        start_date=start_date,
        end_date=end_date,
        conn=context.resources.source_naumen_1,
        conn_bpm=context.resources.source_dwh,
    )

    coffee = getter.get_coffee()
    dinner = getter.get_dinner()
    dnd = getter.get_dnd()
    work_time = getter.get_work_time_data()
    request = getter.get_request_time()
    summary_request = getter.get_summary_status_duration()

    occupancy = getter.get_occupancy()
    calls_duration = getter.get_calls_duration()

    personal_aht = getter.get_personal_aht()

    general = {
        "coffee": coffee,
        "dinner": dinner,
        "dnd": dnd,
        "workTime": work_time,
        "request": request,
        "summaryRequest": summary_request,
        "occupancy": occupancy,
        "callsDuration": calls_duration,
        "personalAHT": personal_aht,
    }

    # Логи
    log.info("[personal.calls] coffee=%d dinner=%d dnd=%d", len(coffee or []), len(dinner or []), len(dnd or []))
    log.info("[personal.calls] workTime=%d request=%d summaryRequest=%d", len(work_time or []), len(request or []), len(summary_request or []))
    log.info("[personal.calls] occupancy=%d callsDuration=%d personalAHT=%d",
             len(occupancy or []), len(calls_duration or []), len(personal_aht or []))

    yield Output(general, "general")
    yield Output(coffee, "coffee")
    yield Output(dinner, "dinner")
    yield Output(dnd, "dnd")
    yield Output(work_time, "workTime")
    yield Output(request, "request")
    yield Output(summary_request, "summaryRequest")
    yield Output(occupancy, "occupancy")
    yield Output(calls_duration, "callsDuration")
    yield Output(personal_aht, "personalAHT")