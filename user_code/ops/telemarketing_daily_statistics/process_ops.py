from __future__ import annotations

import math
import os
from decimal import Decimal, ROUND_HALF_UP

import pandas as pd
import requests

from ops.csf_daily_statistics.process_ops import occupancy_op

import numpy as np

"""
telemarketing_daily_statistics/process_ops.py

Оп‑обёртки для ежедневной статистики телемаркетинга (ГТМ), переписанные по образцу
нашего общего process_ops.py. Здесь мы не рендерим HTML/графики — только собираем
и подготавливаем данные. Визуализация/почта — в visualizations_ops.py.

Ключевые блоки данных, соответствующие PHP‑классу TelemarketingDailyStatisticsNew::send():
• general_sales_data — агрегаты по продажам
• general_calls_data — сводка исходящих и причины недозвонов
• general_performance_data — срез по KPI (shareOfConnection, AHT, ART, Utilization, Occupancy)
• connections_ratio — дополнительные коэффициенты/отношения (если требуется)
• all_data — общий набор, если нужен единый payload

Примечание: Имена методов геттеров‑источников могут отличаться в вашем репо.
Здесь показан каркас; при интеграции подмените вызовы на фактические методы.
"""

from dagster import get_dagster_logger, op, In, Failure

from utils.path import BASE_SQL_PATH

from resources import ReportUtils, source_dwh_resource
from utils.array_operations import left_join_array

try:
    from utils.getters.sales_data_getter import SalesDataGetter
except Exception:
    SalesDataGetter = None

try:
    from utils.getters.calls_data_getter import CallsDataGetter
except Exception:
    CallsDataGetter = None

try:
    from utils.getters.occupancydatagetter import OccupancyDataGetter
except Exception:
    OccupancyDataGetter = None

try:
    from utils.getters.connection_data_service import ConnectionDataService
except Exception:
    ConnectionDataService = None

from dagster import op, Out, Output

from typing import Dict, Any, List, Optional

from datetime import datetime, date, timedelta

from utils.transformers.mp_report import _to_dt

from zoneinfo import ZoneInfo

from utils.mail.mail_sender import ReportsApiConfig, ReportInstanceUploader


# --- Глобальные даты (по образцу общего process_ops.py) ---
dates = ReportUtils().get_utils()
start_date = dates.StartDate
end_date = dates.EndDate
date_list = dates.Dates
report_date = dates.ReportDate
month_start = dates.MonthStart
month_end = dates.MonthEnd

# Позиция ГТМ (как в PHP‑версии)
POSITIONS = [19]

# Поле для join’ов по дате (day‑срез)
DATE_FIELD = "date"

base_sql_path = BASE_SQL_PATH


# ------------------------------
# ВСПОМОГАТЕЛЬНЫЕ ХЕЛПЕРЫ
# ------------------------------

def _setup_data_getter_period_days(getter) -> None:
    """Единая настройка периода = дни (как в нашем общем process_ops.setup_data_getter)."""
    # Совместимость с нашими геттерами: у всех есть set_days_trunked_data/date_format/date_field
    getter.start_date = start_date
    getter.end_date = end_date
    if hasattr(getter, "set_days_trunked_data"):
        getter.set_days_trunked_data()
    if hasattr(getter, "date_field"):
        getter.date_field = DATE_FIELD
    if hasattr(getter, "date_format"):
        getter.date_format = "YYYY-MM-DD"

def first_per_key(data_b: list[dict]) -> dict[str, dict]:
    seen = {}
    for r in data_b:
        k = (r.get("KEY") or "Other").strip()
        if k not in seen:
            seen[k] = {
                "KEY": k,
                "imp_tariff": r.get("imp_tariff"),
                "qwes_recomendet": r.get("qwes_recomendet"),
                "more5_sec": int(r.get("more5_sec") or 0),
                "total": int(r.get("total") or 0),
            }
    return seen

def _norm_date(df: pd.DataFrame, candidates=("date", "day", "dt")) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip().str.lower()
    dcol = next((c for c in candidates if c in df.columns), None)
    if dcol:
        df.rename(columns={dcol: "date"}, inplace=True)
        # приводим к YYYY-MM-DD
        df["date"] = (
            df["date"].astype(str).str.slice(0, 10)
            .str.replace(".", "-", regex=False)
            .str.replace("/", "-", regex=False)
        )
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    else:
        df["date"] = pd.NaT
    return df

def _avg_value(items: List[Dict[str, Any]]) -> float:
    if not items:
        return 0.0
    s = pd.Series([x.get("value") for x in items], dtype="float").fillna(0)
    return round(float(s.mean()))

def _floats_to_ints(obj):
    """Рекурсивно конвертирует все float -> int внутри произвольной структуры."""
    if isinstance(obj, float):
        # защищаемся от NaN/Inf
        if math.isnan(obj) or math.isinf(obj):
            return 0
        return int(obj)
    if isinstance(obj, list):
        return [_floats_to_ints(x) for x in obj]
    if isinstance(obj, dict):
        return {k: _floats_to_ints(v) for k, v in obj.items()}
    return obj

@op(
    required_resource_keys={"source_dwh", "source_naumen_1", "source_dwh_oracle"},
    out=Out(dict, description="general_performance_data для render_general_performance_table_op"),
)
def get_connection_table_data_op(
    context,
    positions: Optional[list] = None,
    start_date: str | None = None,
    end_date: str | None = None,

    start_date_month: str | None = None,

    end_date_month: str | None = None,

) -> Dict[str, Any]:

    ora_conn = context.resources.source_dwh_oracle._conn()

    log = get_dagster_logger()
    positions = positions or [19]

    # Инициализация сервисов
    data_service = ConnectionDataService(conn=context.resources.source_naumen_1, start_date=start_date, end_date=end_date) if ConnectionDataService else None
    data_sales_getter = SalesDataGetter(start_date=start_date, end_date=end_date, dwh_conn=ora_conn, naumen_conn=context.resources.source_naumen_1) if SalesDataGetter else None
    occupancy_data_getter = OccupancyDataGetter(start_date=start_date, end_date=end_date, conn_naumen_1=context.resources.source_naumen_1, position_ids=positions, bpm_conn=context.resources.source_dwh) if OccupancyDataGetter else None
    data_calls_getter = CallsDataGetter(conn=context.resources.source_naumen_1, start_date=start_date, end_date=end_date, conn_bpm=context.resources.source_dwh) if CallsDataGetter else None

    data_calls_getter_month = CallsDataGetter(conn=context.resources.source_naumen_1, start_date=start_date_month, end_date=end_date_month, conn_bpm=context.resources.source_dwh) if CallsDataGetter else None

    # Чтение данных
    share_of_connection = data_service.get_combined_naumen_data()
    share_of_connection_all = data_service.get_combined_naumen_data_all()

    connections = data_sales_getter.get_connections()
    conversion = data_sales_getter.get_detailed_sales_conversion_new()

    occupancy_data_getter.set_days_trunked_data()

    utilization = occupancy_data_getter.get_utilization_rate_data_day(positions)

    occupancy = data_calls_getter.get_occupancy_graphic()

    aht = data_calls_getter.get_call_aht_new()

    aht_all = data_calls_getter.get_call_aht_all()

    aht_all_month = data_calls_getter_month.get_call_aht_all()

    art = data_calls_getter.get_call_art()

    art_all = data_calls_getter.get_call_art_all()

    occupancy = pd.DataFrame(occupancy).to_dict("records")

    data = {
        "shareOfConnection": share_of_connection,
        "averageShareOfConnection": round(
            (sum((x.get("value") or 0) for x in share_of_connection) / len(share_of_connection))
        ) if share_of_connection else 0.0,

        "shareOfConnectionAll": share_of_connection_all,
        "averageShareOfConnectionAll": round(
            (sum((x.get("value") or 0) for x in share_of_connection_all) / len(share_of_connection_all))
        ) if share_of_connection_all else 0.0,

        "connections": connections,
        "averageConnections": round(
            (sum((x.get("CONNECTIONS") or 0) for x in connections) / len(connections))
        ) if connections else 0.0,

        "conversion": conversion,
        "averageConversion": round(
            (sum((x.get("value") or 0) for x in conversion) / len(conversion))
        ) if conversion else 0.0,

        "utilization": utilization,
        "averageUtilization": round(
            (sum((x.get("value") or 0) for x in utilization) / len(utilization))
        ) if utilization else 0.0,

        "occupancy": occupancy,
        "averageOccupancy": round(
            (sum((x.get("value") or 0) for x in occupancy) / len(occupancy))
        ) if occupancy else 0.0,

        "aht": aht,
        "averageAHT": round(
            (sum((x.get("value") or 0) for x in aht) / len(aht))
        ) if aht else 0.0,

        "ahtAll": aht_all,
        "averageAHTAll": round(
            (sum((x.get("value") or 0) for x in aht_all) / len(aht_all))
        ) if aht_all else 0.0,

        "ahtAllMonth": aht_all_month,

        "art": art,
        "averageART": round(
            (sum((x.get("value") or 0) for x in art) / len(art))
        ) if art else 0.0,

        "artAll": art_all,
        "averageARTAll": round(
            (sum((x.get("value") or 0) for x in art_all) / len(art_all))
        ) if art_all else 0.0,
    }

    log.info(f"data: {data}")

    return data


@op(
    required_resource_keys={"source_dwh", "source_naumen_1", "source_naumen_3"},

    out={
        "general": Out(Dict[str, Any], description="Полный словарь, как в PHP"),
        "callsGTM": Out(List[Dict[str, Any]]),
        "callsPercentGTM": Out(List[Dict[str, Any]]),
        "callsDetailGTM": Out(List[Dict[str, Any]]),
        "salesB": Out(List[Dict[str, Any]]),
        "callsRate": Out(List[Dict[str, Any]]),
        "callAHT": Out(List[Dict[str, Any]]),
        "coffee": Out(List[Dict[str, Any]]),
        "dinner": Out(List[Dict[str, Any]]),
        "dnd": Out(List[Dict[str, Any]]),
        "occupancy": Out(List[Dict[str, Any]]),
        "occupancyGraph": Out(List[Dict[str, Any]]),
        "workTime": Out(List[Dict[str, Any]]),
        "personalAHT": Out(List[Dict[str, Any]]),
        "employee": Out(List[Dict[str, Any]]),
        "request": Out(List[Dict[str, Any]]),
        "income": Out(List[Dict[str, Any]]),
        "incomeCalls": Out(List[Dict[str, Any]]),
        "summaryRequest": Out(List[Dict[str, Any]]),
        "callsDuration": Out(List[Dict[str, Any]]),
        "projectSpentTime": Out(List[Dict[str, Any]]),
        "projectSpentTimeUser": Out(List[Dict[str, Any]]),
    },

)
def calls_data_getter_op(
    context,
    start_date: str | None = None,
    end_date: str | None = None,
):

    log = get_dagster_logger()

    get_calls_data = CallsDataGetter(start_date=start_date, end_date=end_date, conn=context.resources.source_naumen_1, conn_bpm=context.resources.source_dwh)

    calls_gtm = get_calls_data.get_naumen_out_calls_data_new()

    calls_percent_gtm =get_calls_data.get_percent_naumen_out_calls_data()
    calls_detail_gtm = get_calls_data.get_detailed_naumen_out_calls_data()
    sales_b = get_calls_data.get_aggregated_out_calls_last_30_days()
    calls_rate = get_calls_data.get_aggregated_call_conversion()
    call_aht = get_calls_data.get_call_aht_new()
    coffee = get_calls_data.get_coffee()
    dinner = get_calls_data.get_dinner()
    dnd = get_calls_data.get_dnd()
    occupancy = get_calls_data.get_occupancy()
    occupancy_graph = get_calls_data.get_occupancy_graphic()
    work_time = get_calls_data.get_work_time_data()
    personal_aht = get_calls_data.get_personal_aht()
    employee = get_calls_data.get_employee_quantity()
    request = get_calls_data.get_request_time()
    income = get_calls_data.get_income_sl()
    income_calls = get_calls_data.get_income_calls()
    summary_request = get_calls_data.get_summary_status_duration()
    calls_duration = get_calls_data.get_calls_duration()

    project_spent_time = get_calls_data.get_project_spent_time_last_30_days()

    project_spent_time_user = get_calls_data.get_time_ratio()

    data: Dict[str, Any] = {
        "callsGTM": calls_gtm,
        "callsPercentGTM": calls_percent_gtm,
        "callsDetailGTM": calls_detail_gtm,
        "salesB": sales_b,
        "callsRate": calls_rate,
        "callAHT": call_aht,
        "coffee": coffee,
        "dinner": dinner,
        "dnd": dnd,
        "occupancy": occupancy,
        "occupancyGraph": occupancy_graph,
        "workTime": work_time,
        "personalAHT": personal_aht,
        "employee": employee,
        "request": request,
        "income": income,
        "incomeCalls": income_calls,
        "summaryRequest": summary_request,
        "callsDuration": calls_duration,
        "projectSpentTime": project_spent_time,
        "projectSpentTimeUser": project_spent_time_user,
    }

    # 5) Логи аккуратно
    log.info("general_calls_data keys: %s", list(data.keys()))
    log.info("callsGTM len=%d", len(calls_gtm or []))

    log.info(f"callsGTM: {calls_gtm}")
    log.info(f"callsPercentGTM: {calls_percent_gtm}")
    log.info(f"callsDetailGTM: {calls_detail_gtm}")
    log.info(f"salesB: {sales_b}")
    log.info(f"callsRate: {calls_rate}")
    log.info(f"callAHT: {call_aht}")
    log.info(f"coffee: {coffee}")
    log.info(f"dinner: {dinner}")
    log.info(f"dnd: {dnd}")
    log.info(f"occupancy: {occupancy}")
    log.info(f"occupancyGraph: {occupancy_graph}")
    log.info(f"workTime: {work_time}")
    log.info(f"personalAHT: {personal_aht}")
    log.info(f"employee: {employee}")
    log.info(f"request: {request}")
    log.info(f"income: {income}")
    log.info(f"incomeCalls: {income_calls}")
    log.info(f"summaryRequest: {summary_request}")
    log.info(f"callsDuration: {calls_duration}")
    log.info(f"projectSpentTime: {project_spent_time}")
    log.info(f"projectSpentTimeUser: {project_spent_time_user}")

    # 6) Мульти-аутпуты
    yield Output(data, "general")
    yield Output(calls_gtm, "callsGTM")
    yield Output(calls_percent_gtm, "callsPercentGTM")
    yield Output(calls_detail_gtm, "callsDetailGTM")
    yield Output(sales_b, "salesB")
    yield Output(calls_rate, "callsRate")
    yield Output(call_aht, "callAHT")
    yield Output(coffee, "coffee")
    yield Output(dinner, "dinner")
    yield Output(dnd, "dnd")
    yield Output(occupancy, "occupancy")
    yield Output(occupancy_graph, "occupancyGraph")
    yield Output(work_time, "workTime")
    yield Output(personal_aht, "personalAHT")
    yield Output(employee, "employee")
    yield Output(request, "request")
    yield Output(income, "income")
    yield Output(income_calls, "incomeCalls")
    yield Output(summary_request, "summaryRequest")
    yield Output(calls_duration, "callsDuration")
    yield Output(project_spent_time, "projectSpentTime")
    yield Output(project_spent_time_user, "projectSpentTimeUser")

@op(
    required_resource_keys={"source_dwh", "source_naumen_1", "source_naumen_3", "source_dwh_oracle"},

    out={

        "general": Out(Dict[str, Any], description="Полный словарь, как в PHP"),

        'salesGTM': Out(List[Dict[str, Any]]),
        'totalASPU': Out(List[Dict[str, Any]]),
        'salesPersonalGTM': Out(List[Dict[str, Any]]),
        'salesA': Out(List[Dict[str, Any]]),
        'salesConversion': Out(List[Dict[str, Any]]),
        'salesProfit': Out(List[Dict[str, Any]]),
        'salesRenewal': Out(List[Dict[str, Any]]),
        'salesPayments': Out(List[Dict[str, Any]]),
        'salesProfitGraphic': Out(List[Dict[str, Any]]),
        'salesRenewalGraphic': Out(List[Dict[str, Any]]),
        'salesActivation': Out(List[Dict[str, Any]]),
        'salesActivationGraphic': Out(List[Dict[str, Any]]),
        'salesStatus': Out(List[Dict[str, Any]]),
        'personalActivationData': Out(List[Dict[str, Any]]),
        'totalASPURenewalData': Out(List[Dict[str, Any]]),
        'totalASPUActivationData': Out(List[Dict[str, Any]]),
        'bonus': Out(List[Dict[str, Any]]),
        'bonusGraphic': Out(List[Dict[str, Any]]),
        'renewalStatus': Out(List[Dict[str, Any]]),
        'firstPricePlan': Out(List[Dict[str, Any]]),
        'ASPU': Out(List[Dict[str, Any]]),
        'mistake': Out(List[Dict[str, Any]]),
        'salesTransitionsData': Out(List[Dict[str, Any]]),
        'salesTransitionsDataDay': Out(List[Dict[str, Any]]),
    },

)
def sales_data_getter_op(
    context,
    start_date: str | None = None,
    end_date: str | None = None,

    report_date: str | None = None,

):

    ora_conn = context.resources.source_dwh_oracle._conn()

    log = get_dagster_logger()

    get_sales_data = SalesDataGetter(start_date=start_date, end_date=end_date, report_date=report_date, dwh_conn=ora_conn, naumen_conn=context.resources.source_naumen_1)

    sales_gtm = get_sales_data.get_aggregated_sales_data()
    total_aspu = get_sales_data.get_total_aspu_data()
    sales_personal_gtm = get_sales_data.get_personal_aggregated_sales_data()
    sales_a = get_sales_data.get_aggregated_count_sales_last_30_days()
    sales_conversion = get_sales_data.get_detailed_sales_conversion()
    sales_profit = get_sales_data.get_sales_profit_data()
    sales_renewal = get_sales_data.get_sales_renewal_data()
    sales_payments = get_sales_data.get_sales_payments_data()
    sales_profit_graphic = get_sales_data.get_sales_profit_graphic()
    sales_renewal_graphic = get_sales_data.get_sales_renewal_graphic()
    sales_activation = get_sales_data.get_sales_activation_data()
    sales_activation_graphic = get_sales_data.get_sales_activation_graphic()
    sales_status = get_sales_data.get_sales_status_data()
    personal_activation_data = get_sales_data.get_personal_activation_data()
    total_aspu_renewal_data = get_sales_data.get_total_aspu_renewal_data()
    total_aspu_activation_data = get_sales_data.get_total_aspu_activation_data()
    bonus = get_sales_data.get_bonus()
    bonus_graphic = get_sales_data.get_bonus_graphic()
    renewal_status = get_sales_data.get_renewal_status_data()
    first_price_plan = get_sales_data.get_first_price_plan()
    aspu = get_sales_data.get_aspu_lift_data()
    mistake = get_sales_data.get_mistakes_data()

    mistake = _floats_to_ints(mistake)

    sales_transitions_data = get_sales_data.get_aggregated_transitions_data()

    sales_transitions_data_day = get_sales_data.get_aggregated_transitions_data_for_report_day()

    data: Dict[str, Any] = {
        'salesGTM': sales_gtm,
        'totalASPU': total_aspu,
        'salesPersonalGTM': sales_personal_gtm,
        'salesA': sales_a,
        'salesConversion': sales_conversion,
        'salesProfit': sales_profit,
        'salesRenewal': sales_renewal,
        'salesPayments': sales_payments,
        'salesProfitGraphic': sales_profit_graphic,
        'salesRenewalGraphic': sales_renewal_graphic,
        'salesActivation': sales_activation,
        'salesActivationGraphic': sales_activation_graphic,
        'salesStatus': sales_status,
        'personalActivationData': personal_activation_data,
        'totalASPURenewalData': total_aspu_renewal_data,
        'totalASPUActivationData': total_aspu_activation_data,
        'bonus': bonus,
        'bonusGraphic': bonus_graphic,
        'renewalStatus': renewal_status,
        'firstPricePlan': first_price_plan,
        'ASPU': aspu,
        'mistake': mistake,
        'salesTransitionsData': sales_transitions_data,
        'salesTransitionsDataDay': sales_transitions_data_day,
    }

    log.info(f"salesGTM: {data['salesGTM']}")
    log.info(f"totalASPU: {data['totalASPU']}")
    log.info(f"salesPersonalGTM: {data['salesPersonalGTM']}")
    log.info(f"salesA: {data['salesA']}")
    log.info(f"salesConversion: {data['salesConversion']}")
    log.info(f"salesProfit: {data['salesProfit']}")
    log.info(f"salesRenewal: {data['salesRenewal']}")
    log.info(f"salesPayments: {data['salesPayments']}")
    log.info(f"salesProfitGraphic: {data['salesProfitGraphic']}")
    log.info(f"salesActivation: {data['salesActivation']}")
    log.info(f"salesActivationGraphic: {data['salesActivationGraphic']}")
    log.info(f"salesStatus: {data['salesStatus']}")
    log.info(f"personalActivationData: {data['personalActivationData']}")
    log.info(f"totalASPURenewalData: {data['totalASPURenewalData']}")
    log.info(f"totalASPUActivationData: {data['totalASPUActivationData']}")
    log.info(f"bonus: {data['bonus']}")
    log.info(f"bonusGraphic: {data['bonusGraphic']}")
    log.info(f"renewalStatus: {data['renewalStatus']}")
    log.info(f"firstPricePlan: {data['firstPricePlan']}")
    log.info(f"ASPU: {data['ASPU']}")
    log.info(f"mistake: {data['mistake']}")
    log.info(f"salesTransitionsData: {data['salesTransitionsData']}")
    log.info(f"salesTransitionsDataDay: {data['salesTransitionsDataDay']}")

    yield Output(data, "general")
    yield Output(sales_gtm, "salesGTM")
    yield Output(total_aspu, "totalASPU")
    yield Output(sales_personal_gtm, "salesPersonalGTM")
    yield Output(sales_a, "salesA")
    yield Output(sales_conversion, "salesConversion")
    yield Output(sales_profit, "salesProfit")
    yield Output(sales_renewal, "salesRenewal")
    yield Output(sales_payments, "salesPayments")
    yield Output(sales_profit_graphic, "salesProfitGraphic")
    yield Output(sales_renewal_graphic, "salesRenewalGraphic")
    yield Output(sales_activation, "salesActivation")
    yield Output(sales_activation_graphic, "salesActivationGraphic")
    yield Output(sales_status, "salesStatus")
    yield Output(personal_activation_data, "personalActivationData")
    yield Output(total_aspu_renewal_data, "totalASPURenewalData")
    yield Output(total_aspu_activation_data, "totalASPUActivationData")
    yield Output(bonus, "bonus")
    yield Output(bonus_graphic, "bonusGraphic")
    yield Output(renewal_status, "renewalStatus")
    yield Output(first_price_plan, "firstPricePlan")
    yield Output(aspu, "ASPU")
    yield Output(mistake, "mistake")
    yield Output(sales_transitions_data, "salesTransitionsData")
    yield Output(sales_transitions_data_day, "salesTransitionsDataDay")

@op(
    required_resource_keys={"source_naumen_1", "source_dwh_oracle"},
    out=Out(List[Dict[str, Any]], description="Список записей по дням для графика конверсии соединений"),
)
def get_connections_ratio_op(
    context,
    start_date: str | None = None,
    end_date: str | None = None,
) -> List[Dict[str, Any]]:
    """
    Соотношение: (кол-во подключений / соединено) * 100 по дням.
    Берём:
      - Naumen (source_naumen_1) → get_naumen_out_calls_data() → фильтруем 'Соединено'
      - Oracle DWH (source_dwh_oracle) → get_connections()
    """

    log = get_dagster_logger()

    def _norm_date(df: pd.DataFrame, date_cols=("date", "day", "day_str", "dt")) -> pd.DataFrame:
        df = df.copy()
        df.columns = [c.strip().lower() for c in df.columns]
        # найти колонку даты
        dcol = next((c for c in date_cols if c in df.columns), None)
        if dcol is None:
            df["date"] = pd.NaT
        else:
            df.rename(columns={dcol: "date"}, inplace=True)
            df["date"] = (
                df["date"].astype(str).str.slice(0, 10)
                .str.replace(".", "-", regex=False)
                .str.replace("/", "-", regex=False)
            )
            df["date"] = pd.to_datetime(df["date"], dayfirst=True, errors="coerce").dt.strftime("%Y-%m-%d")
        return df

    def _ensure_value(df: pd.DataFrame, candidates=("value", "connections", "calls", "cnt", "count")) -> pd.Series:
        # если ни одной из кандидатных колонок нет — вернём нули нужной длины
        for c in candidates:
            if c in df.columns:
                return pd.to_numeric(df[c], errors="coerce")
        return pd.Series(np.nan, index=df.index)

    # ---- helpers ----
    def _to_records(rows):
        out = []
        for r in (rows or []):
            if isinstance(r, dict):
                out.append(r)
            elif hasattr(r, "_asdict"):
                out.append(r._asdict())
            elif hasattr(r, "__dict__"):
                out.append({k: v for k, v in vars(r).items() if not k.startswith("_")})
            else:
                try:
                    out.append(dict(r))
                except Exception:
                    pass
        return out

    calls_getter = CallsDataGetter(
        start_date=start_date,
        end_date=end_date,
        conn=context.resources.source_naumen_1,
    )
    oc = pd.DataFrame(_to_records(calls_getter.get_naumen_out_calls_data()))
    if not oc.empty:
        oc = _norm_date(oc)
        # фильтр 'Соединено'
        if "row" in oc.columns:
            oc = oc[(oc["row"] == "Соединено") | (oc["row"] == "соединено")]
        vals = _ensure_value(oc, ("value", "calls", "cnt", "count"))
        oc["Соединено"] = vals.fillna(0).astype(int)
        oc = oc.groupby("date", as_index=False)["Соединено"].sum()
    else:
        oc = pd.DataFrame(columns=["date", "Соединено"])

    context.log.info(f"oc: {oc}")

    ora_conn = context.resources.source_dwh_oracle._conn()
    try:
        sales_getter = SalesDataGetter(
            start_date=start_date,
            end_date=end_date,
            dwh_conn=ora_conn,
        )
        cn = pd.DataFrame(_to_records(sales_getter.get_connections()))
    finally:
        try:
            ora_conn.close()
        except Exception:
            pass

    if not cn.empty:
        cn = _norm_date(cn)
        cn["кол-во подключений"] = _ensure_value(cn, ("value", "connections", "cnt", "count")).fillna(0).astype(int)
        cn = cn.groupby("date", as_index=False)["кол-во подключений"].sum()
    else:
        cn = pd.DataFrame(columns=["date", "кол-во подключений"])

    context.log.info(f"cn: {cn}")

    # ---- merge + расчёт ----
    merged = pd.merge(oc, cn, on="date", how="outer").fillna(0)
    for col in ["Соединено", "кол-во подключений"]:
        if col in merged.columns:
            merged[col] = merged[col].astype(int)

    merged["соединено / кол-во подключений"] = (
        merged["кол-во подключений"]
        .div(merged["Соединено"].replace(0, np.nan))  # знаменатель — "Соединено"
        .mul(100)
        .round(1)
        .fillna(0.0)
        .astype(float)
    )

    merged = merged.sort_values("date").reset_index(drop=True)
    result: List[Dict[str, Any]] = merged.to_dict("records")

    context.log.info(f"result: {result}")
    return result

@op(required_resource_keys={"source_dwh", "source_naumen_1", "source_naumen_3", "source_dwh_oracle"})
def get_all_data_op(
    context,
    start_date: str | None = None,
    end_date: str | None = None,
) -> list[dict]:

    log = get_dagster_logger()

    data_calls_getter = CallsDataGetter(
        start_date=start_date,
        end_date=end_date,
        conn=context.resources.source_naumen_1,
    )

    sql_path = os.path.join(base_sql_path, "dwh", "get_all_data.sql")
    with open(sql_path, "r", encoding="utf-8") as f:
        query = f.read()

    params = {
        "start_date": start_date,
        "end_date": end_date,
    }

    rows = context.resources.source_dwh_oracle.fetch_all(query, params)

    log.info(f"rows: {rows}")

    log.info(f"dates: {start_date}, {end_date}")

    data_b = data_calls_getter.fetch_telesales_upsales_stats()

    log.info(f"data_b: {data_b}")

    joined = left_join_array(rows, data_b, "KEY")
    log.info(f"data: {joined}")
    return joined

@op(required_resource_keys={"source_dwh", "source_naumen_1", "source_naumen_3", "source_dwh_oracle"})
def utilization_op(
    context,
    positions: list | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> list[dict]:

    log = get_dagster_logger()

    positions = positions or [19]

    occupancy_data_getter = OccupancyDataGetter(start_date=start_date, end_date=end_date, bpm_conn=context.resources.source_dwh, conn_naumen_1=context.resources.source_naumen_1)

    occupancy_data_getter.set_days_trunked_data()

    log.info(f"data: {occupancy_data_getter.get_utilization_rate_data_day(positions)}")

    return occupancy_data_getter.get_utilization_rate_data_day(positions)

def _to_iso_date(x) -> str:
    """Приводит вход (строка/датавремя/дата) к 'YYYY-MM-DD'."""
    dt = _to_dt(x)  # ваш существующий парсер
    if isinstance(dt, datetime):
        return dt.date().isoformat()
    if isinstance(dt, date):
        return dt.isoformat()
    # на крайний случай, если _to_dt вернул что-то иное/None
    return str(dt)[:10] if dt is not None else None

@op(
    out={
        "sd":  Out(str),
        "ed":  Out(str),
        "msd": Out(str),
        "med": Out(str),
        "rp":  Out(str),
    },
    required_resource_keys={"report_utils"},
)
def parse_dates_telemarketing(context):
    d = context.resources.report_utils.get_utils()

    sd  = _to_iso_date(getattr(d, "StartDate"))
    ed  = _to_iso_date(getattr(d, "EndDate"))
    msd = _to_iso_date(getattr(d, "MonthStart"))
    med = _to_iso_date(getattr(d, "MonthEnd"))
    rp  = _to_iso_date(getattr(d, "ReportDate"))

    context.log.info(f"sd={sd} ed={ed} msd={msd} med={med} rp={rp}")

    yield Output(sd,  "sd")
    yield Output(ed,  "ed")
    yield Output(msd, "msd")
    yield Output(med, "med")
    yield Output(rp,  "rp")

@op(
    required_resource_keys={"report_utils"},
    ins={"file_path": In(str, description="Абсолютный путь к PDF файлу.")},
    out=Out(dict, description="Ответ API как dict (JSON) либо {'status_code', 'text'}"),
    description=(
        "Загружает PDF-отчёт на /api/report-instances/upload/{report_id} (RV) по схеме как в curl: "
        "заголовок x-api-key и форма file/title/day. "
        "Дата 'day' по умолчанию = (сегодня в локальной TZ) − 1, формат YYYY.MM.DD."
    ),
)
def upload_report_instance_gtm_op(context, file_path: str) -> Dict[str, Any]:
    cfg = context.op_config or {}

    dates = context.resources.report_utils.get_utils()
    rd = dates.ReportDate
    if isinstance(rd, str):
        report_day = datetime.strptime(rd[:10], "%Y-%m-%d").date()
    elif isinstance(rd, datetime):
        report_day = rd.date()
    else:
        report_day = rd  # date

    day_str = report_day.strftime("%Y.%m.%d")

    tz_name = cfg.get("timezone") or os.getenv("REPORTS_TZ") or "Asia/Bishkek"

    context.log.info(f"День отчёта: {day_str} (from report_utils.ReportDate, TZ={tz_name})")

    base_url = cfg.get("base_url") or os.getenv("REPORTS_API_BASE", "https://rv.o.kg")
    report_id = int(cfg.get("report_id", 7))
    api_key = cfg.get("api_key") or os.getenv("REPORTS_API_KEY") or os.getenv("RV_REPORTS_API_KEY")
    if not api_key:
        raise Failure(description="Не задан API ключ. Укажите op_config.api_key или переменную окружения REPORTS_API_KEY / RV_REPORTS_API_KEY.")

    timeout_connect = int(cfg.get("timeout_connect", 10))
    timeout_read = int(cfg.get("timeout_read", 120))
    verify_ssl = bool(cfg.get("verify_ssl", True))

    title = cfg.get("title") or "ГТМ | Общая ежедневная статистика"

    url = f"{base_url.rstrip('/')}/api/report-instances/upload/{report_id}"
    headers = {"x-api-key": api_key}
    data = {"title": title, "day": day_str}

    context.log.info(f"POST {url} (verify_ssl={verify_ssl})")

    try:
        with open(file_path, "rb") as f:
            files = {"file": (os.path.basename(file_path), f, "application/pdf")}
            resp = requests.post(
                url,
                headers=headers,
                data=data,
                files=files,
                timeout=(timeout_connect, timeout_read),
                verify=verify_ssl,
            )
    except Exception as e:
        context.log.error(f"Ошибка при загрузке: {e}")
        return {"status_code": -1, "text": str(e)}

    context.log.info(f"Статус ответа: {resp.status_code}")
    try:
        return resp.json()
    except Exception:
        return {"status_code": resp.status_code, "text": resp.text}

@op(
    required_resource_keys={"report_utils"},
    ins={"file_path": In(str, description="Абсолютный путь к PDF файлу.")},
    out=Out(dict, description="Ответ API как dict (JSON) либо {'status_code', 'text'}"),
    description=(
        "Загружает PDF-отчёт на /api/report-instances/upload/{report_id} (RV) по схеме как в curl: "
        "заголовок x-api-key и форма file/title/day. "
        "Дата 'day' по умолчанию = (сегодня в локальной TZ) − 1, формат YYYY.MM.DD."
    ),
)
def upload_report_instance_personal_gtm_op(context, file_path: str) -> Dict[str, Any]:
    cfg = context.op_config or {}

    dates = context.resources.report_utils.get_utils()
    rd = dates.ReportDate
    if isinstance(rd, str):
        report_day = datetime.strptime(rd[:10], "%Y-%m-%d").date()
    elif isinstance(rd, datetime):
        report_day = rd.date()
    else:
        report_day = rd  # date

    day_str = report_day.strftime("%Y.%m.%d")

    tz_name = cfg.get("timezone") or os.getenv("REPORTS_TZ") or "Asia/Bishkek"

    context.log.info(f"День отчёта: {day_str} (from report_utils.ReportDate, TZ={tz_name})")

    base_url = cfg.get("base_url") or os.getenv("REPORTS_API_BASE", "https://rv.o.kg")
    report_id = int(cfg.get("report_id", 10))
    api_key = cfg.get("api_key") or os.getenv("REPORTS_API_KEY") or os.getenv("RV_REPORTS_API_KEY")
    if not api_key:
        raise Failure(description="Не задан API ключ. Укажите op_config.api_key или переменную окружения REPORTS_API_KEY / RV_REPORTS_API_KEY.")

    timeout_connect = int(cfg.get("timeout_connect", 10))
    timeout_read = int(cfg.get("timeout_read", 120))
    verify_ssl = bool(cfg.get("verify_ssl", True))

    title = cfg.get("title") or "ГТМ | Персональная ежедневная статистика"

    url = f"{base_url.rstrip('/')}/api/report-instances/upload/{report_id}"
    headers = {"x-api-key": api_key}
    data = {"title": title, "day": day_str}

    context.log.info(f"POST {url} (verify_ssl={verify_ssl})")

    try:
        with open(file_path, "rb") as f:
            files = {"file": (os.path.basename(file_path), f, "application/pdf")}
            resp = requests.post(
                url,
                headers=headers,
                data=data,
                files=files,
                timeout=(timeout_connect, timeout_read),
                verify=verify_ssl,
            )
    except Exception as e:
        context.log.error(f"Ошибка при загрузке: {e}")
        return {"status_code": -1, "text": str(e)}

    context.log.info(f"Статус ответа: {resp.status_code}")
    try:
        return resp.json()
    except Exception:
        return {"status_code": resp.status_code, "text": resp.text}