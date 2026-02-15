from __future__ import annotations

import math
from decimal import Decimal

import requests
from dagster import op, In, Out, Output, AssetKey, Failure

from datetime import datetime as dt, datetime, date
from dagster import op, In
from datetime import datetime, timedelta
from pathlib import Path
from typing import Union, List, Sequence, Dict, Any, Iterable, Set

import pandas as pd
from dagster import op, get_dagster_logger

from resources import ReportUtils
from utils.getters.calls_data_getter import CallsDataGetter
from utils.path import BASE_SQL_PATH
from utils.array_operations import left_join_array, _smart_number

from utils.getters.average_handling_time_data_getter import AverageHandlingTimeDataGetter
from utils.getters.naumen_service_level_data_getter import NaumenServiceLevelDataGetter
from utils.getters.occupancydatagetter import OccupancyDataGetter
from utils.getters.questionnaire_data_getter import QuestionnaireDataGetter

from utils.array_operations import get_difference
from utils.getters.OperatorsCountGetter import Operatorscountgetter
from utils.templates.tables.basic_table import BasicTable
from types import SimpleNamespace
from typing import List, Any, Optional

from zoneinfo import ZoneInfo

from utils.mail.mail_sender import ReportsApiConfig, ReportInstanceUploader

import os


def normalize_date(d):
    """Преобразует datetime в строку YYYY-MM-DD, иначе возвращает как есть."""
    # сохраняем функцию (если нужна где-то ещё), но CSI и questionnaire будут передавать datetime напрямую
    return d.strftime("%Y-%m-%d") if isinstance(d, datetime) else d



@op(required_resource_keys={'source_dwh', 'report_utils', 'source_mp'},
    ins={"start_date": In(datetime), "end_date": In(datetime)})
def questionnaire_data_op(context, start_date, end_date):
    context.log.info(f"questionnaire_data_op: start_date={start_date} ({type(start_date)}), end_date={end_date} ({type(end_date)})")

    # Передаём даты как есть (datetime), не нормализуем
    getter = QuestionnaireDataGetter(
        start_date=start_date,
        end_date=end_date,
        conn=context.resources.source_dwh,
        trunc='day',
        questionnaire_id=4
    )
    res = getter.get_fullness_data()
    context.log.info(f"questionnaire_data = {res}")
    return res


from utils.getters.customer_satisfaction_index_aggregation_data_getter import \
    CustomerSatisfactionIndexAggregationDataGetter

def _to_dt(v):
    if isinstance(v, dt):
        return v
    if isinstance(v, str):
        # сначала пробуем ISO "YYYY-MM-DD"
        try:
            return dt.fromisoformat(v)
        except ValueError:
            # если формат другой, уточни маску
            return dt.strptime(v, "%Y-%m-%d")
    raise TypeError(f"Unsupported date value: {v!r}")
potential_fraud_statuses: dict[str, str] = {
    "notavailable": "Отсутствует",
    "away - CustomAwayReason1": "Обед",
    "dnd": "Блокировка по инициативе супервайзера",
    "custom1": "Кофе",
}

@op(required_resource_keys={'source_dwh', 'report_utils'})
def sl_data_joined_op(
        context,
        start_date,
        end_date,
):
    get_sl_data = NaumenServiceLevelDataGetter(
        start_date=start_date,
        end_date=end_date,
        conn=context.resources.source_dwh,
        trunc='day',
    )

    get_sl_data.filter_projects_by_group_array(get_sl_data.project_array)
    sl_data = get_sl_data.get_aggregated_data()

    get_sl_data.reload_projects()
    get_sl_data.filter_projects_by_group_array(get_sl_data.project_array, type_='IVR')
    sl_data_ivr = get_sl_data.get_aggregated_data_ivr()

    sl_data_joined = left_join_array(sl_data['items'], sl_data_ivr['items'], left_join_on='date')

    context.log.info(f"sl_data_joined = {sl_data_joined}")

    return sl_data_joined

@op(required_resource_keys={'source_dwh', 'report_utils', 'source_naumen_1', 'source_naumen_3'})
def occupancy_op(
        context,
        start_date,
        end_date,
):
    occupancy_getter = OccupancyDataGetter(
        start_date=start_date,
        end_date=end_date,
        conn_naumen_1=context.resources.source_naumen_1,
        conn_naumen_3 = context.resources.source_naumen_3,
        bpm_conn=context.resources.source_dwh,
    )
    occupancy = occupancy_getter.get_csf_data(position_ids=[35, 36, 37, 39], conn=occupancy_getter.conn_naumen_1)

    context.log.info(f"occupancy = {occupancy}")
    return occupancy

@op(required_resource_keys={'source_dwh', 'report_utils', 'source_naumen_3', 'source_naumen_1', 'source_cp'})
def operators_count_fact_op(
        context, start_date, end_date, report_date
):
    operators_count_getter = Operatorscountgetter(
        report_day=report_date,
        start_date=start_date,
        end_date=end_date,
        naumen_conn_1=context.resources.source_naumen_1,
        naumen_conn_3=context.resources.source_naumen_3,
        bpm_conn=context.resources.source_dwh,
        conn=context.resources.source_cp,
    )
    operators_count_plan = operators_count_getter.get_labor_data()
    context.log.info(f"operators_count_plan = {operators_count_plan}")
    operators_count_fact = operators_count_getter.get_fact_operatorts()
    context.log.info(f"operators_count_fact = {operators_count_fact}")

    erlang_and_fact_difference = get_difference(main=operators_count_plan, secondary=operators_count_fact, common_key="hour", decreasing="erlang", subtraction="fact_operators", difference="erlang_and_fact_operators", strict=True)
    erlang_and_fact_labor_difference = get_difference(main=operators_count_plan, secondary=operators_count_plan, common_key="hour", decreasing="erlang", subtraction="fact_labor", difference="erlang_and_fact_labor", strict=True)

    detailed_daily_data = left_join_array(operators_count_plan, operators_count_fact, left_join_on="hour")
    detailed_daily_data = left_join_array(detailed_daily_data, erlang_and_fact_difference, left_join_on="hour")
    detailed_daily_data = left_join_array(detailed_daily_data, erlang_and_fact_labor_difference, left_join_on="hour")

    context.log.info(f"detailed_daily_data = {detailed_daily_data}")
    return detailed_daily_data


@op(required_resource_keys={'source_dwh', 'report_utils', 'source_mp'},
    ins={"start_date": In(datetime), "end_date": In(datetime)})
def questionnaire_data_op(context, start_date, end_date):
    context.log.info(f"questionnaire_data_op: start_date={start_date} ({type(start_date)}), end_date={end_date} ({type(end_date)})")
    sd = normalize_date(start_date)
    ed = normalize_date(end_date)

    getter = QuestionnaireDataGetter(
        start_date=sd,
        end_date=ed,
        conn=context.resources.source_dwh,
        trunc='day',
        questionnaire_id=4
    )
    res = getter.get_fullness_data()
    context.log.info(f"questionnaire_data = {res}")
    return res


def _get_csi_data_impl(context, start_date, end_date, group=None, channel=None, period=None):
    # НЕ нормализуем даты — принимаем как есть
    context.log.info(f"_get_csi_data_impl called: start_date={start_date} ({type(start_date)}), end_date={end_date} ({type(end_date)}), period={period}")

    if period is None:
        period = SimpleNamespace(start_date=start_date, end_date=end_date, date_format=None, period='day')

    if group is None:
        group = ["General"]
    if channel is None:
        channel = ["naumen3", "naumen"]

    # Передаём start_date/end_date как есть. Передаём date_format если он есть в period,
    # чтобы класс сразу получил правильный date_format (и не потребовался setup_data_getter).
    getter = CustomerSatisfactionIndexAggregationDataGetter(
        start_date=start_date,
        end_date=end_date,
        conn=context.resources.source_dwh,
        channels=channel,
        trunc=getattr(period, "period", "day"),
        date_format=getattr(period, "date_format", None),
        set_date_format=getattr(period, "set_date_format", False),
        project_name=getattr(period, "project_name", "") if getattr(period, "project_name", None) else ""
    )

    context.log.info(f"Created CSI getter: start_date={getter.start_date} ({type(getter.start_date)}), end_date={getter.end_date} ({type(getter.end_date)}), trunc={getter.trunc}, date_format={getter.date_format}")

    # Если нужны фильтры проектов — вызываем методы геттера (оставляем логику)
    if hasattr(getter, 'filter_projects_by_group_array') and getattr(getter, 'selected_projects', None):
        getter.filter_projects_by_group_array(getter.selected_projects)

    # Вызываем нужный метод (как раньше)
    if hasattr(getter, 'get_combined_rating_result_with_group') and group:
        data = getter.get_combined_rating_result_with_group(group, channel)
    else:
        data = getter.get_combined_rating_result(group, channel)

    context.log.info(f" data (rows): {len(data) if data else 0}")
    return data


@op(required_resource_keys={'source_dwh', 'report_utils', 'source_mp'},
    ins={"start_date": In(datetime), "end_date": In(datetime)})
def get_csi_data(context, start_date, end_date, group=None, channel=None, period=None):
    data = _get_csi_data_impl(context, start_date, end_date, group, channel, period)
    context.log.info(f"get_csi_data: retrieved {len(data) if data else 0} rows")
    context.log.info(f" data = {data}")
    return data

@op(required_resource_keys={'source_dwh', 'report_utils', 'source_mp'},
    ins={"start_date": In(datetime), "end_date": In(datetime)})
def get_daily_detailed_data(context, start_date, end_date, group=None, channel=None, period=None):
    if group is None:
        group = ["General"]
    if channel is None:
        channel = ["naumen3", "naumen"]

    if period is None:
        period = SimpleNamespace(start_date=start_date, end_date=end_date, date_format=None, period='day')

    data = _get_csi_data_impl(context, start_date, end_date, group, channel, period)
    context.log.info(f"daily_data rows: {len(data) if data else 0}")
    context.log.info(f"daily_data columns: {data}")
    return data

@op(required_resource_keys={'source_dwh', 'report_utils', 'source_naumen_3', 'source_naumen_1', 'source_cp'})
def operators_count_fact_monthly_op(
    context, start_date, end_date, report_date
):
    operators_count_getter = Operatorscountgetter(
        report_day=report_date,
        start_date=start_date,
        end_date=end_date,
        naumen_conn_1=context.resources.source_naumen_1,
        naumen_conn_3=context.resources.source_naumen_3,
        bpm_conn=context.resources.source_dwh,
        conn=context.resources.source_cp,
    )
    operators_count_plan = operators_count_getter.get_monthly_fact_operators(start_date, end_date)
    context.log.info(f"operators_count_plan = {operators_count_plan}")
    return operators_count_plan

def _add_occupancy(rows):
    out = []
    for r in rows:
        ringing  = int(r.get("ringing", 0) or 0)
        speaking = int(r.get("speaking", 0) or 0)
        wrapup   = int(r.get("wrapup", 0) or 0)
        normal   = int(r.get("normal", 0) or 0)

        call_handling_time = ringing + speaking + wrapup
        productive_time = normal + call_handling_time
        r["occupancy"] = round(call_handling_time / productive_time * 100, 2) if productive_time > 0 else None
        out.append(r)
    return out

@op(required_resource_keys={'source_dwh', 'report_utils', 'source_naumen_3', 'source_naumen_1', 'source_cp'})
def get_daily_detailed_data_op(context, start_date, end_date, report_date, month_start, month_end, group, views, position_ids, project_name):

    def _items(x):
        return x.get("items", []) if isinstance(x, dict) else (x or [])

    def _ensure_hour(rows, date_key="date"):
        out = []
        for r in rows:
            rr = dict(r)  # не мутируем исходные данные
            if "hour" in rr and rr["hour"]:
                # нормализуем уже заданный hour -> HH:MM:SS
                s = str(rr["hour"]).strip().replace("T", " ")
                s = s[:-1] + "+00:00" if s.endswith("Z") else s
                try:
                    rr["hour"] = datetime.fromisoformat(s).time().strftime("%H:%M:%S")
                except Exception:
                    rr["hour"] = s.split()[-1] if ":" in s else "00:00:00"
            else:
                v = rr.get(date_key)
                if isinstance(v, datetime):
                    rr["hour"] = v.time().strftime("%H:%M:%S")
                elif isinstance(v, str):
                    s = v.strip().replace("T", " ")
                    s = s[:-1] + "+00:00" if s.endswith("Z") else s
                    try:
                        rr["hour"] = datetime.fromisoformat(s).time().strftime("%H:%M:%S")
                    except Exception:
                        rr["hour"] = s.split()[1] if (" " in s and ":" in s.split()[1]) else "00:00:00"
                else:
                    rr["hour"] = "00:00:00"
            out.append(rr)
        return out

    NAN_STRINGS: Set[str] = {"nan", "null", "none"}

    def _is_nullish(v: Any) -> bool:
        """True для None/NaN/Inf и строк 'nan'|'null'|'none' (без регистра/пробелов)."""
        if v is None:
            return True
        if isinstance(v, float):
            return math.isnan(v) or math.isinf(v)
        if isinstance(v, Decimal):
            return v.is_nan() or v.is_infinite()
        if isinstance(v, str) and v.strip().lower() in NAN_STRINGS:
            return True
        return False


    def _zeroize_rows(
            rows: List[Dict[str, Any]],
            keep_keys: Set[str] = {"date", "hour", "projectName"},
    ) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for r in rows or []:
            rr: Dict[str, Any] = {}
            for k, v in r.items():
                if k in keep_keys:
                    rr[k] = v
                else:
                    rr[k] = 0 if _is_nullish(v) else _smart_number(v)
            out.append(rr)
        return out

    operators_count_getter = Operatorscountgetter(
        report_day = report_date,
        start_date=start_date,
        end_date=end_date,
        view_ids = views,
        naumen_conn_1=context.resources.source_naumen_1,
        naumen_conn_3=context.resources.source_naumen_3,
        bpm_conn=context.resources.source_dwh,
        position_id=position_ids,
        conn=context.resources.source_cp,
    )
    fact_operators = operators_count_getter.get_fact_operatorts()
    fact_and_labor_operators_difference = get_difference(fact_operators, operators_count_getter.get_labor_data(), common_key="hour", decreasing="fact_operators", subtraction="fact_labor", difference="fact_operators_and_fact_labor", strict=True)
    fact_and_plan_operators_count = left_join_array(operators_count_getter.get_labor_data(), fact_operators, left_join_on="hour")
    ost_employers_data = left_join_array(fact_and_plan_operators_count, fact_and_labor_operators_difference, left_join_on="hour")
    context.log.info(f"ost_employers_data = {ost_employers_data}")

    context.log.info(f"start_date = {start_date}, end_date = {end_date}, report_date = {report_date}, month_start = {month_start}, month_end = {month_end}")

    naumen_service_getter = NaumenServiceLevelDataGetter(
        trunc='hour', start_date=report_date, end_date=end_date, conn=context.resources.source_dwh
    )
    setup_data_getter(naumen_service_getter, 'hour', start_date, end_date, report_date, month_start, month_end)
    naumen_service_getter.filter_projects_by_group_array(group)
    main_data_dynamic_general = naumen_service_getter.get_aggregated_data_set_date()

    naumen_service_getter.reload_projects()
    naumen_service_getter.filter_projects_by_group_array(group, type_='IVR')
    main_data_dynamic_general_ivr = naumen_service_getter.get_aggregated_data_ivr_set_date()

    main_data_report_day = left_join_array(
        main_data_dynamic_general, main_data_dynamic_general_ivr, left_join_on="date"
    )

    context.log.info(f"main_data_report_day = {main_data_report_day}")

    # ----- AHT -----
    avg_getter = AverageHandlingTimeDataGetter(
        dwh_conn=context.resources.source_dwh,
        start_date=start_date,
        end_date=end_date,
        report_date=report_date,
    )
    setup_data_getter(avg_getter, 'hour', start_date, end_date, report_date, month_start, month_end)
    avg_getter.filter_projects_by_group_array(group)
    avg_getter.set_date_format = True
    handling_items = avg_getter.get_aggregated_data()
    # handling_items = _ensure_hour(_items(handling_times_data_report_day), date_key="date")

    context.log.info(f"handling_items = {handling_items['items']}")

    # ----- Occupancy / CSF -----
    occ_getter = OccupancyDataGetter(
        bpm_conn=context.resources.source_dwh,
        conn_naumen_1=context.resources.source_naumen_1,
        conn_naumen_3=context.resources.source_naumen_3,
        start_date=report_date,
        end_date=end_date,
    )
    setup_data_getter(occ_getter, 'hour', start_date, end_date, report_date, month_start, month_end)


    get_dagster_logger().info(f"period in gddd= {occ_getter.period}")
    occ_getter.filter_projects_by_group_array(group)
    occupancy_items = occ_getter.get_aggregated_data_filtered(
        position_ids=position_ids, view_ids=views, project_name=project_name
    )

    context.log.info(f"occupancy_data_report_day = {occupancy_items['items']}")

    # ----- CSI -----
    csi_getter = CustomerSatisfactionIndexAggregationDataGetter(
        conn=context.resources.source_dwh,
        start_date=start_date,
        end_date=end_date,
    )
    setup_data_getter(csi_getter, 'hour', start_date, end_date, report_date, month_start, month_end)  # предполагаем, что это выставит date_field='hour'
    csi_data_report_day = csi_getter.get_combined_rating_result(group=group, channel=['naumen3', 'naumen'])
    # На всякий случай нормализуем 'hour'
    csi_items = csi_data_report_day
    csi_items = _ensure_hour(csi_items, date_key="hour")

    get_dagster_logger().info(f"csi_items = {csi_items}")

    getter_ivr_9999_transition = CallsDataGetter(conn=context.resources.source_naumen_3, start_date=start_date, end_date=end_date)

    context.log.info(f"getter_ivr_9999_transition = {getter_ivr_9999_transition.end_date}")

    setup_data_getter(getter_ivr_9999_transition, 'hour', start_date, end_date, report_date, month_start, month_end)

    data = getter_ivr_9999_transition.get_ivr_9999_by_hour(project_name)

    # ----- финальная сборка (всё по 'hour') -----
    detailed = left_join_array(main_data_report_day, main_data_report_day, left_join_on="hour")
    detailed = left_join_array(detailed, handling_items['items'], left_join_on="hour")
    detailed = left_join_array(detailed, occupancy_items['items'], left_join_on="hour")
    detailed = left_join_array(detailed, csi_items, left_join_on="hour")
    detailed = left_join_array(detailed, data, left_join_on="hour")

    daily_detailed_data = left_join_array(detailed, ost_employers_data, left_join_on="hour")

    daily_detailed_data = _zeroize_rows(daily_detailed_data)
    context.log.info("daily_detailed_data: все NaN/None/null заменены на 0")

    context.log.info(f"daily_detailed_data = {daily_detailed_data}")

    return daily_detailed_data




@op(required_resource_keys={'source_dwh', 'report_utils', 'source_naumen_3', 'source_naumen_1'})
def statuses_data_op(context, start_date, end_date):
    statuses_data = get_statuses_data(conn_naumen=context.resources.source_naumen_1, conn_naumen3=context.resources.source_naumen_3, bpm_conn=context.resources.source_dwh,
                                      start_date=start_date, end_date=end_date, position_ids=[35, 36, 37, 39], potential_fraud_statuses=potential_fraud_statuses)
    context.log.info(f"statuses_data {statuses_data}")
    return statuses_data

@op
def month_statuses_data_op(context, data):
    month_statuses_data = get_potential_fraud_statuses(data=data['month'], potential_fraud_statuses=potential_fraud_statuses)
    context.log.info(f"month_statuses_data = {month_statuses_data}")
    return month_statuses_data

@op
def count_statuses_data_op(context, data):
    count_statuses_data = get_potential_fraud_statuses(data=data['count'], potential_fraud_statuses=potential_fraud_statuses)
    context.log.info(f"count_statuses_data = {count_statuses_data}")
    return count_statuses_data

@op(required_resource_keys={'source_dwh', 'report_utils'})
def get_fullness_data(context, start_date, report_date):
    questionnaire_getter = QuestionnaireDataGetter(
        conn=context.resources.source_dwh,
        start_date=start_date,
        end_date=report_date,
        questionnaire_id=4,
    )
    fullness_data = questionnaire_getter.get_fullness_data()
    context.log.info(f"fullness_data = {fullness_data.to_dict(orient='records')}")
    return fullness_data.to_dict(orient="records")


def get_statuses_data(
    conn_naumen,
    conn_naumen3,
    bpm_conn,
    start_date: datetime.date,
    end_date: datetime.date,
    position_ids: list[int],
    potential_fraud_statuses
):
    sql = Path(BASE_SQL_PATH) / "bpm" / "get_active_users_logins.sql"
    query = sql.read_text(encoding="utf-8")
    params = {
        "position_ids": position_ids,
        "start_date": start_date,
        "end_date": end_date,
    }
    df = pd.read_sql_query(query, bpm_conn, params=params)
    staff_units_goo = df["login"].tolist()
    get_dagster_logger().info(f"staff_units_goo = {staff_units_goo}")

    sql_path = Path(BASE_SQL_PATH) / "bpm" / "get_login_group_goo_raw.sql"
    query = sql_path.read_text(encoding="utf-8")
    params = {
        "position_ids": position_ids,
        "start_date": start_date,
        "end_date": end_date,
    }
    df = pd.read_sql_query(query, bpm_conn, params=params)
    login_group_goo = {
        str(row["login"]): str(row["department_abbreviation"])
        for _, row in df.iterrows()
    }
    get_dagster_logger().info(f"login_group_goo = {login_group_goo}")

    def handler(data: List[Dict[str, Any]], key: str, employees: List[str]) -> Dict[str, Dict[str, int]]:
        """
        data: список словарей, где есть поля 'login', 'date', 'status' и key ('duration' или 'count')
        key: 'duration' или 'count'
        employees: список логинов (staffUnitsGOO)

        Возвращает словарь:
            {date: {status: sum(value)}}
        """
        response: Dict[str, Dict[str, int]] = {}
        employees_set = set(employees)

        for item in data:
            if item["login"] in employees_set and int(item.get(key, 0)) > 0:
                date = item["date"]
                status = item["status"]
                value = int(item[key])

                if date not in response:
                    response[date] = {status: value}
                else:
                    response[date][status] = response[date].get(status, 0) + value

        return response

    def response(data: Dict[str, Dict[str, int]], key: str) -> List[Dict[str, Any]]:
        """
        data: {date: {status: value}}
        key: 'duration' или 'count'

        Возвращает список словарей:
            [{'date': ..., 'status': ..., key: value}, ...]
        """
        result: List[Dict[str, Any]] = []
        for day, statuses in data.items():
            for status, value in statuses.items():
                result.append({"date": day, "status": status, key: value})
        return result

    def sum_duration(array1: List[Dict[str, Any]], array2: List[Dict[str, Any]], curr: str) -> List[Dict[str, Any]]:
        result: Dict[str, Dict[str, Any]] = {}
        merged = array1 + array2

        for item in merged:
            key = f"{item['date']}|{item['status']}"
            value = int(item.get(curr, 0) or 0)  # <-- привели к int
            if key not in result:
                result[key] = {"date": item["date"], "status": item["status"], curr: value}
            else:
                result[key][curr] += value

        return list(result.values())

    def get_user_statuses_by_connection(
            conn,
            start_date,
            end_date,
            staff_units_goo: Iterable[str],  # список логинов из StaffUnit::activeUnits(...)->pluck('login')
            login_group_goo: Dict[str, str],  # login → department (например title)
            potential_fraud_statuses: Dict[str, str],  # = $this->PotentialFraudStatuses
    ):
        """
          - month: агрегированные длительности (после handler + response)
          - count: агрегированные количества (после handler + response)
          - login: развернутая выборка по Fraud-статусам с маппингом статусов и департаментов
        """

        month_sql = Path(BASE_SQL_PATH) / "naumen" / "get_user_statuses_month_data.sql"
        month_sql = month_sql.read_text(encoding="utf-8")
        count_sql = Path(BASE_SQL_PATH) / "naumen" / "get_user_statuses_count_data.sql"
        count_sql = count_sql.read_text(encoding="utf-8")
        params = {"start_date": start_date, "end_date": end_date}
        get_dagster_logger().info(f"params = {params}")

        month_df = pd.read_sql_query(month_sql, conn, params=params)

        get_dagster_logger().info(f"month_df = {month_df}")

        count_df = pd.read_sql_query(count_sql, conn, params=params)

        # нормализация даты (чтобы было как в PHP date_trunc)
        month_df["date"] = pd.to_datetime(month_df["date"]).dt.strftime("%Y-%m-%d %H:%M:%S")
        count_df["date"] = pd.to_datetime(count_df["date"]).dt.strftime("%Y-%m-%d %H:%M:%S")

        staff_units_goo = set(staff_units_goo)
        month_filtered = month_df[month_df["login"].isin(staff_units_goo)].copy()
        count_filtered = count_df[count_df["login"].isin(staff_units_goo)].copy()

        month_records = month_filtered.to_dict("records")
        month_dict = handler(month_records, "duration", list(staff_units_goo))
        month_response = response(month_dict, "duration")

        count_records = count_filtered.to_dict("records")
        count_dict = handler(count_records, "count", list(staff_units_goo))
        count_response = response(count_dict, "count")

        fraud_keys = set(potential_fraud_statuses.keys())

        logins_acc: Dict[str, Dict[pd.Timestamp, Dict[str, float]]] = {}
        for _, row in month_df.iterrows():
            login = row["login"]
            date_ = row["date"]
            status = row["status"]
            dur = float(row["duration"])

            if (login in staff_units_goo) and (status in fraud_keys):
                logins_acc.setdefault(login, {}).setdefault(date_, {}).setdefault(status, 0.0)
                logins_acc[login][date_][status] += dur

        login_data: List[dict] = []
        for login, dates in logins_acc.items():
            for date_, statuses in dates.items():
                for status, duration in statuses.items():
                    login_data.append({
                        "date": date_,
                        "login": login,
                        "status": potential_fraud_statuses[status],
                        "duration": duration,
                        "department": login_group_goo.get(login)
                    })

        result = {
            "month": month_response,
            "count": count_response,
            "login": login_data
        }
        get_dagster_logger().info(f"get_user_by_connection = {result}f")
        return {
            "month": month_response,
            "count": count_response,
            "login": login_data
        }

    naumen = get_user_statuses_by_connection(conn=conn_naumen, start_date=start_date, end_date=end_date, staff_units_goo=staff_units_goo, login_group_goo=login_group_goo, potential_fraud_statuses=potential_fraud_statuses,)
    naumen3 = get_user_statuses_by_connection(conn=conn_naumen3, start_date=start_date, end_date=end_date, staff_units_goo=staff_units_goo, login_group_goo=login_group_goo, potential_fraud_statuses=potential_fraud_statuses,)

    month = sum_duration(naumen["month"], naumen3["month"], "duration")
    count = sum_duration(naumen["count"], naumen3["count"], "count")
    login_data = naumen["login"] + naumen3["login"]

    result = {
        "month": month,
        "count": count,
        "login": login_data
    }
    get_dagster_logger().info(f"get_statuses_data = {result}f")
    return result


def get_potential_fraud_statuses(data, potential_fraud_statuses):
    def norm(s): return (s or "").strip().lower()
    mapping = {norm(k): v for k, v in potential_fraud_statuses.items()}

    main, secondary = [], []
    for item in data:
        status_raw = item.get("status")
        status_norm = norm(status_raw)
        if status_norm in mapping:
            it = dict(item)  # копия
            it["status"] = mapping[status_norm]
            main.append(it)
        else:
            secondary.append(dict(item))  # тоже копия

    return {"main": main, "secondary": secondary}



def setup_data_getter(data_getter, period, start_date, end_date, report_date, month_start, month_end):
    if period == 'hour':
        data_getter.start_date = report_date
        data_getter.end_date = data_getter.end_date
        data_getter.date_field = 'hour'
        data_getter.set_hours_trunked_data()
    elif period == 'day':
        data_getter.start_date = data_getter.start_date
        data_getter.end_date = data_getter.end_date
        data_getter.set_days_trunked_data()
    elif period == 'month':
        data_getter.start_date = month_start
        data_getter.end_date = month_end
        data_getter.set_month_trunked_data()

@op(
    out={
        "general":          Out(list[str]),
        "saima":            Out(list[str]),
        "obank_money_bank": Out(list[str]),
        "agent":            Out(list[str]),
        "terminals":        Out(list[str]),
        "akcha_bulak":      Out(list[str]),
        "entrepreneurs":    Out(list[str]),
    },
)
def get_groups(context):
    groups = {
        "general": ["General"],
        "saima": ["Saima"],
        "obank_money_bank": ["O!Bank", "Money", "Bank"],
        "agent": ["Agent"],
        "terminals": ["Terminals"],
        "akcha_bulak": ["AkchaBulak"],
        "entrepreneurs": ["Entrepreneurs"],
    }
    context.log.info(f"[get_groups] {groups}")
    for key in ["general","saima","obank_money_bank","agent","terminals","akcha_bulak","entrepreneurs"]:
        vals = groups[key]
        yield Output(vals, key, metadata={"count": len(vals), "labels": ", ".join(vals)})

@op(
    out={
        "pos_35": Out(list[int]),
        "pos_39": Out(list[int]),
        "pos_36": Out(list[int]),
    },
)
def get_position_ids(context):
    payload = {"pos_35": [35], "pos_39": [39], "pos_36": [36]}
    context.log.info(f"[get_position_ids] {payload}")
    for key in ("pos_35", "pos_39", "pos_36"):
        yield Output(payload[key], key, metadata={"count": 1, "values": str(payload[key])})

@op(
    out={
        "views_8": Out(list[int]),
        "views_16": Out(list[int]),
        "views_9": Out(list[int]),
    },
)
def get_views(context):
    payload = {
        "views_8": [8],
        "views_16": [16],
        "views_9": [9]
    }
    context.log.info(f"[get_views] {payload}")
    for key in ("views_8", "views_16", "views_9"):
        yield Output(payload[key], key, metadata={"count": 1, "values": str(payload[key])})

@op(
    out={
        "line_707": Out(str),
        "saima_project_name": Out(str),
        "obank": Out(str),
        "oagent": Out(str),
        "oterminals": Out(str),
        "akcha_bulak_project_name": Out(str),
        "okassa": Out(str),
    },
)
def get_project_names(context):
    payload = {
        "line_707": "Линия 707",
        "saima_project_name": "Saima",
        "obank": "О!Банк",
        "oagent": "О!Агент",
        "oterminals": "О!Терминалы",
        "akcha_bulak_project_name": "Акча-Булак",
        "okassa": "О!Касса",
    }
    context.log.info(f"[get_project_names] {payload}")
    for key in ("line_707","saima_project_name","obank","oagent","oterminals","akcha_bulak_project_name","okassa"):
        yield Output(payload[key], key, metadata={"value": payload[key]})

@op(

    name="parse_dates_csf",

    out={
        "sd":  Out(dt),
        "ed":  Out(dt),
        "msd": Out(dt),
        "med": Out(dt),
        "rp": Out(dt)
    },
    required_resource_keys={"report_utils"},
)
def parse_dates(context):
    d = context.resources.report_utils.get_utils()
    sd  = _to_dt(getattr(d, "StartDate"))
    ed  = _to_dt(getattr(d, "EndDate"))
    msd = _to_dt(getattr(d, "MonthStart"))
    med = _to_dt(getattr(d, "MonthEnd"))
    rp = _to_dt(getattr(d,"ReportDate"))
    context.log.info(f"sd={sd} ed={ed} msd={msd} med={med} rp={rp}")
    yield Output(sd,  "sd")
    yield Output(ed,  "ed")
    yield Output(msd, "msd")
    yield Output(med, "med")
    yield Output(rp, "rp")


def render_day_profile(detailed_data):
    table = BasicTable()
    table.fields = [
        {"field": "hour", "title": "Часы"},
        {
            "field": "erlang",
            "title": "Эрланг",
            "paint": True,
            "summary": "sum",
            "round": 1,
        },
        {
            "field": "fact_labor",
            "title": "График",
            "paint": True,
            "summary": "sum",
            "round": 1,
        },
        {
            "field": "fact_operators",
            "title": "Факт",
            "paint": True,
            "summary": "sum",
            "round": 1,
        },
        {
            "field": "fact_operators_and_fact_labor",
            "title": "Дельта Факт и График",
            "customPaint": True,
            "summary": "sum",
            "round": 1,
        },
        {
            "field": "ivr_total",
            "title": "Поступило на IVR",
            "paint": True,
            "summary": "sum",
            "round": 1,
        },
        {
            "field": "total_to_operators",
            "title": "Распределено на операторов",
            "paint": True,
            "summary": "sum",
            "round": 1,
        },
        {
            "field": "answered",
            "title": "Отвечено операторами",
            "paint": True,
            "summary": "sum",
            "round": 1,
        },
        {
            "field": "total_queue",
            "title": "Завершены в очереди",
            "paint": True,
            "summary": "sum",
            "round": 1,
        },
        {
            "field": "SL",
            "title": "Service Level",
            "paint": "desc",
            "round": 1,
            "advancedSummaryAvg": {
                "numerator": "sla_answered",
                "denominator": "sla_total",
                "round": 1,
            },
        },
        {
            "field": "ACD",
            "title": "Handled Calls Rate",
            "paint": "desc",
            "round": 1,
            "advancedSummaryAvg": {
                "numerator": "answered",
                "denominator": "total_to_operators",
                "round": 1,
            },
        },
        {
            "field": "occupancy",
            "title": "Occupancy",
            "paint": "asc",
            "round": 1,
            "advancedSummaryAvg": {
                "numerator": "handling_time",
                "denominator": "product_time",
                "round": 1,
            },
        },
    ]

    table.Items = detailed_data
    return table


def _to_ymd(x: Any) -> str:
    if isinstance(x, (date, datetime)):
        return x.strftime("%Y-%m-%d")
    if x is None:
        return ""
    s = str(x)
    return s[:10]

@op(required_resource_keys={'source_dwh', 'report_utils', 'source_naumen_3', 'source_naumen_1'})
def get_projects_detail_data(context, start_date: Any, end_date: Any) -> List[Dict[str, Any]]:

    start_date = _to_ymd(start_date)
    end_date = _to_ymd(end_date)

    projects = [
        {"array": ["General", "TechSup"], "title": "ОСНОВНАЯ ЛИНИЯ - 707"},
        {"array": ["Saima"], "title": "САЙМА - 0706 909 000"},
        {"array": ["Money", "O!Bank"], "title": "О! ДЕНЬГИ + O! BANK - 999, 9999, 8008, 0700000999"},
        {"array": ["Terminals"], "title": "О! ТЕРМИНАЛЫ - 799"},
        {"array": ["Agent"], "title": "О! АГЕНТ - 5858"},
        {"array": ["Telesales"], "title": "TELESALES - Номер ГТМ"},
        {"array": ["Entrepreneurs"], "title": "ОФД - 7878"},
        {"array": ["AkchaBulak"], "title": "АКЧАБУЛАК"},
    ]

    if not projects or not start_date or not end_date:
        context.log.warning("get_projects_detail_data: пустые projects/start_date/end_date")
        return []

    out: List[Dict[str, Any]] = []

    for prj in projects:
        title = prj.get("title", "Без названия")
        groups = prj.get("array", [])
        context.log.info(f"[projects_detail_data] {title}: {groups} — {start_date}..{end_date}")

        sl_getter = NaumenServiceLevelDataGetter(
            trunc='day', start_date=start_date, end_date=end_date, conn=context.resources.source_dwh
        )
        sl_getter.filter_projects_by_group_array(groups)
        sl_data = sl_getter.get_aggregated_data_set_date()

        sl_getter.reload_projects()
        sl_getter.filter_projects_by_group_array(groups, type_='IVR')
        sl_data_ivr = sl_getter.get_aggregated_data_ivr_set_date()

        sl_joined = left_join_array(sl_data, sl_data_ivr, left_join_on="date")
        out.append({"project_title": title, "data_joined": sl_joined})

        context.log.info(f"{out}")

    return out

@op(
    required_resource_keys={"report_utils"},
    ins={"file_path": In(str, description="Абсолютный путь к PDF файлу.")},
    out=Out(dict, description="Ответ API как dict (JSON) либо {'status_code', 'text'}"),
    description=(
        "Загружает PDF-отчёт на /api/report-instances/upload/{report_id} (RV) "
        "по схеме: заголовок x-api-key и форма file/title/day. "
        "Дата 'day' берётся из report_utils.ReportDate (формат YYYY.MM.DD)."
    ),
)
def upload_report_instance_csf_op(context, file_path: str) -> Dict[str, Any]:
    cfg = context.op_config or {}

    # day только из report_utils
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
    report_id = int(cfg.get("report_id", 1))
    api_key = cfg.get("api_key") or os.getenv("REPORTS_API_KEY") or os.getenv("RV_REPORTS_API_KEY")
    if not api_key:
        raise Failure(
            description="Не задан API ключ. Укажите op_config.api_key или REPORTS_API_KEY / RV_REPORTS_API_KEY."
        )

    timeout_connect = int(cfg.get("timeout_connect", 10))
    timeout_read = int(cfg.get("timeout_read", 120))
    verify_ssl = bool(cfg.get("verify_ssl", True))

    title = cfg.get("title") or "ИСС | CSF статистика"

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