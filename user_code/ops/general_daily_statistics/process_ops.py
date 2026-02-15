import base64
import copy
import io
import json
import math
from collections import OrderedDict
from datetime import datetime, date, timedelta

import requests
from dagster import op, get_dagster_logger, In, Out, Failure
from matplotlib import pyplot as plt

from resources import ReportUtils
from utils.array_operations import left_join_array, array_merge, get_difference, left_join_array_dict
from utils.getters.OperatorsCountGetter import Operatorscountgetter
from utils.getters.average_handling_time_data_getter import AverageHandlingTimeDataGetter
from utils.getters.calls_data_getter import CallsDataGetter
from utils.getters.critical_error_accuracy_data_getter import CriticalErrorAccuracyDataGetter
from utils.getters.customer_satisfaction_index_aggregation_data_getter import \
    CustomerSatisfactionIndexAggregationDataGetter
from utils.getters.customer_satisfaction_index_data_getter import CustomerSatisfactionIndexDataGetter
from utils.getters.ivr_transaction_data_getter import IVRTransactionDataGetter
from utils.getters.jira_data_getter_next import JiraDataGetterNext
from utils.getters.messaging_platform_data_getter import MessagingPlatformDataGetter
from utils.getters.naumen_service_level_data_getter import NaumenServiceLevelDataGetter
from utils.getters.occupancydatagetter import OccupancyDataGetter
from utils.getters.omni_data_getter import OmniDataGetter
from utils.getters.repeat_calls_data_getter import RepeatCallsDataGetter
from utils.getters.task_solving_speed_data_getter import TaskSolvingSpeedDataGetter
from utils.report_data_collector import set_jira_issues_data
from utils.templates.charts.multi_axis_chart import MultiAxisChart
from utils.transformers.mp_report import _parse_dt_any

from zoneinfo import ZoneInfo

from utils.mail.mail_sender import ReportsApiConfig, ReportInstanceUploader

from typing import Dict, Any, List

import os

gpo_position_ids = [10, 41, 46]
goo_position_ids = [35, 36, 39]

mp_included_projects = [1,4,5,8,9,10,11,12,15,20]
project_position_names = {
        35: "ГОО1 (НУР)",
        36: "ГОО2 (Грин)",
        39: "ГОО4 (Saima)",
        10: "ГПО1 (НУР)",
        46: "ГПО2 (ГРИН)",
        41: "ГПО4 (Saima)",
    }

projects_name_array = {
            "General": "Линия 707",
            "Saima": "Saima",
            "Terminals": "О!Терминалы",
            "O!Bank": "О!Bank + O!Деньги",
            "AkchaBulak": "Акча Булак",
            "Entrepreneurs": "O!Касса",
            "Agent": "О!Агент",
            "O!Market": "О!Маркет",
        }

all_projects = {
            "General": ["General"],
            "Saima": ["Saima"],
            "Terminals": ["Terminals"],
            "O!Bank": ["O!Bank", "Money", "Bank"],
            "AkchaBulak": ["AkchaBulak"],
            "Entrepreneurs": ["Entrepreneurs"],
            "Agent": ["Agent"],
            "O!Market": ["O!Market"],
        }

general_projects_array = [
            "General",
            "Saima",
            "Terminals",
            "O!Bank",
            "Money",
            "Bank",
            "AkchaBulak",
            "Entrepreneurs",
            "Agent",
            "O!Market",
        ]

projects_names = "Все проекты"
callers = ["*707", "707", "0705700700"]

jira_projects = {
    "10005": "SAIMA",
    "10101": "SAIMA",
    "10100": "SAIMA",
    "10102": "SAIMA",
    "10104": "SAIMA",
    "10108": "SAIMA",
    "10209": "SAIMA",
    "10211": "SAIMA",
    "10212": "SAIMA",
    "10306": "SAIMA",
    "10307": "SAIMA",
    "10308": "SAIMA",
    "10309": "SAIMA",
    "10310": "SAIMA",
    "10311": "SAIMA",
    "10312": "SAIMA",
    "10314": "SAIMA",
    "10317": "SAIMA",
    "10324": "SAIMA",
    "10322": "SAIMA",
    "10401": "SAIMA",
    "10406": "SAIMA",
    "10501": "SAIMA",
    "10502": "SAIMA",
    "10512": "SAIMA",
    "10513": "SAIMA",
    "10600": "SAIMA",
    "10802": "SAIMA",
    "10813": "SAIMA",
    "10901": "SAIMA",
    "10904": "SAIMA",
    "11000": "SAIMA",
    "11102": "SAIMA",
    "11302": "SAIMA",
    "11505": "SAIMA",
    "11606": "SAIMA",
    "11707": "SAIMA",
    "11708": "SAIMA",
    "11718": "SAIMA",
    "11722": "SAIMA",
    "12000": "SAIMA",
    "12200": "SAIMA",
    "12208": "SAIMA",
    "12209": "SAIMA",
    "12300": "SAIMA",
    "12302": "SAIMA",
    "12313": "SAIMA",
    "12908": "SAIMA",
    "13100": "SAIMA",
    "13207": "SAIMA",
    "13614": "SAIMA",
    "13900": "SAIMA",
}

project_titles = {
    "10301": "Линия - 707",
    "10302": "Линия - 707",
    "10303": "Линия - 707",
    "10306": "Линия - 707",
    "10307": "Линия - 707",
    "10308": "Линия - 707",
    "10304": "О! Деньги",
    "11309": "О! Терминал",
    "11503": "О! Агент",
    "12700": "Акча Булак",
    "11603": "О!Касса",
}

def _normalize_occupancy_payload(payload: Any) -> List[Dict[str, Any]]:
    if payload is None:
        return []

    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            return []

    if hasattr(payload, "to_dict"):
        try:
            payload = payload.to_dict(orient="records")
        except Exception:
            payload = []

    if isinstance(payload, dict):
        items = payload.get("items")
        payload = items if isinstance(items, list) else [payload]

    out: List[Dict[str, Any]] = []
    if isinstance(payload, list):
        for r in payload:
            if not isinstance(r, dict):
                continue
            d = r.get("date") or r.get("day") or r.get("hour") or r.get("period")
            occ = r.get("occupancy")
            if occ is None:
                occ = r.get("value") or r.get("percent") or r.get("pct")
            if d is None or occ is None:
                continue
            try:
                occ_val = float(occ)
            except Exception:
                continue
            out.append({"date": str(d), "occupancy": occ_val})

    # Дедуп без сортировки: сохраняем порядок первой встречи даты;
    # при повторе обновляем значение, но порядок не меняется.
    dedup = OrderedDict()
    for row in out:
        dedup[row["date"]] = row["occupancy"]

    return [{"date": d, "occupancy": v} for d, v in dedup.items()]

# Occupancy
@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def get_occupancy_data_dynamic_general_goo_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    get_occupancy_data_dynamic = OccupancyDataGetter(bpm_conn=context.resources.source_dwh, conn_naumen_1=context.resources.source_naumen_1, conn_naumen_3=context.resources.source_naumen_3,
                                                     start_date=start_date, end_date=end_date)
    get_occupancy_data_dynamic.set_days_trunked_data()
    get_occupancy_data_dynamic.filter_projects_by_group_array(general_projects_array)

    occupancy_data_dynamic_general_goo = get_occupancy_data_dynamic.get_aggregated_data(position_ids=goo_position_ids)

    occupancy_data_dynamic_general_goo = _normalize_occupancy_payload(occupancy_data_dynamic_general_goo)

    context.log.info(f"occupancy_data_dynamic_general_goo = {occupancy_data_dynamic_general_goo}")
    return occupancy_data_dynamic_general_goo

# Occupancy - month
@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def get_occupancy_data_month_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    get_occupancy_data_month = OccupancyDataGetter(bpm_conn=context.resources.source_dwh, conn_naumen_3=context.resources.source_naumen_3, conn_naumen_1=context.resources.source_naumen_1,
                                                   start_date=month_start, end_date=month_end, )
    get_occupancy_data_month.set_months_trunked_data()
    get_occupancy_data_month.filter_projects_by_group_array(general_projects_array)
    occupancy_data_month_general = get_occupancy_data_month.get_aggregated_data(position_ids=goo_position_ids)
    context.log.info(f"occupancy_data_month_general = {occupancy_data_month_general}")
    data = occupancy_data_month_general

    if isinstance(data, dict):
        items = data.get("items", [])
    elif isinstance(data, list):
        items = data
    elif hasattr(data, "to_dict"):  # pandas DataFrame
        try:
            items = data.to_dict(orient="records")
        except Exception:
            items = []
    else:
        items = []

    occupancy_current_month_general = (
        round(float(items[0].get("occupancy", 0)), 1) if items else 0
    )

    context.log.info(f"type={type(occupancy_data_month_general)} sample={str(occupancy_data_month_general)[:300]}")

    context.log.info(f"occupancy_current_month_general = {occupancy_current_month_general}")
    return occupancy_current_month_general

#Utilization Rate for day period
# GOO
@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def get_goo_utilization_rate_date_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    occupancy_data_getter = OccupancyDataGetter(bpm_conn=context.resources.source_dwh,
                                                   conn_naumen_3=context.resources.source_naumen_3,
                                                   conn_naumen_1=context.resources.source_naumen_1,
                                                   start_date=date_list[0], end_date=end_date)
    occupancy_data_getter.set_days_trunked_data()
    occupancy_data_getter.filter_projects_by_group_array(general_projects_array)
    goo_utilization_rate_date = occupancy_data_getter.get_daily_utilization_rate_data(position_ids=goo_position_ids)
    context.log.info(f"goo_utilization_rate_date = {goo_utilization_rate_date}")
    return goo_utilization_rate_date

# GPO
@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def get_gpo_utilization_rate_date_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    occupancy_data_getter = OccupancyDataGetter(bpm_conn=context.resources.source_dwh,
                                                   conn_naumen_3=context.resources.source_naumen_3,
                                                   conn_naumen_1=context.resources.source_naumen_1,
                                                   start_date=date_list[0], end_date=end_date)
    occupancy_data_getter.set_days_trunked_data()
    occupancy_data_getter.filter_projects_by_group_array(general_projects_array)
    gpo_utilization_rate_date = occupancy_data_getter.get_daily_utilization_rate_data(position_ids=gpo_position_ids)
    context.log.info(f"gpo_utilization_rate_date = {gpo_utilization_rate_date}")
    return gpo_utilization_rate_date

#Utilization Rate for Month
@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def get_goo_utilization_rate_date_month_general_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    occupancy_data_getter = OccupancyDataGetter(bpm_conn=context.resources.source_dwh,
                                                conn_naumen_3=context.resources.source_naumen_3,
                                                conn_naumen_1=context.resources.source_naumen_1,
                                                start_date=date_list[0], end_date=end_date)
    occupancy_data_getter.set_days_trunked_data()
    occupancy_data_getter.filter_projects_by_group_array(general_projects_array)
    goo_utilization_rate_date_month_general = occupancy_data_getter.get_daily_utilization_rate_data(position_ids=goo_position_ids, period='Month')
    goo_utilization_rate_date_month_general_rounded = round(goo_utilization_rate_date_month_general['utilization_rate'], 1)
    context.log.info(f"goo_utilization_rate_date_month_general_rounded = {goo_utilization_rate_date_month_general_rounded}")
    return goo_utilization_rate_date_month_general_rounded

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def get_gpo_utilization_rate_date_month_general_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    occupancy_data_getter = OccupancyDataGetter(bpm_conn=context.resources.source_dwh,
                                                conn_naumen_3=context.resources.source_naumen_3,
                                                conn_naumen_1=context.resources.source_naumen_1,
                                                start_date=date_list[0], end_date=end_date)
    occupancy_data_getter.set_days_trunked_data()
    occupancy_data_getter.filter_projects_by_group_array(general_projects_array)
    gpo_utilization_rate_date_month_general = occupancy_data_getter.get_daily_utilization_rate_data(position_ids=goo_position_ids, period='Month')

    context.log.info(f"gpo_utilization_rate_date_month_general = {gpo_utilization_rate_date_month_general['utilization_rate']}")

    gpo_utilization_rate_date_month_general_rounded = round(gpo_utilization_rate_date_month_general['utilization_rate'], 1)
    context.log.info(f"gpo_utilization_rate_date_month_general_rounded = {gpo_utilization_rate_date_month_general_rounded}")
    return gpo_utilization_rate_date_month_general_rounded

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def get_aggregated_data_by_position_id_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    aggregated_data_by_position_id = []
    for position_id in goo_position_ids:
        occupancy_data_getter = OccupancyDataGetter(bpm_conn=context.resources.source_dwh,conn_naumen_3=context.resources.source_naumen_3,
                                                conn_naumen_1=context.resources.source_naumen_1,
                                                start_date=start_date, end_date=end_date, project_position_names=project_position_names, position_ids=[position_id])

        occupancy_data_getter.set_date_format = True

        occupancy_data_getter.set_days_trunked_data()

        context.log.info(f"project_position_names: {project_position_names}")

        project_position_utilization_rate_date = occupancy_data_getter.get_daily_utilization_rate_data(position_ids=[position_id])
        get_dagster_logger().info(f"project_position_utilization_rate_date = {project_position_utilization_rate_date}")
        occupancy_data_getter.reload_projects()
        project_position_occupancy_data_dynamic = occupancy_data_getter.get_aggregated_data(position_ids=[position_id])
        context.log.info(f"project_position_occupancy_data_dynamic = {project_position_occupancy_data_dynamic}")
        temp = left_join_array(project_position_utilization_rate_date, project_position_occupancy_data_dynamic['items'], left_join_on='date')

        aggregated_data_by_position_id.extend(temp)
    context.log.info(f"aggregated_data_by_position_id = {aggregated_data_by_position_id}")
    return aggregated_data_by_position_id


@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def get_all_utilization_rate_date_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    occupancy_data_getter = OccupancyDataGetter(bpm_conn=context.resources.source_dwh,
                                                conn_naumen_3=context.resources.source_naumen_3,
                                                conn_naumen_1=context.resources.source_naumen_1,
                                                start_date=start_date, end_date=end_date)
    occupancy_data_getter.set_days_trunked_data()
    all_utilization_rate_date = occupancy_data_getter.get_daily_utilization_rate_data(position_ids=goo_position_ids)
    context.log.info(f"all_utilization_rate_date = {all_utilization_rate_date}")
    return all_utilization_rate_date

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def get_utilization_rate_date_month_general_op(context, all_utilization_rate_date):
    utilization_rate_date_month_general = round(all_utilization_rate_date[0]['utilization_rate'], 1)
    context.log.info(f"utilization_rate_date_month_general = {utilization_rate_date_month_general}")
    return utilization_rate_date_month_general

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def get_all_occupancy_date(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    occupancy_data_getter = OccupancyDataGetter(bpm_conn=context.resources.source_dwh,
                                                conn_naumen_3=context.resources.source_naumen_3,
                                                conn_naumen_1=context.resources.source_naumen_1,
                                                start_date=start_date, end_date=end_date)
    occupancy_data_getter.set_days_trunked_data()
    occupancy_data_getter.reload_projects()
    all_occupancy_date = occupancy_data_getter.get_aggregated_data(position_ids=goo_position_ids)

    context.log.info(f"before_all_occupancy_date = {all_occupancy_date}")

    all_occupancy_date = _normalize_occupancy_payload(all_occupancy_date)

    context.log.info(f"all_occupancy_date = {all_occupancy_date}")

    return all_occupancy_date

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def get_occupancy_data_month_general_op(context, all_occupancy_date):
    context.log.info(f"type={type(all_occupancy_date)} sample={str(all_occupancy_date)[:300]}")

    def _extract_occupancy(x) -> float:
        if x is None:
            return 0.0
        if isinstance(x, (int, float)):
            return float(x)
        if isinstance(x, dict):
            if "occupancy" in x:
                return float(x.get("occupancy") or 0)
            if "items" in x and isinstance(x["items"], list) and x["items"]:
                first = x["items"][0]
                if isinstance(first, dict):
                    return float(first.get("occupancy", 0) or 0)
                if isinstance(first, (int, float)):
                    return float(first)
            return 0.0
        if isinstance(x, (list, tuple)):
            if not x:
                return 0.0
            first = x[0]
            if isinstance(first, dict):
                return float(first.get("occupancy", 0) or 0)
            if isinstance(first, (int, float)):
                return float(first)
        return 0.0

    val = _extract_occupancy(all_occupancy_date)
    occupancy_date_month_general = round(val, 1)
    context.log.info(f"occupancy_date_month_general = {occupancy_date_month_general}")
    return occupancy_date_month_general

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def get_cea_data_dynamic_by_groups_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    get_cea_data_dynamic = CriticalErrorAccuracyDataGetter(bpm_conn=context.resources.source_dwh, start_date=start_date, end_date=end_date)
    get_cea_data_dynamic.set_days_trunked_data()
    get_cea_data_dynamic.project_names = projects_name_array
    cea_data_dynamic_by_groups = get_cea_data_dynamic.get_aggregated_data_by_project()
    context.log.info(f"cea_data_dynamic_by_groups = {cea_data_dynamic_by_groups}")
    return cea_data_dynamic_by_groups

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def get_graphic_aggregated_data_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    graphic_aggregated_data = []
    projects_name_array['AllProjects'] = "Все проекты"

    for key, project in all_projects.items():
        get_main_data_by_project_name = NaumenServiceLevelDataGetter(conn=context.resources.source_dwh, start_date=start_date, end_date=end_date,
                                                                   project_names=projects_name_array)

        get_main_data_by_project_name.set_days_trunked_data()
        get_main_data_by_project_name.set_date_format = True
        get_main_data_by_project_name.filter_projects_by_group_array(project)
        get_main_data_by_project_name.project_name = key
        main_data_by_project_name = get_main_data_by_project_name.get_aggregated_data()

        get_main_data_by_project_name.reload_projects()
        get_main_data_by_project_name.set_date_format = True

        context.log.info(f"key: {key}")

        if key == 'O!Market':
            get_main_data_by_project_name.filter_projects_by_group_array(project, 'Operator')
        else:
            get_main_data_by_project_name.filter_projects_by_group_array(project, 'IVR')

        main_data_by_project_name_ivr = get_main_data_by_project_name.get_aggregated_data_ivr()

        temp = left_join_array(main_data_by_project_name['items'], main_data_by_project_name_ivr['items'], left_join_on='date')

        get_handling_times_data_by_project_name = AverageHandlingTimeDataGetter(dwh_conn=context.resources.source_dwh, start_date=start_date, end_date=end_date)
        get_handling_times_data_by_project_name.set_date_format = True
        get_handling_times_data_by_project_name.set_days_trunked_data()
        get_handling_times_data_by_project_name.filter_projects_by_group_array(project)
        get_handling_times_data_by_project_name.project_names = projects_name_array
        get_handling_times_data_by_project_name.project_name = key
        handling_times_data_by_project_name = get_handling_times_data_by_project_name.get_aggregated_data()


        temp = left_join_array(temp, handling_times_data_by_project_name['items'], 'date')

        get_rc_data_by_project_name = RepeatCallsDataGetter(start_date=start_date, end_date=end_date, dwh_conn=context.resources.source_dwh)
        get_rc_data_by_project_name.SetDateFormat = True
        get_rc_data_by_project_name.ProjectName = key
        get_rc_data_by_project_name.ProjectNames = projects_name_array
        get_rc_data_by_project_name.set_days_trunked_data()
        rc_data_by_project_name = get_rc_data_by_project_name.get_aggregated_data(table='00:00:00', groups=project)


        temp = left_join_array(temp, rc_data_by_project_name['items'], 'date')

        ivr9999 = CallsDataGetter(conn=context.resources.source_naumen_3, start_date=start_date, end_date=end_date)
        ivr9999.StartDate = start_date
        ivr9999.EndDate = end_date
        ivr9999.set_days_trunked_data()

        data_9999 = ivr9999.get_ivr_9999_by_day(key)
        for row in data_9999:
            d = (row.get('date') or '').strip()
            if d:
                try:
                    row['date'] = datetime.strptime(d, '%d.%m.%Y').strftime('%Y-%m-%d 00:00:00')
                except ValueError:
                    row['date'] = None
        data_9999 = [r for r in data_9999 if r.get('date')]

        get_dagster_logger().info(f"data_9999: {data_9999}")

        temp = left_join_array(temp, data_9999, 'date')

        for r in temp:
            base = r.get('ivr_total')
            plus = r.get('ivr_from_9999')
            base = 0.0 if (base is None or (isinstance(base, float) and math.isnan(base))) else float(base)
            plus = 0.0 if (plus is None or (isinstance(plus, float) and math.isnan(plus))) else float(plus)
            r['ivr_total'] = int(round(base + plus))
            r.pop('ivr_from_9999', None)

        graphic_aggregated_data = array_merge(graphic_aggregated_data, temp)
        context.log.debug(f"project={key}, appended={len(temp)}")

    context.log.info(f"graphic_aggregated_data len={len(graphic_aggregated_data)}")

    context.log.info(f"graphic_aggregated_data: {graphic_aggregated_data}")

    return graphic_aggregated_data

#7.6
@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def get_csi_data_by_project_name_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    get_csi_data_by_project_name = CustomerSatisfactionIndexAggregationDataGetter(conn=context.resources.source_dwh, start_date=start_date, end_date=end_date)
    get_csi_data_by_project_name.set_date_format = True
    get_csi_data_by_project_name.set_days_truncated_data()
    csi_data_by_project_name = get_csi_data_by_project_name.get_combined_rating_result_with_group(["General", "O!Bank", "Agent", "Terminals", "Saima","AkchaBulak","Entrepreneurs"], ['naumen', 'naumen3'])

    context.log.info(f"csi_data_by_project_name = {csi_data_by_project_name}")
    return csi_data_by_project_name

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def get_csi_data_dynamic_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    get_csi_data_by_project_name = CustomerSatisfactionIndexAggregationDataGetter(conn=context.resources.source_dwh, start_date=start_date, end_date=end_date)
    get_csi_data_by_project_name.set_date_format = True
    get_csi_data_by_project_name.set_days_truncated_data()

    context.log.info(f"start_date_in_op: {start_date}")
    context.log.info(f"end_date_in_op: {end_date}")

    csi_data_dynamic = get_csi_data_by_project_name.get_combined_rating_result(["General", "O!Bank", "Agent", "Terminals", "Saima","AkchaBulak","Entrepreneurs"], ['naumen', 'naumen3', 'cp'])
    context.log.info(f"csi_data_dynamic = {csi_data_dynamic}")
    return csi_data_dynamic

# AHT, ART
@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def get_handling_times_data_dynamic_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    get_handling_times_data_dynamic = AverageHandlingTimeDataGetter(dwh_conn=context.resources.source_dwh, start_date=start_date, end_date=end_date)
    get_handling_times_data_dynamic.set_days_trunked_data()
    get_handling_times_data_dynamic.filter_projects_by_group_array(general_projects_array)
    handling_times_data_dynamic_general = get_handling_times_data_dynamic.get_aggregated_data()
    context.log.info(f"handling_times_data_dynamic_general = {handling_times_data_dynamic_general}")
    return handling_times_data_dynamic_general

# AHT, ART - month
@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def get_handling_times_data_dynamic_month_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    get_handling_times_data_dynamic_month = AverageHandlingTimeDataGetter(dwh_conn=context.resources.source_dwh, start_date=month_start, end_date=month_end)
    get_handling_times_data_dynamic_month.set_months_trunked_data()
    get_handling_times_data_dynamic_month.filter_projects_by_group_array(general_projects_array)
    handling_times_data_dynamic_month_general = get_handling_times_data_dynamic_month.get_aggregated_data()
    context.log.info(f"handling_times_data_dynamic_month_general = {handling_times_data_dynamic_month_general}")
    return handling_times_data_dynamic_month_general

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def get_aht_general_average_data_op(context, handling_times_data_dynamic_month_general):
    aht_general_average_data = round(handling_times_data_dynamic_month_general['items'][0]['average_handling_time'], 1)
    context.log.info(f"aht_general_average_data = {aht_general_average_data}")
    return aht_general_average_data

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def get_average_ringing_time_current_month_general_op(context, handling_times_data_dynamic_month_general):
    average_ringing_time_current_month_general = round(handling_times_data_dynamic_month_general['items'][0]['average_ringing_time'], 1)
    context.log.info(f"average_ringing_time_current_month_general = {average_ringing_time_current_month_general}")
    return average_ringing_time_current_month_general

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def get_average_holding_time_current_month_general_op(context,handling_times_data_dynamic_month_general):
    average_holding_time_current_month_general = round(handling_times_data_dynamic_month_general['items'][0]['average_holding_time'], 1)
    context.log.info(f"average_holding_time_current_month_general = {average_holding_time_current_month_general}")
    return average_holding_time_current_month_general

# CSI - Текущий месяц
@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def get_csi_data_month_main_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    get_csi_data_by_project_name = CustomerSatisfactionIndexAggregationDataGetter(conn=context.resources.source_dwh, start_date=start_date, end_date=end_date)
    get_csi_data_by_project_name.set_date_format = True
    get_csi_data_by_project_name.set_months_truncated_data()
    csi_data_month_main = get_csi_data_by_project_name.get_combined_rating_result(["General", "O!Bank", "Agent", "Terminals", "Saima","AkchaBulak","Entrepreneurs"], ['naumen', 'naumen3'])
    csi_data_month_main = dict(csi_data_month_main[0]) if csi_data_month_main else {
        "customer_satisfaction_index": 0,
        "conversion": 0
    }
    context.log.info(f"csi_data_month_main = {csi_data_month_main}")
    return csi_data_month_main

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def csi_general_average_data_op(context, csi_data_month_main):
    csi_general_average_data = round(csi_data_month_main['customer_satisfaction_index'], 1)
    context.log.info(f"csi_general_average_data = {csi_general_average_data}")
    return csi_general_average_data

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def conversion_average_data_op(context, csi_data_month_main):
    conversion_average_data = round(csi_data_month_main['conversion'], 1)
    context.log.info(f"conversion_average_data = {conversion_average_data}")
    return conversion_average_data

# RC All PROJECTS
@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def rc_data_dynamic_main_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    get_rc_data_dynamic = RepeatCallsDataGetter(start_date=start_date, end_date=end_date, dwh_conn=context.resources.source_dwh)
    get_rc_data_dynamic.set_days_trunked_data()
    rc_data_dynamic_main = get_rc_data_dynamic.get_aggregated_data(table='00:00:00', groups=general_projects_array)
    context.log.info(f"rc_data_dynamic_main = {rc_data_dynamic_main}")
    return rc_data_dynamic_main


# RC - month
@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def rc_general_average_data_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    get_rc_data_month = RepeatCallsDataGetter(start_date=month_start, end_date=month_end, dwh_conn=context.resources.source_dwh)
    get_rc_data_month.set_months_trunked_data()
    rc_data_dynamic_main_month = get_rc_data_month.get_aggregated_data(table='00:00:00', groups=general_projects_array)
    rc_general_average_data = round(rc_data_dynamic_main_month['items'][0]['repeat_calls'], 1)
    context.log.info(f"rc_general_average_data {rc_general_average_data}")
    return rc_general_average_data

# SL, HCR All PROJECTS
@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def main_data_dynamic_general_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    get_main_data_dynamic = NaumenServiceLevelDataGetter(start_date=start_date, end_date=end_date, conn=context.resources.source_dwh)
    get_main_data_dynamic.set_days_trunked_data()
    get_main_data_dynamic.filter_projects_by_group_array(general_projects_array)
    main_data_dynamic_general = get_main_data_dynamic.get_aggregated_data()
    context.log.info(f"main_data_dynamic_general = {main_data_dynamic_general}")
    return main_data_dynamic_general

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def main_data_dynamic_general_ivr_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    get_main_data_dynamic = NaumenServiceLevelDataGetter(start_date=start_date, end_date=end_date,
                                                       conn=context.resources.source_dwh)
    get_main_data_dynamic.set_days_trunked_data()
    get_main_data_dynamic.filter_projects_by_group_array(general_projects_array)
    get_main_data_dynamic.reload_projects()
    get_main_data_dynamic.filter_projects_by_group_array(general_projects_array, 'IVR')
    main_data_dynamic_general_ivr = get_main_data_dynamic.get_aggregated_data_ivr()
    context.log.info(f"main_data_dynamic_general_ivr = {main_data_dynamic_general_ivr}")
    return main_data_dynamic_general_ivr

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def self_service_rate_current_month_general_array_op(context, main_data_dynamic_general_ivr):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    get_main_data_dynamic = NaumenServiceLevelDataGetter(start_date=month_start, end_date=month_end, conn=context.resources.source_dwh)
    get_main_data_dynamic.set_months_trunked_data()
    self_service_rate_current_month_general_array = (round(main_data_dynamic_general_ivr['items'][0]['SSR'], 1)
                                                     if main_data_dynamic_general_ivr['items'][0]['SSR'] is not None else 0 )
    context.log.info(f"self_service_rate_current_month_general_array {self_service_rate_current_month_general_array}")
    return self_service_rate_current_month_general_array


# SL, HCR - Объедение данных для получения профиля дня
@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def graphic_calls_dynamic_general_joined_op(context, main_data_dynamic_general, main_data_dynamic_general_ivr):
    graphic_calls_dynamic_general_joined = left_join_array_dict(main_data_dynamic_general, main_data_dynamic_general_ivr, left_join_on='date')
    context.log.info(f'graphic_calls_dynamic_general_joined {graphic_calls_dynamic_general_joined}')
    return graphic_calls_dynamic_general_joined

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def max_ivr_total_op(context, graphic_calls_dynamic_general_joined):
    max_ivr_total = None
    for calls_data in graphic_calls_dynamic_general_joined:
        if max_ivr_total is None or calls_data["ivr_total"] > max_ivr_total:
            max_ivr_total = calls_data["ivr_total"]
    context.log.info(f"max_ivr_total {max_ivr_total}")
    return max_ivr_total

# SL, HCR - month
@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def main_data_month_general_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    get_main_data_month = NaumenServiceLevelDataGetter(start_date=month_start, end_date=month_end, conn=context.resources.source_dwh)
    get_main_data_month.set_months_trunked_data()
    get_main_data_month.filter_projects_by_group_array(general_projects_array)
    main_data_month_general = get_main_data_month.get_aggregated_data()
    context.log.info(f"main_data_month_general {main_data_month_general}")
    return main_data_month_general

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def sl_general_average_data_op(context, main_data_month_general):
    sl_general_average_data = (
        round(main_data_month_general['items'][0]['SL'], 1)
        if main_data_month_general['items'][0]['SL'] is not None
        else 0
    )
    context.log.info(f"sl_general_average_data {sl_general_average_data}")
    return sl_general_average_data

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def acd_general_average_data_op(context, main_data_month_general):
    acd_general_average_data = (round(main_data_month_general['items'][0]['ACD'], 1)
                                if main_data_month_general['items'][0]['ACD'] is not None
                                else 0)
    context.log.info(f"acd_general_average_data {acd_general_average_data}")
    return acd_general_average_data

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def ssr_general_average_data_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    get_main_data_month = NaumenServiceLevelDataGetter(start_date=month_start, end_date=month_end,
                                                     conn=context.resources.source_dwh)
    get_main_data_month.set_months_trunked_data()
    get_main_data_month.filter_projects_by_group_array(general_projects_array)
    get_main_data_month.reload_projects()
    get_main_data_month.filter_projects_by_group_array(general_projects_array, 'IVR')
    main_data_month_general_ivr = get_main_data_month.get_aggregated_data_ivr()
    ssr_general_average_data = (
        round(main_data_month_general_ivr['items'][0]['SSR'], 1)
        if main_data_month_general_ivr['items'][0]['SSR'] is not None
        else 0
    )
    context.log.info(f"ssr_general_average_data {ssr_general_average_data}")
    return ssr_general_average_data

# SL, HCR GENERAL FOR LANGUAGES CHART
@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def main_data_dynamic_general_arpu_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    get_al_main_data_dynamic = NaumenServiceLevelDataGetter(start_date=start_date, end_date=end_date, conn=context.resources.source_dwh)
    get_al_main_data_dynamic.set_days_trunked_data()
    get_al_main_data_dynamic.filter_projects_by_group(group='General')
    main_data_dynamic_general_arpu = get_al_main_data_dynamic.get_arpu_trunked_data()
    context.log.info(f'main_data_dynamic_general_arpu {main_data_dynamic_general_arpu}')
    return main_data_dynamic_general_arpu

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def main_data_dynamic_general_language_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    get_al_main_data_dynamic = NaumenServiceLevelDataGetter(start_date=start_date, end_date=end_date,
                                                          conn=context.resources.source_dwh)
    get_al_main_data_dynamic.set_days_trunked_data()
    get_al_main_data_dynamic.filter_projects_by_group(group='General')
    main_data_dynamic_general_language = get_al_main_data_dynamic.get_language_trunked_data()
    context.log.info(f'main_data_dynamic_general_language {main_data_dynamic_general_language}')
    return main_data_dynamic_general_language

# CEA
@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def cea_data_dynamic_general_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    get_cea_data_dynamic = CriticalErrorAccuracyDataGetter(start_date=start_date, end_date=end_date, bpm_conn=context.resources.source_dwh)
    get_cea_data_dynamic.set_days_trunked_data()
    cea_data_dynamic_general = get_cea_data_dynamic.get_aggregated_data()
    context.log.info(f"cea_data_dynamic_general {cea_data_dynamic_general}")
    return cea_data_dynamic_general

# CEA - month
@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def cea_current_month_general_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    get_cea_data_dynamic = CriticalErrorAccuracyDataGetter(start_date=month_start, end_date=month_end, bpm_conn=context.resources.source_dwh)
    get_cea_data_dynamic.set_months_trunked_data()
    cea_data_month_general = get_cea_data_dynamic.get_aggregated_data()
    cea_current_month_general = round(cea_data_month_general['items'][0]['critical_error_accuracy'], 1)
    context.log.info(f"cea_current_month_general {cea_current_month_general}")
    return cea_current_month_general

# GetTaskSolvingSpeedData
@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def closed_tasks_data_general_dynamic_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    get_task_solving_speed_data = TaskSolvingSpeedDataGetter(start_date=start_date, end_date=end_date, bpm_conn=context.resources.source_dwh)
    get_task_solving_speed_data.set_days_trunked_data()
    closed_tasks_data_general_dynamic = get_task_solving_speed_data.get_closed_tasks_data_general_dynamic()
    context.log.info(f"closed_tasks_data_general_dynamic = {closed_tasks_data_general_dynamic}")
    return closed_tasks_data_general_dynamic

# GetTaskSolvingSpeedData - month
@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def closed_tasks_sla_month_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    get_task_solving_speed_data_month = TaskSolvingSpeedDataGetter(start_date=month_start, end_date=month_end, bpm_conn=context.resources.source_dwh)
    get_task_solving_speed_data_month.set_months_trunked_data()
    closed_tasks_sla_month = round(get_task_solving_speed_data_month.get_closed_tasks_data_general_dynamic()['items'][0]['sla'], 1)
    context.log.info(f"closed_tasks_sla_month {closed_tasks_sla_month}")
    return closed_tasks_sla_month


# Данные за отчетный день
#    IVR Main Data Report Day
@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def ivr_main_data_report_day_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    getter = NaumenServiceLevelDataGetter(
        start_date=report_date,
        end_date=end_date,
        conn=context.resources.source_dwh
    )
    getter.set_hours_trunked_data()
    context.log.info(f"date_format {getter.date_format}, trunc {getter.trunc}")
    getter.filter_projects_by_group_array(general_projects_array, "IVR")

    context.log.info(f"report_date = {getter.start_date}, {getter.end_date}")
    ivr_main_data_report_day_general = getter.get_aggregated_data_ivr()
    context.log.info(f"ivr_main_data_report_day_general = {ivr_main_data_report_day_general}")

    return ivr_main_data_report_day_general

# Main Data Report Day
@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def main_data_report_day_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    getter = NaumenServiceLevelDataGetter(
        start_date=report_date,
        end_date=end_date,
        conn=context.resources.source_dwh
    )
    getter.set_hours_trunked_data()
    context.log.info(f"date_format {getter.date_format}, trunc {getter.trunc}")

    getter.filter_projects_by_group_array(general_projects_array)

    main_data_report_day_general = getter.get_aggregated_data()
    context.log.info(f"main_data_report_day_general = {main_data_report_day_general}")

    return main_data_report_day_general

# HandlingTimes Data Report Day
@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def handling_times_data_report_day_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    getter = AverageHandlingTimeDataGetter(
        start_date=report_date,
        end_date=end_date,
        dwh_conn=context.resources.source_dwh
    )
    getter.set_hours_trunked_data()

    getter.filter_projects_by_group_array(general_projects_array)

    handling_times_data_report_day_general = getter.get_aggregated_data()
    context.log.info(f"handling_times_data_report_day_general = {handling_times_data_report_day_general}")

    return handling_times_data_report_day_general

# Occupancy Data Report Day
@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def occupancy_data_report_day_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    getter = OccupancyDataGetter(
        start_date=report_date,
        end_date=end_date,
        bpm_conn=context.resources.source_dwh,
        conn_naumen_1=context.resources.source_naumen_1,
        conn_naumen_3=context.resources.source_naumen_3,
    )
    getter.set_hours_trunked_data()
    getter.filter_projects_by_group_array(general_projects_array)

    occupancy_data_report_day_general = getter.get_aggregated_data(goo_position_ids)
    context.log.info(f"occupancy_data_report_day_general = {occupancy_data_report_day_general}")

    return occupancy_data_report_day_general

# CSI Data Report Day
# 7.6 CSI
@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def csi_data_report_day_general_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    getter = CustomerSatisfactionIndexAggregationDataGetter(
        start_date=start_date,
        end_date=end_date,
        conn=context.resources.source_dwh
    )

    getter.set_date_format = True
    getter.date_field = "hour"
    getter.set_hours_truncated_data()
    csi_data_report_day_general = getter.get_combined_rating_result(
        ["General", "O!Bank", "Agent", "Terminals", "Saima", "AkchaBulak", "Entrepreneurs"],
        ["naumen", "naumen3"]
    )

    context.log.info(f"csi_data_report_day_general = {csi_data_report_day_general}")
    return csi_data_report_day_general

# График и факт кол-ва операторов ГОО
@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'source_cp', 'report_utils'})
def operators_count_plan_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    operators_count = Operatorscountgetter(report_day=report_date, start_date=start_date, end_date=end_date, naumen_conn_1=context.resources.source_naumen_1,
                                           naumen_conn_3=context.resources.source_naumen_3, bpm_conn=context.resources.source_dwh, conn=context.resources.source_cp)
    operators_count_plan = operators_count.get_labor_data()
    context.log.info(f"operators_count_plan = {operators_count_plan}")
    return operators_count_plan

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'source_cp', 'report_utils'})
def operators_count_factop(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    operators_count = Operatorscountgetter(report_day=report_date, start_date=start_date, end_date=end_date, naumen_conn_1=context.resources.source_naumen_1,
                                           naumen_conn_3=context.resources.source_naumen_3, bpm_conn=context.resources.source_dwh, conn=context.resources.source_cp)
    operators_count_fact = operators_count.get_fact_operatorts()
    context.log.info(f"operators_count_plan = {operators_count_fact}")
    return operators_count_fact

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def fact_and_labor_operators_difference_goo_op(context, operators_count_fact, operators_count_plan):
    fact_and_labor_operators_difference_goo = get_difference(operators_count_fact, operators_count_plan, 'hour', 'fact_operators', 'fact_labor', 'fact_operators_and_fact_labor')
    context.log.info(fact_and_labor_operators_difference_goo)
    return fact_and_labor_operators_difference_goo


@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def detailed_daily_data_goo_op(context, main_data_report_day_general, handling_times_data_report_day_general, occupancy_data_report_day_general, csi_data_report_day_general,
                            ivr_main_data_report_day_general, operators_count_plan, operators_count_fact, fact_and_labor_operators_difference_goo):
    detailed_daily_data_goo = left_join_array(main_data_report_day_general['items'], handling_times_data_report_day_general['items'], 'hour')

    context.log.info(f"occupancy_data_report_day_general: {occupancy_data_report_day_general}")

    detailed_daily_data_goo = left_join_array(detailed_daily_data_goo, occupancy_data_report_day_general['items'], 'hour')

    context.log.info(f"before_detailed_daily_data_goo: {detailed_daily_data_goo}")

    detailed_daily_data_goo = left_join_array(detailed_daily_data_goo, csi_data_report_day_general, 'hour')
    detailed_daily_data_goo = left_join_array(detailed_daily_data_goo, ivr_main_data_report_day_general['items'], 'hour')
    detailed_daily_data_goo = left_join_array(detailed_daily_data_goo, operators_count_plan, 'hour')
    detailed_daily_data_goo = left_join_array(detailed_daily_data_goo, operators_count_fact, 'hour')
    detailed_daily_data_goo = left_join_array(detailed_daily_data_goo, fact_and_labor_operators_difference_goo, 'hour')
    context.log.info(f'detailed_daily_data_goo {detailed_daily_data_goo}')
    return detailed_daily_data_goo

# ** ** ** * < ГПО > ** ** ** **
# За месяц
@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def digital_self_service_rate_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    mp_getter = MessagingPlatformDataGetter(start_date=start_date, end_date=end_date, conn=context.resources.source_dwh)
    mp_getter.set_days_trunked_data()
    mp_getter.project_ids = [1, 4, 6, 8, 9, 10, 12, 15, 20]
    digital_self_service_rate =mp_getter.get_self_service_rate()
    context.log.info(f"digital_self_service_rate {digital_self_service_rate}")
    return digital_self_service_rate

@op(required_resource_keys={"source_dwh", "source_naumen_3", "source_naumen_1", "report_utils"})
def digital_self_service_rate_month_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    project_ids = [1, 4, 6, 8, 9, 10, 12, 15]

    base_getter = MessagingPlatformDataGetter(
        start_date=start_date,
        end_date=end_date,
        conn=context.resources.source_dwh,
    )

    # месячный trunc (на всякий случай поддержим оба нейминга)
    if hasattr(base_getter, "set_months_trunked_data"):
        base_getter.set_months_trunked_data()
    else:
        base_getter.set_months_trunked_data()

    total_bot = 0.0
    total_all = 0.0

    for pid in project_ids:
        getter = copy.copy(base_getter)  # аналог clone в PHP
        getter.project_ids = [pid]

        rows = getter.get_self_service_rate()
        row = rows[0] if rows else None

        # поддержим и dict, и объект (если getter возвращает модели/record)
        if isinstance(row, dict):
            bot = row.get("bot_handled_chats") or 0
            all_ = row.get("all_chats") or 0
        else:
            bot = getattr(row, "bot_handled_chats", 0) or 0
            all_ = getattr(row, "all_chats", 0) or 0

        total_bot += float(bot)
        total_all += float(all_)

    self_service_rate = round(total_bot * 100 / total_all, 1) if total_all > 0 else 0.0

    context.log.info(f"self_service_rate_month total_bot={total_bot} total_all={total_all} rate={self_service_rate}")
    return self_service_rate

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def month_saima_handled_chats_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    mp_getter = MessagingPlatformDataGetter(start_date=start_date, end_date=end_date, conn=context.resources.source_dwh)
    mp_getter.set_days_trunked_data()
    mp_getter.project_ids = [1]
    month_saima_handled_chats =mp_getter.get_self_service_rate()
    context.log.info(f"digital_self_service_rate {month_saima_handled_chats}")
    return month_saima_handled_chats

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def month_bank_handled_chats_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    mp_getter = MessagingPlatformDataGetter(start_date=start_date, end_date=end_date, conn=context.resources.source_dwh)
    mp_getter.set_days_trunked_data()
    mp_getter.project_ids = [8]
    month_bank_handled_chats =mp_getter.get_self_service_rate()
    context.log.info(f"month_bank_handled_chats = {month_bank_handled_chats}")
    return month_bank_handled_chats

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def month_agent_handled_chats_op(context, month_saima_handled_chats, month_bank_handled_chats):
    self_service_rate = 0

    if not month_bank_handled_chats and month_saima_handled_chats:
        self_service_rate = month_saima_handled_chats[0]['self_service_rate']

    elif month_bank_handled_chats and not month_saima_handled_chats:
        self_service_rate = month_bank_handled_chats[0]['self_service_rate']

    elif month_bank_handled_chats and month_saima_handled_chats:
        bot_handled_chats = (
                month_bank_handled_chats[0]['bot_handled_chats']
                + month_saima_handled_chats[0]['bot_handled_chats']
        )
        all_chats = (
                month_bank_handled_chats[0]['all_chats']
                + month_saima_handled_chats[0]['all_chats']
        )
        if all_chats != 0:
            self_service_rate = round(bot_handled_chats * 100 / all_chats, 1)

    else:
        self_service_rate = 0

    context.log.info(f"self_service_rate {self_service_rate}")
    return self_service_rate

# ART И ASoA
@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def mp_data_messengers_aggregated_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    mp_getter = MessagingPlatformDataGetter(start_date=start_date, end_date=end_date, conn=context.resources.source_dwh)
    mp_getter.set_days_trunked_data()
    mp_getter.project_ids = mp_included_projects
    mp_data_messengers_aggregated = mp_getter.get_aggregated()
    context.log.info(f"mp_data_messengers_aggregated {mp_data_messengers_aggregated}")
    return mp_data_messengers_aggregated

# По часам
@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'source_cp', 'report_utils'})
def hour_mp_data_messengers_detailed_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    mp_getter = MessagingPlatformDataGetter(conn=context.resources.source_dwh, start_date=report_date, end_date=end_date,
                                            date_field='hour', project_ids=mp_included_projects, trunc='hour', date_format='')
    hour_mp_data_messengers_detailed = mp_getter.get_full_aggregate(trunc='hour', start_date=report_date, end_date=end_date, project_ids=mp_included_projects, date_field='hour', conn=context.resources.source_dwh)
    get_dagster_logger().info(f"hour_mp_data_messengers_detailed = {hour_mp_data_messengers_detailed}")
    context.log.info(f"start_date {report_date}, end_date {end_date}")
    hour_data_messengers_detailed = mp_getter.get_mp_entered_chats(trunc='hour', start_date=report_date, end_date=end_date, project_ids=mp_included_projects, date_field='hour', conn=context.resources.source_cp)
    get_dagster_logger().info(f"get_mp_entered_chats = {hour_data_messengers_detailed} ")
    hour_mp_data_messengers_detailed = left_join_array(hour_mp_data_messengers_detailed, hour_data_messengers_detailed, left_join_on='hour')
    context.log.info(f"hour_mp_data_messengers_detailed {hour_mp_data_messengers_detailed}")
    return hour_mp_data_messengers_detailed

# По месяцам
@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def mp_month_data_messengers_aggregated_saima_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    mp_getter = MessagingPlatformDataGetter(start_date=month_start, end_date=month_end, conn=context.resources.source_dwh)
    mp_getter.set_days_trunked_data()
    mp_month_data_messengers_aggregated_saima = mp_getter.get_aggregated()
    context.log.info(f"mp_month_data_messengers_aggregated_saima {mp_month_data_messengers_aggregated_saima}")
    return mp_month_data_messengers_aggregated_saima

@op
def month_average_reaction_time_op(context, mp_month_data_messengers_aggregated_saima):
    month_average_reaction_time = round(
        mp_month_data_messengers_aggregated_saima[0]['average_reaction_time']
        if mp_month_data_messengers_aggregated_saima else 0,
        1
    )
    context.log.info(f"month_average_reaction_time_{month_average_reaction_time}")
    return month_average_reaction_time

@op
def month_average_speed_to_answer_op(context, mp_month_data_messengers_aggregated_saima):
    month_average_speed_to_answer = round(
        mp_month_data_messengers_aggregated_saima[0]['average_speed_to_answer']
        if mp_month_data_messengers_aggregated_saima else 0,
        1
    )
    context.log.info(f"month_average_speed_to_answer_{month_average_speed_to_answer}")
    return month_average_speed_to_answer

# CEA ГПО
@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def cea_data_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    cea_data_getter = CriticalErrorAccuracyDataGetter(start_date=start_date, end_date=end_date, bpm_conn=context.resources.source_dwh)
    cea_data_getter.set_days_trunked_data()
    cea_data = cea_data_getter.get_aggregated_data_by_position(gpo_position_ids)
    context.log.info(f"cea_data {cea_data}")
    return cea_data

# Среднее за месяц
@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def month_average_cea_data_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates
    start_date_year = dates.StartDateYear

    cea_data_getter = CriticalErrorAccuracyDataGetter(start_date=start_date_year, end_date=month_end, bpm_conn=context.resources.source_dwh)
    cea_data_getter.set_months_trunked_data()
    month_cea_data_aggregated = cea_data_getter.get_aggregated_data_by_position(gpo_position_ids)
    month_average_cea_data = round(month_cea_data_aggregated['items'][0]['critical_error_accuracy']
                                   if month_cea_data_aggregated['items'] else 0, 1)
    context.log.info(f"month_average_cea_data {month_average_cea_data}")
    return month_average_cea_data

# CSI
@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def csi_data_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    csi_data = get_jivo_csi_data(start_date=start_date, end_date=end_date, statistics_period='day', channel_ids=[5051, 5001, 5151, 5251, 5201], dwh_conn=context.resources.source_dwh)
    context.log.info(f"csi_data {csi_data}")
    return csi_data

# CSI Data Report Date
@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def csi_report_day_data_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    csi_report_day_data = get_jivo_csi_data(start_date=report_date, end_date=end_date, statistics_period='hour', channel_ids=[5051, 5001, 5151, 5251, 5201], dwh_conn=context.resources.source_dwh)
    context.log.info(f"csi_report_day_data {csi_report_day_data}")
    return csi_report_day_data


# Среднее за месяц ГПО
@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def month_csi_data_aggregated_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    month_csi_data_aggregated = get_jivo_csi_data(start_date=month_start, end_date=month_end, statistics_period='month', channel_ids=[5051, 5001, 5151, 5251, 5201], dwh_conn=context.resources.source_dwh)
    context.log.info(f"month_csi_data_aggregated {month_csi_data_aggregated}")
    return month_csi_data_aggregated

@op
def month_average_csi_data_op(context, month_csi_data_aggregated):
    month_average_csi_data = round(month_csi_data_aggregated[0]['customer_satisfaction_index']
                                   if month_csi_data_aggregated else 0, 1)
    context.log.info(f"month_average_csi_data {month_average_csi_data}")
    return month_average_csi_data

@op
def month_average_conversion_csi_data_op(context, month_csi_data_aggregated):
    month_average_conversion_csi_data = round(month_csi_data_aggregated[0]['conversion']
                                   if month_csi_data_aggregated else 0, 1)
    context.log.info(f"month_average_conversion_csi_data {month_average_conversion_csi_data}")
    return month_average_conversion_csi_data

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'source_cp', 'report_utils'})
def detailed_daily_data_gpo_op(context, hour_mp_data_messengers_detailed, csi_report_day_data):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    operators_count = Operatorscountgetter(report_day=report_date, start_date=start_date, end_date=end_date, naumen_conn_1=context.resources.source_naumen_1,
                                           naumen_conn_3=context.resources.source_naumen_3, bpm_conn=context.resources.source_dwh, conn=context.resources.source_cp)
    operators_count.view_ids = [17, 11]
    operators_count.position_id = gpo_position_ids
    get_dagster_logger().info(f'attributes {operators_count.__dict__}')
    operators_count_plan = operators_count.get_labor_data()
    get_dagster_logger().info(f"operators_count_plan {operators_count_plan}")


    operators_count_fact = operators_count.get_gpo_fact_operators()
    get_dagster_logger().info(f"operators_count_fact {operators_count_fact}")


    fact_and_labor_operators_difference_gpo = get_difference(operators_count_fact, operators_count_plan, 'hour', 'fact_operators', 'fact_labor', 'fact_operators_and_fact_labor')

    # jivo_data_getter = JivoHandlingTimeDataGetter(start_date=report_date, end_date=end_date, conn=context.resources.source_dwh)
    # jivo_data_getter.add_period()

    detailed_daily_data_gpo = left_join_array(operators_count_plan, operators_count_fact, 'hour')
    get_dagster_logger().info(f"detailed_daily_data_gpo {detailed_daily_data_gpo}")

    detailed_daily_data_gpo = left_join_array(detailed_daily_data_gpo, fact_and_labor_operators_difference_gpo, 'hour')
    get_dagster_logger().info(f"detailed_daily_data_gpo {detailed_daily_data_gpo}")

    detailed_daily_data_gpo = left_join_array(detailed_daily_data_gpo, hour_mp_data_messengers_detailed, 'hour')
    get_dagster_logger().info(f"detailed_daily_data_gpo {detailed_daily_data_gpo}")

    detailed_daily_data_gpo = left_join_array(detailed_daily_data_gpo, csi_report_day_data, 'hour')

    context.log.info(f'detailed_daily_data_gpo {detailed_daily_data_gpo}')

    return detailed_daily_data_gpo


@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def chat_data_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    mp_data_getter = MessagingPlatformDataGetter(start_date=start_date, end_date=end_date, conn=context.resources.source_dwh)
    mp_data_getter.project_ids = mp_included_projects
    mp_data_getter.pst_positions = [10, 34, 41, 46]
    mp_data_getter.set_days_trunked_data()
    chat_data = mp_data_getter.get_detailed_with_bot()
    project_titles = {
        "O! Money - WhatsApp": "O!Деньги - WhatsApp",
        "O!Money - WhatsApp": "O!Деньги - WhatsApp",
        "O! Money - Telegram": "O!Деньги - Telegram",
        "O!Money - Telegram": "O!Деньги - Telegram",
        "O! Store - ostore.kg": "O!Store - ostore.kg",
        "O! Store - Telegram": "O!Store - Telegram",
        "O! Money - wiki.dengi.kg": "O!Деньги - wiki.dengi.kg",
        "O!Money - wiki.dengi.kg": "O!Деньги - wiki.dengi.kg",
    }

    # Преобразование chatData
    chat_data = [
        {**item, "project": project_titles.get(item["project"], item["project"])}
        for item in chat_data
    ]
    context.log.info(f"chat_data {chat_data}")
    return chat_data

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'source_cp', 'report_utils'})
def technical_works_data_report_day_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    getter = OmniDataGetter(start_date=report_date, end_date=end_date, conn=context.resources.source_cp)
    getter.project_ids = [1,4,5,6,8,9,10,11,12,13,14,15]
    technical_works_data_report_day = getter.get_omni_actual_technical_works_data()
    technical_works_data_report_day.sort(key=lambda x: x.get("date", ""))
    context.log.info(f"technical_works_data_report_day {technical_works_data_report_day}")
    return technical_works_data_report_day

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'source_cp', 'report_utils'})
def request_data_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    getter = OmniDataGetter(start_date=date_list[0], end_date=end_date, conn=context.resources.source_cp)
    getter.splits = []
    getter.project_ids = [1,4,6,8,9,10,11,12,14,15]
    getter.set_days_trunked_data()
    get_dagster_logger().info(f"getter {getter.__dict__}")
    items = getter.get_requests_data()
    result = []
    unique_ids = {}

    test = []
    for item in items:
        key = (
            f"{item['request_title']}-"
            f"{item['request_label_title']}-"
            f"{item['reason_title']}-"
            f"{item['project_title']}-"
            f"{item['request_status_title']}"
        )

        if key not in unique_ids:
            unique_ids[key] = len(unique_ids) + 1

        result.append({
            "id": unique_ids[key],
            "date": item["date"],
            "project_title": "Линия 707" if item["project_title"] == "General" else item["project_title"],
            "request_title": item["request_title"],
            "reason_title": item["reason_title"],
            "request_label_title": item["request_label_title"],
            "count": item["count"],
            "status": item["request_status_title"],
        })
        test.append(item["date"])
    test = set(test)
    context.log.info(f"request_data = {result}")
    context.log.info(f"test = {test}")
    return result



@op(required_resource_keys={'source_nod_dinosaur', 'report_utils'})
def ivr_transitions_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    getter = IVRTransactionDataGetter(start_date=date_list[0], end_date=end_date, nod_dinosaur_conn=context.resources.source_nod_dinosaur)
    getter.set_days_trunked_data()
    items = getter.get_level_data(callers=callers, parents=[3,4,5,6,138])
    context.log.info(f"ivr_transitions_op {items}")
    return items

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def day_bot_handled_chats_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    mp_data_getter = MessagingPlatformDataGetter(start_date=start_date, end_date=end_date, conn=context.resources.source_dwh)
    day_bot_handled_chats = mp_data_getter.get_bot_handled_chats(trunc='day', start_date=start_date, end_date=end_date, projects=[1, 4, 6, 8, 9, 10, 12, 15, 20])

    day_bot_handled_chats = filter_data(day_bot_handled_chats)
    context.log.info(f"day_bot_handled_chats = {day_bot_handled_chats}")
    return day_bot_handled_chats

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def month_bot_handled_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    mp_data_getter = MessagingPlatformDataGetter(conn=context.resources.source_dwh)
    rows = mp_data_getter.get_bot_handled_chats_percentage(trunc='month', start_date=start_date, end_date=end_date, projects=[1, 4, 6, 8, 9, 10, 12, 15, 20])
    if rows:
        last = rows[-1]  # ORDER BY 1 в SQL — берём последний месяц
        total = float(last.get("total_handled_chats") or 0)
        bots = float(last.get("bot_handled_chats") or 0)
        month_handled_handled_chats = round((bots / total * 100) if total else 0.0, 1)
    else:
        month_handled_handled_chats = 0.0

    context.log.info(f"month_handled_handled_chats = {month_handled_handled_chats}")
    return month_handled_handled_chats

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'source_cp', 'report_utils'})
def day_bot_projects_handled_chats_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    mp_data_getter = MessagingPlatformDataGetter(conn=context.resources.source_dwh)
    day_bot_projects_handled_chats = mp_data_getter.get_bot_handled_chats_percentage_with_project(trunc='day', start_date=start_date, end_date=end_date, projects=[1, 4, 6, 8, 9, 10, 12, 15, 20])
    day_bot_projects_handled_chats = filter_data(day_bot_projects_handled_chats)

    mp_data_getter = MessagingPlatformDataGetter(conn=context.resources.source_cp)

    projects_map = mp_data_getter.projects_map()

    enriched: list[dict] = []
    for row in day_bot_projects_handled_chats:
        r = dict(row)

        if not r.get("project"):
            pid = r.get("project_id")
            try:
                pid_int = int(pid) if pid is not None else None
            except (TypeError, ValueError):
                pid_int = None
            r["project"] = projects_map.get(pid_int, f"Project #{pid}")

        if "bot_handled_chats_percentage" in r and r["bot_handled_chats_percentage"] is not None:
            try:
                r["bot_handled_chats_percentage"] = float(r["bot_handled_chats_percentage"])
            except (TypeError, ValueError):
                pass

        enriched.append(r)

    day_bot_projects_handled_chats = enriched

    context.log.info(f"day_bot_projects_handled_chats = {day_bot_projects_handled_chats}")
    return day_bot_projects_handled_chats

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'source_dwh_prod', 'report_utils'})
def sl_jira_data_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    jira_getter = JiraDataGetterNext(start_date=start_date, end_date=end_date, dwh_conn=context.resources.source_dwh_prod, jira_nur = context.resources.source_dwh_prod, jira_saima=None)
    projects = {
        "General":   {"title": "Линия - 707/0705 700 700", "id": 6,  "topic_view_id": 8,  "jira_project_id": "10301", "jira_project_key": ["MS","CSA","MA"]},
        "Saima":     {"title": "Saima - 0706 909 000",     "id": 12, "topic_view_id": 1,  "jira_project_id": "11005", "jira_project_key": ["MS","CSA","MA"]},
        "Money":     {"title": "O!Bank + О!Деньги (Jira NUR) - 999/0700 000 999", "id": 8, "topic_view_id": 7, "jira_project_id": "10304", "jira_project_key": ["PAYM","APP"]},
        "Terminals": {"title": "О! Терминал - 799/0702 000 799", "id": 17, "topic_view_id": 9, "jira_project_id": "11309", "jira_project_key": ["TERM"]},
        "Agent":     {"title": "О! Агент - 5858/0505 58 55 58", "id": 1,  "topic_view_id": 6, "jira_project_id": "11503", "jira_project_key": ["AG"]},
        "AkchaBulak":{"title": "Акча Булак - 405/0702 000 405", "id": 21, "topic_view_id": 14, "jira_project_id": "12700", "jira_project_key": "AB"},
        "OFD":       {"title": "О!Касса - 7878/0501 78 00 78", "id": 5,  "topic_view_id": 10, "jira_project_id": "11603", "jira_project_key": "OFD"},
    }

    def projkey_filter(keys):
        if keys is None:
            return ("TRUE", {})
        if isinstance(keys, (str, bytes)):
            keys = [keys]
        keys = list(keys)
        if not keys:
            return ("FALSE", {})
        return ("sjisd.project_key = ANY(%(proj_keys)s::text[])", {"proj_keys": keys})
    tables = {}
    for proj in projects.values():
        sla_month = jira_getter.get_sla_by_groups(
            conn=context.resources.source_dwh_prod,
            groups=[proj["id"]],
            group=False,
            callback=lambda p=proj: projkey_filter(p["jira_project_key"]),
        )

        max_val = None
        for item in sla_month:
            cur = item.get("all_resolved_issues") or 0
            if max_val is None or cur > max_val:
                max_val = cur

        if max_val is None:
            max_val = 0

        if max_val >= 100:
            max_val = round(max_val / 90) * 100

        max_val *= 2

        get_dagster_logger().info(f"items={sla_month}")
        get_dagster_logger().info(f"max_val={max_val}")

        graphic_sl_dynamic_general = MultiAxisChart(
        graph_title="",
        y_axes_name="Количество",
        y_axes_min=0,
        y_axes_max=max_val,
        items=sla_month,
        fields=[
            {"field": "date", "title": "", "type": "label"},
            {"field": "sla_reached", "title": "Кол-во закрытых задач вовремя", "chartType": "grouped_bars", "round": 0},
            {"field": "all_resolved_issues", "title": "Кол-во закрытых задач", "chartType": "grouped_bars", "round": 0},
            {"field": "sla", "title": "SLA", "chartType": "line", "round": 0},
                ],
            )
        graphic_sl_dynamic_general.combine(3, max_percent=max_val)

        fig, ax = plt.subplots(
            figsize=(graphic_sl_dynamic_general.width / graphic_sl_dynamic_general.dpi, graphic_sl_dynamic_general.height / graphic_sl_dynamic_general.dpi),
            dpi=graphic_sl_dynamic_general.dpi,
        )
        graphic_sl_dynamic_general.render_graphics(ax)

        buf = io.BytesIO()
        fig.savefig(buf, format="svg", bbox_inches="tight")
        plt.close(fig)
        buf.seek(0)
        svg_64 = base64.b64encode(buf.read()).decode("utf-8")

        tables[proj['title']] = svg_64
        context.log.info(f"title = {proj['title']}")
    return tables

@op(required_resource_keys={'source_dwh_prod', 'report_utils'})
def jira_data_saima_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    jira_data_saima = JiraDataGetterNext(start_date=start_date, end_date=end_date, dwh_conn=context.resources.source_dwh_prod, jira_nur=context.resources.source_dwh_prod,
                                       jira_saima=context.resources.source_dwh_prod, jira='jirasd_saima')
    jira_data_saima.ProjectKeys = ['MA','MS']

    saima = jira_data_saima.get_created_issues_by_responsible_groups_with_id()
    context.log.info(f"saima_jira = {saima}")
    return saima

@op(required_resource_keys={'source_dwh_prod', 'report_utils'})
def jira_data_nur_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    jira_data_nur = JiraDataGetterNext(start_date=start_date, end_date=end_date, dwh_conn=context.resources.source_dwh_prod, jira_nur=context.resources.source_dwh_prod,
                                       jira_saima=context.resources.source_dwh_prod, jira='jirasd_nur')
    context.log.info(f"responsible_groups = {jira_data_nur.responsible_groups}")
    nur = jira_data_nur.get_created_issues_by_responsible_groups_with_id()
    context.log.info(f"nur_jira = {nur}")
    return nur

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def nur_saima_aggregated_data_op(context, saima_data, nur_data):
    saima_aggregated_data = set_jira_issues_data(saima_data, projects=jira_projects)
    nur_aggregated_data = set_jira_issues_data(nur_data, projects=project_titles)
    results = saima_aggregated_data + nur_aggregated_data
    context.log.info(f"results = {results}")
    return results


def get_jivo_csi_data(start_date, end_date, statistics_period, channel_ids, dwh_conn):
    data_csi_lr = CustomerSatisfactionIndexDataGetter(start_date=start_date, end_date=end_date, bpm_conn=dwh_conn, trunc='day')
    if statistics_period == 'day':
        data_csi_lr.set_days_trunked_data()
    elif statistics_period == 'month':
        data_csi_lr.set_months_trunked_data()
    elif statistics_period == 'hour':
        data_csi_lr.set_hours_trunked_data()

    kpi_data_csi_slr = data_csi_lr.get_aggregated_csi_slr_data(channel_ids)

    data_csi_nod = CustomerSatisfactionIndexAggregationDataGetter(start_date=start_date, end_date=end_date, conn=dwh_conn)
    if statistics_period == 'day':
        data_csi_nod.set_days_truncated_data()
    elif statistics_period == 'month':
        data_csi_nod.set_months_truncated_data()
    elif statistics_period == 'hour':
        data_csi_nod.set_hours_truncated_data()

    def _h(d: str) -> str:
        """
        Универсально приводит строку даты/времени к формату 'HH:MM:SS'.
        Поддерживает:
          - 'YYYY-MM-DD HH:MM:SS'
          - 'YYYY-MM-DD'  (возвращает '00:00:00')
          - 'HH:MM:SS'
        """
        if not d:
            return "00:00:00"
        d = d.strip()
        try:
            return datetime.strptime(d, "%Y-%m-%d %H:%M:%S").strftime("%H:%M:%S")
        except ValueError:
            pass
        try:
            return datetime.strptime(d, "%H:%M:%S").strftime("%H:%M:%S")
        except ValueError:
            pass
        try:
            datetime.strptime(d, "%Y-%m-%d")
            return "00:00:00"
        except ValueError:
            pass
        return "00:00:00"

    data_csi_nod.channels = ['jivo', 'messaging-platform', 'cp']
    kpi_data_csi_nod = data_csi_nod.get_aggregated_data()

    if not kpi_data_csi_nod.get("items") and kpi_data_csi_slr:
        return [
            {
                "date": item["date"],
                "hour": _h(item["date"]),
                "good_marks": item["good_marks"],
                "all_marks": item["all_marks"],
                "bad_marks": item["bad_marks"],
                "neutral_marks": item["neutral_marks"],
                "conversion": item["conversion"],
                "customer_satisfaction_index": (
                    item["good_marks"] / (item["good_marks"] + item["bad_marks"]) * 100
                    if (item["good_marks"] + item["bad_marks"]) else None
                ),
            }
            for item in kpi_data_csi_slr
        ]

    out = []
    items = kpi_data_csi_nod.get("items", [])

    for item in items:
        slr_match = next((i for i in kpi_data_csi_slr if i.get("date") == item.get("date")), None)
        if not slr_match:
            out.append({
                "date": item["date"],
                "hour": _h(item["date"]),
                "good_marks": item["good_marks"],
                "all_marks": item["all_marks"],
                "bad_marks": item["bad_marks"],
                "neutral_marks": item["neutral_marks"],
                "conversion": item["conversion"],
                "customer_satisfaction_index": (
                    item["good_marks"] / (item["good_marks"] + item["bad_marks"]) * 100
                    if (item["good_marks"] + item["bad_marks"]) else None
                ),
            })
            continue
        total_good = item["good_marks"] + slr_match["good_marks"]
        total_bad = item["bad_marks"] + slr_match["bad_marks"]
        total_neutral = item["neutral_marks"] + slr_match["neutral_marks"]
        total_marks = total_good + total_bad + total_neutral
        conversion = 0
        if item.get("all_contacts"):
            conversion = total_marks / item["all_contacts"] * 100
        out.append({
            "date": item["date"],
            "hour": _h(item["date"]),
            "good_marks": total_good,
            "bad_marks": total_bad,
            "neutral_marks": total_neutral,
            "all_marks": total_marks,
            "customer_satisfaction_index": (
                total_good / (total_good + total_bad) * 100
                if (total_good + total_bad) else None
            ),
            "conversion": conversion,
        })
    return out

def filter_data(items):
    project_titles = {
        1: "Saima",
        8: "Bank",
        9: "O!Bank",
    }

    result = []
    for item in items:
        item_copy = item.copy()

        project_id = item_copy.get("project_id")
        date_str = item_copy.get("date")

        if date_str is not None:
            date = _parse_dt_any(date_str)
        else:
            date = None

        if project_id in project_titles:
            item_copy["project"] = project_titles[project_id]

        if project_id is not None:
            if (project_id == 1 and date < datetime(2024, 10, 16)) or \
               (project_id == 8 and date < datetime(2024, 10, 25)):
                continue
            result.append(item_copy)
        else:
            if date is not None and date < datetime(2024, 10, 16):
                continue
            result.append(item_copy)

    return result

@op(
    required_resource_keys={"report_utils"},
    ins={"file_path": In(str)},
    out=Out(dict),
)
def upload_report_instance_general_op(context, file_path: str) -> Dict[str, Any]:
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
    report_id = int(cfg.get("report_id", 4))
    api_key = cfg.get("api_key") or os.getenv("REPORTS_API_KEY") or os.getenv("RV_REPORTS_API_KEY")
    if not api_key:
        raise Failure("Не задан API ключ (REPORTS_API_KEY / RV_REPORTS_API_KEY или op_config.api_key).")

    timeout_connect = int(cfg.get("timeout_connect", 10))
    timeout_read = int(cfg.get("timeout_read", 120))
    verify_ssl = bool(cfg.get("verify_ssl", True))

    title = cfg.get("title") or "ИСС | Общая ежедневная статистика"

    url = f"{base_url.rstrip('/')}/api/report-instances/upload/{report_id}"
    headers = {"x-api-key": api_key}
    data = {"title": title, "day": day_str}

    try:
        with open(file_path, "rb") as f:
            files = {"file": (os.path.basename(file_path), f, "application/pdf")}
            resp = requests.post(url, headers=headers, data=data, files=files,
                                 timeout=(timeout_connect, timeout_read), verify=verify_ssl)
    except Exception as e:
        return {"status_code": -1, "text": str(e)}

    try:
        return resp.json()
    except Exception:
        return {"status_code": resp.status_code, "text": resp.text}