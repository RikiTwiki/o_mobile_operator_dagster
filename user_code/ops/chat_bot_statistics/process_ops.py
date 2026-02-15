from dagster import get_dagster_logger, op, In, Out, Failure
import requests
from zoneinfo import ZoneInfo

from utils.mail.mail_sender import ReportsApiConfig, ReportInstanceUploader

from ops.chat_bot_statistics.visualization_ops import render_cost_line_graph
from resources import ReportUtils, source_dwh_resource
from utils.array_operations import left_join_array
from utils.getters.critical_error_accuracy_data_getter import CriticalErrorAccuracyDataGetter
from utils.getters.messaging_platform_data_getter import MessagingPlatformDataGetter
from utils.getters.omni_data_getter import OmniDataGetter
from utils.report_data_collector import get_cea_data, get_cea_data_by_project

from zoneinfo import ZoneInfo

from utils.mail.mail_sender import ReportsApiConfig, ReportInstanceUploader

import os

from datetime import date, timedelta, datetime, timezone

from typing import Dict, Any

bot_staff_unit_id = 189
date_field = 'date'
comment = None


all_data = {
    "titles": ["Saima", "O!Bank", "О!"],
    "ids": [1,4,5,10,11,12,15,19,22]
}

saima = {
    "titles": ["Saima"],
    "ids": [1]
}

bank = {
    "titles": ["O!Bank"],
    "ids": [15]
}

nur = {
    "titles": ["О!"],
    "ids": [4]
}

project_map = {
    1: "saima",
    4: "nur",
    15: "bank",
}

ob = {
    "ids": [19]
}

ab = {
    "titles": ["AkchaBulak"],
    "ids": [10]
}

dwh_conn = source_dwh_resource

@op(required_resource_keys={'source_dwh', 'report_utils'})
def has_note_daily_control_data_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    return get_has_note_daily_control_data(all_data['titles'], conn=context.resources.source_dwh, start_date=start_date, end_date=end_date, date_list=date_list)

@op(required_resource_keys={'source_dwh', 'report_utils'})
def has_error_nur_daily_control_data_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    return get_has_error_daily_control_data(nur['titles'], conn=context.resources.source_dwh, start_date=start_date, end_date=end_date, date_list=date_list)

@op(required_resource_keys={'source_dwh', 'report_utils'})
def has_error_saima_daily_control_data_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    return get_has_error_daily_control_data(saima['titles'], conn=context.resources.source_dwh, start_date=start_date, end_date=end_date, date_list=date_list)

@op(required_resource_keys={'source_dwh', 'report_utils'})
def has_error_bank_daily_control_data_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    return get_has_error_daily_control_data(bank['titles'], conn=context.resources.source_dwh, start_date=start_date, end_date=end_date, date_list=date_list)

@op(required_resource_keys={'source_cp', 'source_dwh', 'report_utils'})
def daily_detailed_data_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    return get_daily_detailed_data(period='day', projects=all_data, conn=context.resources.source_cp, dwh_conn=context.resources.source_dwh, start_date=start_date, end_date=end_date)

@op(required_resource_keys={'source_cp', 'source_dwh', 'report_utils'})
def saima_daily_detailed_data_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    return get_daily_detailed_data(period='day', projects=saima, conn=context.resources.source_cp, dwh_conn=context.resources.source_dwh, start_date=start_date, end_date=end_date)

@op(required_resource_keys={'source_cp', 'source_dwh', 'report_utils'})
def bank_daily_detailed_data_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    return get_daily_detailed_data(period='day', projects=bank, conn=context.resources.source_cp, dwh_conn=context.resources.source_dwh, start_date=start_date, end_date=end_date)

@op(required_resource_keys={'source_cp', 'source_dwh', 'report_utils'})
def nur_daily_detailed_data_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    return get_daily_detailed_data(period='day', projects=nur, conn=context.resources.source_cp, dwh_conn=context.resources.source_dwh, start_date=start_date, end_date=end_date)

@op(required_resource_keys={'source_cp', 'source_dwh', 'report_utils'})
def akchabulak_daily_detailed_data_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    return get_daily_detailed_data(period='day', projects=ab, conn=context.resources.source_cp, dwh_conn=context.resources.source_dwh, start_date=start_date, end_date=end_date)

@op(required_resource_keys={'source_cp', 'source_dwh', 'report_utils'})
def obrand_daily_detailed_data_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    return get_daily_detailed_data(period='day', projects=ob, conn=context.resources.source_cp, dwh_conn=context.resources.source_dwh, start_date=start_date, end_date=end_date)

@op(required_resource_keys={'source_dwh', 'report_utils'})
def daily_data_bot_percentage_by_project_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    getter = MessagingPlatformDataGetter(start_date=start_date, end_date=end_date, conn=context.resources.source_dwh)
    getter.project_ids = all_data['ids']
    getter.set_days_trunked_data()
    data = getter.get_bot_aggregated_percentage_with_project(exclude_splits=True)

    daily_bot_percentage_by_project = set_id_to_title(data)
    get_dagster_logger().info(f'Daily Bot Percentage by Project: {daily_bot_percentage_by_project}')
    return daily_bot_percentage_by_project

@op(required_resource_keys={'source_dwh', 'report_utils'})
def daily_bot_percentage_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    getter = MessagingPlatformDataGetter(start_date=start_date, end_date=end_date, conn=context.resources.source_dwh)
    getter.project_ids = all_data['ids']
    getter.set_days_trunked_data()
    daily_bot_percentage = getter.get_bot_aggregated_percentage(exclude_splits=True)

    get_dagster_logger().info(f" daily_bot_percentage = {daily_bot_percentage}")
    return daily_bot_percentage

@op(required_resource_keys={'source_dwh', 'report_utils'})
def bea_daily_detailed_data_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    return get_cea_data(conn= context.resources.source_dwh, period='day', title=['mp'], start_date=start_date, end_date=end_date)

@op(required_resource_keys={'source_dwh', 'report_utils'})
def nur_daily_detailed_data_cea_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    return get_cea_data_by_project(conn= context.resources.source_dwh, period='day', project_titles=['О!'], start_date=start_date, end_date=end_date)

@op(required_resource_keys={'source_dwh', 'report_utils'})
def bank_daily_detailed_data_cea_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    return get_cea_data_by_project(conn= context.resources.source_dwh, period='day', project_titles=['O!Bank'], start_date=start_date, end_date=end_date)

@op(required_resource_keys={'source_dwh', 'report_utils'})
def saima_daily_detailed_data_cea_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    return get_cea_data_by_project(conn= context.resources.source_dwh, period='day', project_titles=['Saima'], start_date=start_date, end_date=end_date)

sk = os.getenv("OPENAI_API_KEY")
projects = {
    "proj_Hc2CnoWqAvC3oQShdi4sycBt": "saima",
    "proj_wJmCifljsDnFikngWwPl1Z1v": "nur",
    "proj_dZvsA7tV6zyal39uD37Ag5dN": "bank"
}
@op(required_resource_keys={'report_utils'})
def cost_tokens_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    get_dagster_logger().info(f" sk = {sk}, projects = {projects} result = {get_openai_usage_tokens(sk, projects, end_date)}")
    return get_openai_usage_tokens(sk, projects, end_date)

@op(required_resource_keys={'report_utils'})
def cost_usd_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    get_dagster_logger().info(f"get_openai_cost = {get_openai_cost(sk, projects, end_date)}")

    return get_openai_cost(sk, projects, end_date)

@op(required_resource_keys={'report_utils'})
def cost_usd_by_token_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    get_dagster_logger().info(f"cost_usd_by_token_op = {get_openai_cost_line_item(sk, projects, end_date)}")
    return get_openai_cost_line_item(sk, projects, end_date)

@op(required_resource_keys={'source_dwh', 'report_utils'})
def dialogs_count_by_projects_op(context, cost_usd):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    mp_data_getter = MessagingPlatformDataGetter(start_date=start_date, end_date=end_date, conn=context.resources.source_dwh)
    mp_data_getter.set_days_trunked_data()
    mp_data_getter.date_format = 'YYYY-MM-DD'
    mp_data_getter.date_field = "date"
    mp_data_getter.project_ids = [1, 4, 15]
    dialogs_count_by_projects = mp_data_getter.get_bot_aggregated_by_project_ids(True)
    get_dagster_logger().info(f" dialogs_count_by_projects = {dialogs_count_by_projects}")
    dialogs_count = {}

    if not cost_usd:
        get_dagster_logger().info("dialogs_count_by_projects_op: cost_usd is empty/None → []")
        return []

    for cost in cost_usd:
        date = cost["date"]

        for item in dialogs_count_by_projects:
            if item["date"] == date and item["project_id"] in project_map:
                project_key = project_map[item["project_id"]]
                cost_key = project_key.split("_")[0]
                cost_value = cost.get(cost_key, 0)

                if date not in dialogs_count:
                    dialogs_count[date] = {"date": date}

                dialogs_count[date][project_key] = round(cost_value / item["total"], 3) * 100

    items = list(dialogs_count.values())
    get_dagster_logger().info(f" items = {items}")

    return items

@op(required_resource_keys={'source_dwh', 'report_utils'})
def replies_count_by_projects_op(context, cost_usd):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    mp_data_getter = MessagingPlatformDataGetter(start_date=start_date, end_date=end_date, conn=context.resources.source_dwh)
    mp_data_getter.set_days_trunked_data()
    mp_data_getter.date_format = 'YYYY-MM-DD'
    mp_data_getter.date_field = "date"
    mp_data_getter.project_ids = [1, 4, 15]
    replies_count_by_projects = mp_data_getter.get_bot_aggregated_messages_by_project_ids(True)

    replies_count = {}

    if not cost_usd:
        get_dagster_logger().info("dialogs_count_by_projects_op: cost_usd is empty/None → []")
        return []

    for cost in cost_usd:
        date = cost["date"]

        for item in replies_count_by_projects:
            if item["date"] == date and item["project_id"] in project_map:
                project_key = project_map[item["project_id"]]
                cost_key = project_key.split("_")[0]
                cost_value = cost.get(cost_key, 0)

                if date not in replies_count:
                    replies_count[date] = {"date": date}

                replies_count[date][project_key] = round(cost_value / item["total"], 3) * 100

    items = list(replies_count.values())
    get_dagster_logger().info(f"items = {items}")
    return items

@op(required_resource_keys={'source_cp', 'report_utils'})
def request_data_saima_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    return request_data(projects=saima['ids'], conn=context.resources.source_cp, splits=[2,4,5,3,6], date_list=date_list, end_date=end_date)

@op(required_resource_keys={'source_cp', 'report_utils'})
def request_data_obank_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    return request_data([11,15], conn=context.resources.source_cp, splits=[150,151,152,153,154,155], date_list=date_list, end_date=end_date)

@op(required_resource_keys={'source_cp', 'report_utils'})
def request_data_nur_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    return request_data([4], conn=context.resources.source_cp, splits=[108,109,110,111,112,113,106,158,107], date_list=date_list, end_date=end_date)


def get_openai_usage_tokens(sk, projects, end_date):
    end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
    start_date_obj = end_date_obj - timedelta(days=30)

    start_date_int = int(start_date_obj.timestamp())
    end_date_int = int(end_date_obj.timestamp())

    # 2. Параметры запроса
    limit = 31
    url = "https://ccp.o.kg/g/openai/v1/organization/usage/completions"
    headers = {
        "x-api-key": "da766b87-3934-4368-b66b-8739349368f9",
        "Authorization": f"Bearer {sk}",
    }
    params = {
        "start_time": start_date_int,
        "end_time": end_date_int,
        "bucket_width": "1d",
        "group_by": "project_id",
        "limit": limit,
    }

    # 3. Запрос
    response = requests.get(url, headers=headers, params=params, verify=False)

    if response.status_code != 200:
        return None

    data = response.json()
    arr: dict = {}

    # 4. Обработка
    if data.get("data") and isinstance(data["data"], list):
        for items in data["data"]:
            if items.get("results") and "start_time" in items:
                # дата с +6 часов
                date = datetime.fromtimestamp(items["start_time"] + 6 * 3600, tz=timezone.utc).strftime("%Y-%m-%d")

                for item in items["results"]:
                    project_id = item.get("project_id")
                    if not project_id or project_id not in projects:
                        continue

                    prefix = projects[project_id]

                    if prefix not in arr:
                        arr[prefix] = []

                    arr[prefix].append(
                        {
                            "date": date,
                            "input_tokens": round(item.get("input_tokens", 0) / 1_000_000, 3),
                            "output_tokens": round(item.get("output_tokens", 0) / 1_000_000, 3),
                            "input_cached_tokens": round(item.get("input_cached_tokens", 0) / 1_000_000, 3),
                        }
                    )

    return arr

def get_openai_cost(sk, projects, end_date):
    # 1. Даты (с -29 дней до end_date + 1 день)
    end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
    start_date_obj = end_date_obj - timedelta(days=31)
    end_date_plus1 = end_date_obj + timedelta(days=0)

    start_date_int = int(start_date_obj.timestamp())
    end_date_int = int(end_date_plus1.timestamp())

    get_dagster_logger().info(f"start_date = {start_date_int}, {end_date_int}")

    # 2. Параметры запроса
    limit = 31
    url = "https://ccp.o.kg/g/openai/v1/organization/costs"
    headers = {
        "x-api-key": "da766b87-3934-4368-b66b-8739349368f9",
        "Authorization": f"Bearer {sk}",
    }
    params = {
        "start_time": start_date_int,
        "end_time": end_date_int,
        "bucket_width": "1d",
        "group_by": "project_id",
        "limit": limit,
    }

    # 3. Запрос
    response = requests.get(url, headers=headers, params=params, verify=False)
    if response.status_code != 200:
        return None

    data = response.json()
    arr: dict[str, dict] = {}

    # 4. Обработка JSON
    if data.get("data") and isinstance(data["data"], list):
        for items in data["data"]:
            if items.get("results") and "start_time" in items:
                # дата с +6 часов (как в PHP)
                date = datetime.fromtimestamp(items["start_time"] + 6 * 3600, tz=timezone.utc).strftime("%Y-%m-%d")

                if date not in arr:
                    arr[date] = {"date": date}

                for item in items["results"]:
                    project_id = item.get("project_id")
                    if not project_id or project_id not in projects:
                        continue

                    prefix = projects[project_id]
                    value = item.get("amount", {}).get("value", 0)

                    # накапливаем сумму по проекту
                    arr[date][prefix] = round(arr[date].get(prefix, 0) + value, 1)

    # 5. Возвращаем как список
    return list(arr.values())

def get_openai_cost_line_item(sk, projects, end_date):
    # --- 1) Даты ([-29 дней ; +1 день] от self.end_date) ---
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    start_dt = end_dt - timedelta(days=29)
    end_dt_plus1 = end_dt + timedelta(days=1)

    start_ts = int(start_dt.timestamp())
    end_ts = int(end_dt_plus1.timestamp())

    # --- 2) HTTP-запрос ---
    url = "https://ccp.o.kg/g/openai/v1/organization/costs"
    headers = {
        "x-api-key": "da766b87-3934-4368-b66b-8739349368f9",
        "Authorization": f"Bearer {sk}",
    }
    params = {
        "start_time": start_ts,
        "end_time": end_ts,
        "bucket_width": "1d",
        "group_by": "project_id,line_item",
        "limit": 31,
    }

    resp = requests.get(url, headers=headers, params=params, verify=False)
    if resp.status_code != 200:
        return None
    payload = resp.json()

    # --- 3) Агрегация по проекту и дате ---
    summary: dict[str, dict[str, dict]] = {}

    data = payload.get("data", [])
    for items in data:
        results = items.get("results")
        start_time = items.get("start_time")
        if not results or start_time is None:
            continue

        # +6 часов, как в PHP
        date_str = datetime.fromtimestamp(start_time + 6 * 3600, tz=timezone.utc).strftime(
            "%Y-%m-%d")

        for item in results:
            project_id = item.get("project_id")
            if not project_id or project_id not in projects:
                continue

            project_name = projects[project_id]
            cost = (item.get("amount") or {}).get("value", 0) or 0
            line_item = (item.get("line_item") or "").lower()

            # инициализация ячейки
            proj_dates = summary.setdefault(project_name, {})
            day_row = proj_dates.setdefault(
                date_str,
                {"date": date_str, "input_cost": 0.0, "output_cost": 0.0, "total_cost": 0.0},
            )

            # распределяем по типам
            if "input" in line_item:
                day_row["input_cost"] += cost
            elif "output" in line_item:
                day_row["output_cost"] += cost

            day_row["total_cost"] += cost

    # --- 4) Приводим к формату {project: [ {date,...}, ... ]} ---
    for project, dates_map in list(summary.items()):
        rows = list(dates_map.values())
        rows.sort(key=lambda r: r["date"])
        summary[project] = rows

    return summary



def get_has_note_daily_control_data(projects, conn, start_date, end_date, date_list):
    getter = CriticalErrorAccuracyDataGetter(start_date=date_list[0], end_date=end_date, projects=projects, bpm_conn=conn)
    get_dagster_logger().info(f"params start_date = {start_date}, end_date = {end_date}, projects = {projects}")
    getter.staff_unit_id = bot_staff_unit_id
    get_dagster_logger().info(f"params staff_unit_id = {bot_staff_unit_id}")
    getter.set_days_trunked_data()
    get_dagster_logger().info(f"trunc = {getter.trunc}, date_format = {getter.date_format}, period = {getter.period}")
    result = getter.get_has_note_daily_control_data()
    get_dagster_logger().info(f" return {result}")

    return getter.get_has_note_daily_control_data()

def get_has_error_daily_control_data(projects, conn, start_date, end_date, date_list):
    getter = CriticalErrorAccuracyDataGetter(start_date=date_list[0], end_date=end_date, projects=projects, bpm_conn=conn)
    get_dagster_logger().info(f"params start_date = {start_date}, end_date = {end_date}, projects = {projects}")
    getter.staff_unit_id = bot_staff_unit_id
    getter.set_days_trunked_data()

    result = getter.get_has_error_daily_control_data()
    get_dagster_logger().info(f" return {result}")
    return result

def get_daily_detailed_data(period, projects, conn, dwh_conn, start_date, end_date):
    getter = MessagingPlatformDataGetter(conn=dwh_conn, start_date=start_date, end_date=end_date)
    setup_data_getter(getter, period, start_date, end_date)
    getter.project_ids = projects['ids']

    agg_data = getter.get_aggregated_data_detailed_with_started_at(exclude_splits=True)
    get_dagster_logger().info(f" agg_data = {agg_data}")

    getter = MessagingPlatformDataGetter(conn=conn, start_date=start_date, end_date=end_date)
    setup_data_getter(getter, period, start_date, end_date)
    getter.project_ids = projects['ids']

    incoming = getter.get_entered_chats(exclude_splits=True)
    get_dagster_logger().info(f" incoming = {incoming}")
    result = left_join_array(agg_data, incoming, date_field)
    get_dagster_logger().info(f"date_field {date_field}")
    get_dagster_logger().info(f" result after incoming = {result}")
    return result

def request_data(projects, conn, splits=None, date_list=None, end_date=None):
    if splits is None:
        splits = []

    requests_data_getter = OmniDataGetter(start_date=date_list[0], end_date=end_date, splits=splits, project_ids=projects, conn=conn)
    requests_data_getter.set_days_trunked_data()
    result = requests_data_getter.get_requests_data()
    get_dagster_logger().info(f" result = {result}")
    return result


def set_id_to_title(data):
    projects = {
        1: "Saima",
        4: "Nur",
        12: "O!Store",
        10: "Akcha Bulak",
        11: "Money",
        5: "Halyk Bank",
        15: "O!Bank",
        19: "O!Brand",
    }

    for item in data:
        if hasattr(item, "project_id"):
            item.project = projects.get(item.project_id)
        elif isinstance(item, dict) and "project_id" in item:
            item["project"] = projects.get(item["project_id"])

    return data

def setup_data_getter(data_getter, period, start_date, end_date, report_date=None, month_start=None, month_end=None):
    if period == 'hour':
        data_getter.start_date = report_date
        data_getter.end_date = end_date
        data_getter.date_field = 'hour'
        data_getter.set_hours_trunked_data()
        data_getter.date_format = "HH24:MI:SS"
    elif period == 'day':
        data_getter.start_date = start_date
        data_getter.end_date = end_date
        data_getter.date_field = 'date'
        data_getter.set_days_trunked_data()
    elif period == 'month':
        data_getter.start_date = month_start
        data_getter.end_date = month_end
        data_getter.set_months_trunked_data()
        data_getter.date_field = "date"

@op(
    required_resource_keys={"report_utils"},
    ins={"file_path": In(str, description="Абсолютный путь к PDF файлу.")},
    out=Out(dict, description="Ответ API как dict (JSON) либо {'status_code', 'text'}"),
    description=(
        "Загружает PDF-отчёт на /api/report-instances/upload/{report_id} (RV) по схеме как в curl: "
        "заголовок x-api-key и форма file/title/day. "
        "Дата 'day' берётся из report_utils.ReportDate (формат YYYY.MM.DD)."
    ),
)
def upload_report_instance_chat_bot_op(context, file_path: str) -> Dict[str, Any]:
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
    report_id = int(cfg.get("report_id", 2))
    api_key = cfg.get("api_key") or os.getenv("REPORTS_API_KEY") or os.getenv("RV_REPORTS_API_KEY")
    if not api_key:
        raise Failure(
            description="Не задан API ключ. Укажите op_config.api_key или REPORTS_API_KEY / RV_REPORTS_API_KEY."
        )

    timeout_connect = int(cfg.get("timeout_connect", 10))
    timeout_read = int(cfg.get("timeout_read", 120))
    verify_ssl = bool(cfg.get("verify_ssl", True))

    title = cfg.get("title") or "ЧАТ-БОТ | Общая ежедневная статистика"

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