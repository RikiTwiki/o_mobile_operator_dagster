import os
from collections import defaultdict


from utils.templates.tables.header_class import  SectionHeader
from dagster import op, In, Out, AssetKey, Output, MetadataValue
from typing import List, Dict, Any, Optional
import pandas as pd

from utils.statistic_dates import StatisticDates
from utils.templates.tables.MatplotlibTableConstructor2 import MatplotlibTableConstructor2, get_data_in_dates
from utils.templates.tables.basic_table_plt import BasicTable
from utils.templates.tables.header_class import SectionHeader
from utils.mail.mail_report import MailReport

from utils.path import BASE_SQL_PATH
from utils.transformers.mp_report import daily_params, extract_from_sql, monthly_params

from utils.transformers.mp_report import process_aggregation_project_data

from utils.templates.charts.multi_axis_chart import MultiAxisChart

from utils.templates.tables.pivot_table_plt import PivotTable

from utils.transformers.mp_report import remap_project_titles, left_join_array

from utils.templates.charts.basic_line_graphics import BasicLineGraphics
from utils.pdf_report import PDFReport2

@op(
    required_resource_keys={"source_dwh", "report_utils", "mp_daily_config"},
    description="Агрегированные по дням данные (Saima)",
)
def saima_aggregated_by_day(context):
    params = daily_params(context)
    SQL_AGG = os.path.join(BASE_SQL_PATH, "mp", "get_aggregated.sql")
    return extract_from_sql(context, params, [1], SQL_AGG)

@op(
    required_resource_keys={"source_dwh", "report_utils", "mp_daily_config"},
    description="Агрегированные по дням данные ('General', 'Emag')",
)
def general_aggregated_by_day(context):
    params = daily_params(context)
    SQL_AGG = os.path.join(BASE_SQL_PATH, "mp", "get_aggregated.sql")
    return extract_from_sql(context, params, [3], SQL_AGG)

@op(
    required_resource_keys={"source_dwh", "report_utils", "mp_daily_config"},
    description="Агрегированные по дням данные ('AkchaBulak', 'Money', 'Agent', 'Bank', 'O!Bank')",
)
def other_aggregated_by_day(context):
    params = daily_params(context)
    SQL_AGG = os.path.join(BASE_SQL_PATH, "mp", "get_aggregated.sql")
    return extract_from_sql(context, params, [4,5,6,8,9], SQL_AGG)

@op(
    required_resource_keys={"source_dwh", "report_utils", "mp_daily_config"},
    description="Агрегированные по дням данные (Saima)",
)
def saima_entered_by_day(context):
    params = daily_params(context)
    SQL_ENTERED = os.path.join(BASE_SQL_PATH, "mp", "get_entered_chats.sql")
    return extract_from_sql(context, params, [1], SQL_ENTERED)

@op(
    required_resource_keys={"source_dwh", "report_utils", "mp_daily_config"},
    description="Агрегированные по дням данные ('General', 'Emag')",
)
def general_entered_by_day(context):
    params = daily_params(context)
    SQL_ENTERED = os.path.join(BASE_SQL_PATH, "mp", "get_entered_chats.sql")
    return extract_from_sql(context, params, [3], SQL_ENTERED)

@op(
    required_resource_keys={"source_dwh", "report_utils", "mp_daily_config"},
    description="Агрегированные по дням данные ('AkchaBulak', 'Money', 'Agent', 'Bank', 'O!Bank')",
)
def other_entered_by_day(context):
    params = daily_params(context)
    SQL_ENTERED = os.path.join(BASE_SQL_PATH, "mp", "get_entered_chats.sql")
    return extract_from_sql(context, params, [4,5,6,8,9], SQL_ENTERED)

@op(
    required_resource_keys={"source_dwh", "report_utils", "mp_daily_config"},
    description="Агрегированные по дням данные (Saima)",
)
def saima_handled_by_day(context):
    params = daily_params(context)
    SQL_ALL_HANDLED = os.path.join(BASE_SQL_PATH, "mp", "get_all_aggregated.sql")
    return extract_from_sql(context, params, [1], SQL_ALL_HANDLED)

@op(
    required_resource_keys={"source_dwh", "report_utils", "mp_daily_config"},
    description="Агрегированные по дням данные ('General', 'Emag')",
)
def general_handled_by_day(context):
    params = daily_params(context)
    SQL_ALL_HANDLED = os.path.join(BASE_SQL_PATH, "mp", "get_all_aggregated.sql")
    return extract_from_sql(context, params, [3], SQL_ALL_HANDLED)

@op(
    required_resource_keys={"source_dwh", "report_utils", "mp_daily_config"},
    description="Агрегированные по дням данные ('AkchaBulak', 'Money', 'Agent', 'Bank', 'O!Bank')",
)
def other_handled_by_day(context):
    params = daily_params(context)
    SQL_ALL_HANDLED = os.path.join(BASE_SQL_PATH, "mp", "get_all_aggregated.sql")
    return extract_from_sql(context, params, [4,5,6,8,9], SQL_ALL_HANDLED)

@op(
    required_resource_keys={"source_dwh", "report_utils", "mp_daily_config"},
    description="Агрегированные по дням данные (saima)",
)
def saima_bot_handled_by_day(context):
    params = daily_params(context)
    SQL_BOT_HANDLED = os.path.join(BASE_SQL_PATH, "mp", "bot_handled_chats.sql")
    return extract_from_sql(context, params, [1], SQL_BOT_HANDLED)

@op(
    required_resource_keys={"source_dwh", "report_utils", "mp_daily_config"},
    description="Агрегированные по дням данные (general, emag)",
)
def general_bot_handled_by_day(context):
    params = daily_params(context)
    SQL_BOT_HANDLED = os.path.join(BASE_SQL_PATH, "mp", "bot_handled_chats.sql")
    return extract_from_sql(context, params, [3], SQL_BOT_HANDLED)

@op(
    required_resource_keys={"source_dwh", "report_utils", "mp_daily_config"},
    description="Агрегированные по дням данные",
)
def other_bot_handled_by_day(context):
    params = daily_params(context)
    SQL_BOT_HANDLED = os.path.join(BASE_SQL_PATH, "mp", "bot_handled_chats.sql")
    return extract_from_sql(context, params, [4,5,6,8,9], SQL_BOT_HANDLED)


@op(
    required_resource_keys={"source_dwh", "report_utils", "mp_daily_config"},
    description="Агрегированные за месяц (saima)",
)
def saima_aggregated_by_month(context):
    params = monthly_params(context)
    SQL_AGG = os.path.join(BASE_SQL_PATH, "mp", "get_aggregated.sql")
    return extract_from_sql(context, params, [1], SQL_AGG)

@op(
    required_resource_keys={"source_dwh", "report_utils", "mp_daily_config"},
    description="Агрегированные за месяц (General, Emag)",
)
def general_aggregated_by_month(context):
    params = monthly_params(context)
    SQL_AGG = os.path.join(BASE_SQL_PATH, "mp", "get_aggregated.sql")
    return extract_from_sql(context, params, [3], SQL_AGG)

@op(
    required_resource_keys={"source_dwh", "report_utils", "mp_daily_config"},
    description="Агрегированные за месяц ('AkchaBulak', 'Money', 'Agent', 'Bank', 'O!')",
)
def other_aggregated_by_month(context):
    params = monthly_params(context)
    SQL_AGG = os.path.join(BASE_SQL_PATH, "mp", "get_aggregated.sql")
    return extract_from_sql(context, params, [4, 5, 6, 8, 9], SQL_AGG)


@op(
    required_resource_keys={"source_dwh", "report_utils", "mp_daily_config"},
    description="Показатель self-service по проектам Saima",
)
def saima_service_rate_by_day_op(context):
    """
    Аналог Php кода allSelfServiceRate: загружает данные по self-service rate для проектов 1
    """
    params = daily_params(context)
    sql_path = os.path.join(BASE_SQL_PATH, "mp", "self_service_rate.sql")
    ret = extract_from_sql(context, params, [1], sql_path)
    context.log.info(ret)
    return extract_from_sql(context, params, [1], sql_path)

@op(
    required_resource_keys={"source_dwh", "report_utils", "mp_daily_config"},
    description="Показатель self-service по проектам Saima",
)
def bank_service_rate_by_day_op(context):
    """
    Аналог Php кода allSelfServiceRate: загружает данные по self-service rate для проектов 3
    """
    params = daily_params(context)
    sql_path = os.path.join(BASE_SQL_PATH, "mp", "self_service_rate.sql")
    return extract_from_sql(context, params, [8, 9], sql_path)

@op(
    required_resource_keys={"source_dwh", "report_utils", "mp_daily_config"},
    description="Показатель self-service по проектам Saima",
)
def service_rate_saima_by_month_op(context):
    """
    Аналог Php кода allSelfServiceRate: загружает данные по self-service rate для проектов 1
    """
    params = monthly_params(context)
    sql_path = os.path.join(BASE_SQL_PATH, "mp", "self_service_rate.sql")
    records = extract_from_sql(context, params, [1], sql_path)
    if records and 'self_service_rate' in records[0]:
        rate = records[0]['self_service_rate']
    else:
        rate = 0.0
    rate_float = float(rate)
    context.log.info(f'return: {rate_float}')
    return rate_float

@op(
    required_resource_keys={"source_dwh", "report_utils", "mp_daily_config"},
    description="Показатель месяцы self-service по проектам Bank",
)
def service_rate_bank_by_month_op(context):
    """
    Аналог Php кода allSelfServiceRate: загружает данные по self-service rate для проектов 8, 9
    """
    params = monthly_params(context)
    sql_path = os.path.join(BASE_SQL_PATH, "mp", "self_service_rate.sql")
    records = extract_from_sql(context, params, [8, 9], sql_path)
    if records and 'self_service_rate' in records[0]:
        rate = records[0]['self_service_rate']
    else:
        rate = 0.0
    rate_float = float(rate)
    context.log.info(f'return: {rate_float}')
    return rate_float

# 1) Сборка дневной и месячной агрегации в единый словарь
@op(
    required_resource_keys={"source_dwh"},
    ins={
        "aggregated_by_day":   In(asset_key=AssetKey(["aggregated_by_day"])),
        "entered_by_day":      In(asset_key=AssetKey(["entered_by_day"])),
        "all_handled_by_day":  In(asset_key=AssetKey(["all_handled_by_day"])),
        "bot_handled_by_day":  In(asset_key=AssetKey(["bot_handled_by_day"])),
        "aggregated_by_month": In(asset_key=AssetKey(["aggregated_by_month"])),
    },
    out=Out(Dict),
)
def all_projects_data(
    context,
    aggregated_by_day,
    entered_by_day,
    all_handled_by_day,
    bot_handled_by_day,
    aggregated_by_month,
) -> Dict:
    sources = {
        "entered_by_day": entered_by_day,
        "all_handled_by_day": all_handled_by_day,
        "bot_handled_by_day": bot_handled_by_day,
    }

    df = pd.DataFrame(aggregated_by_day)

    for name, src in sources.items():
        df_src = pd.DataFrame(src)
        context.log.info(f"{name} columns: {df_src.columns.tolist()}")
        if "date" in df_src.columns:
            df = df.merge(df_src, on="date", how="left")
        else:
            context.log.warning(
                f"Источник `{name}` не содержит колонки 'date', merge пропущен."
            )
    df = df.fillna(0)
    day_data = df.to_dict(orient="records")

    if aggregated_by_month:
        m0 = aggregated_by_month[0]
        month_avg_rt = round(m0.get("average_reaction_time", 0))
        month_avg_sta = round(m0.get("average_speed_to_answer", 0))
    else:
        month_avg_rt = month_avg_sta = 0

    context.log.info(f" 'dayData':{day_data}, 'monthAverageReactionTime': {month_avg_rt}, 'monthAverageSpeedToAnswer': {month_avg_sta},")
    return {
        "dayData": day_data,
        "monthAverageReactionTime": month_avg_rt,
        "monthAverageSpeedToAnswer": month_avg_sta,
    }
@op(out=Out(Dict))
def saima_project_data(context,
    saima_aggregated_by_day,
    saima_entered_by_day,
    saima_all_handled_by_day,
    saima_bot_handled_by_day,
    saima_aggregated_by_month
) -> Dict:
    return process_aggregation_project_data(context, saima_aggregated_by_day, saima_entered_by_day, saima_all_handled_by_day, saima_bot_handled_by_day, saima_aggregated_by_month)

@op(out=Out(Dict))
def general_project_data(context,
    general_aggregated_by_day,
    general_entered_by_day,
    general_all_handled_by_day,
    general_bot_handled_by_day,
    general_aggregated_by_month)-> Dict:
    return process_aggregation_project_data(context, general_aggregated_by_day, general_entered_by_day, general_all_handled_by_day, general_bot_handled_by_day, general_aggregated_by_month)

@op(out=Out(Dict))
def other_project_data(context,
    other_aggregated_by_day,
    other_entered_by_day,
    other_all_handled_by_day,
    other_bot_handled_by_day,
    other_aggregated_by_month) -> Dict:
    return process_aggregation_project_data(context, other_aggregated_by_day, other_entered_by_day, other_all_handled_by_day, other_bot_handled_by_day, other_aggregated_by_month)

# 2) Загрузка ассета all_service_rate_by_day
@op(ins={"all_service_rate_by_day": In(asset_key=AssetKey(["all_service_rate_by_day"]))}, out=Out(List[Dict]))
def load_self_service_rate(context, all_service_rate_by_day: List[Dict]) -> List[Dict]:
    context.log.info(f'return: {all_service_rate_by_day}')
    return all_service_rate_by_day

# 3) Загрузка ассета service_rate_all_by_month
@op(ins={"service_rate_all_by_month": In(asset_key=AssetKey(["service_rate_all_by_month"]))}, out=Out(float))
def load_month_self_service_rate(context, service_rate_all_by_month: List[Dict]) -> float:
    if service_rate_all_by_month:
        rate = service_rate_all_by_month[0].get("self_service_rate", 0)
        context.log.info(f'return: {float(rate)}')

        return float(rate)
    context.log.info(f'return: {0.0}')

    return 0.0

@op(
    required_resource_keys={"source_dwh", "report_utils", "mp_daily_config"},
)
def day_data_messengers_Detailed_op(context):
    params = daily_params(context)
    sql_path = os.path.join(BASE_SQL_PATH, "mp", "get_project_detailed.sql")
    with open(sql_path, "r", encoding="utf-8") as f:
        sql = f.read()

    with context.resources.source_dwh as conn:
        df = pd.read_sql_query(sql, conn, params=params)

    context.log.info(f'return: {df.to_dict(orient="records")}')
    return df.to_dict(orient="records")

@op(
    required_resource_keys={"source_dwh", "report_utils", "mp_daily_config"},
)
def day_MPData_Get_Users_Detailed_op(context):
    params = daily_params(context)
    sql_path = os.path.join(BASE_SQL_PATH, "mp", "get_users_detailed.sql")
    with open(sql_path, "r", encoding="utf-8") as f:
        sql = f.read()
    with context.resources.source_dwh as conn:
        df = pd.read_sql_query(sql, conn, params=params)

    context.log.info(f'return: {df.to_dict(orient="records")}')
    return df.to_dict(orient="records")

@op(
    required_resource_keys={"source_dwh", "report_utils", "mp_daily_config"},
)
def day_MPData_Get_Users_Detailed_By_View_op(context):
    params = daily_params(context)
    sql_path = os.path.join(BASE_SQL_PATH, "mp", "get_users_detailed_byview.sql")
    with open(sql_path, "r", encoding="utf-8") as f:
        sql = f.read()
    with context.resources.source_dwh as conn:
        df = pd.read_sql_query(sql, conn, params=params)

    context.log.info(f'return: {df.to_dict(orient="records")}')
    return df.to_dict(orient="records")

@op(
    required_resource_keys={"source_dwh", "report_utils", "mp_daily_config"},
)
def normal_shif_logins_op(context):
    dates = context.resources.report_utils.get_utils()
    params = daily_params(context)
    params['month_start'] = dates.MonthStart
    params['position_ids'] = [10, 41, 46]
    params_all = params.copy()
    params['excluded_shifts'] = [9, 10, 11, 12, 13, 19]
    sql = """
        SELECT DISTINCT
            u.login,
            sv.login   AS supervisor,
            d.abbreviation
        FROM bpm.staff_units AS su
        LEFT JOIN bpm.positions AS p
            ON p.id = su.position_id
        LEFT JOIN bpm.departments AS d
            ON d.id = p.department_id
        LEFT JOIN bpm.team_assigns AS ts
            ON ts.staff_unit_id = su.id
           AND ts.month = %(month_start)s
        LEFT JOIN bpm.users AS u
            ON u.id = su.user_id
        LEFT JOIN bpm.staff_units AS sus
            ON ts.staff_unit_team_lead_id = sus.id
        LEFT JOIN bpm.users AS sv
            ON sv.id = sus.user_id
        LEFT JOIN bpm.timetable_data_detailed AS tdd
            ON tdd.staff_unit_id = su.id
        WHERE su.position_id = ANY(%(position_ids)s)
          AND (
                (su.accepted_at < %(start_date)s AND su.dismissed_at >= %(end_date)s)
             OR (su.accepted_at < %(end_date)s     AND su.dismissed_at IS NULL)
          )
          AND su.user_id IS NOT NULL
          AND tdd.date = %(start_date)s
    """
    sql_filter = sql
    if params['excluded_shifts']:
        sql_filter += "\n    AND tdd.timetable_shift_id = ANY(%(excluded_shifts)s)"

    with context.resources.source_dwh as conn:
        df_cross = pd.read_sql_query(sql_filter, conn, params=params)
    with context.resources.source_dwh as conn:
        df_all = pd.read_sql_query(sql, conn, params=params_all)

    normal_shift_logins = [
        login
        for login in df_all['login']
        if login not in df_cross['login']
    ]
    context.log.info(f'return: {normal_shift_logins}')
    return normal_shift_logins

@op(
    required_resource_keys={"source_dwh", "report_utils", "mp_daily_config"},
)
def normal_Shift_Statuses_op(context, logins):
    params = daily_params(context)
    params['position_ids'] = [10, 41, 46]
    params['logins'] = logins
    sql_path = os.path.join(BASE_SQL_PATH, "mp", "get_user_statuses_by_positions.sql")

    with open(sql_path, "r", encoding="utf-8") as f:
        sql = f.read()
    with context.resources.source_dwh as conn:
        df = pd.read_sql_query(sql, conn, params=params)

    context.log.info(f'return: {df.to_dict(orient="records")}')
    return df.to_dict(orient="records")

@op(
    required_resource_keys={"source_dwh", "report_utils", "mp_daily_config"},
)
def cross_Day_Shift_Sheets_op(context):
    params = daily_params(context)
    params['position_ids'] = [10, 41, 46]
    sql_path = os.path.join(BASE_SQL_PATH, "mp", "get_active_user_sheets.sql")
    with open(sql_path, "r", encoding="utf-8") as f:
        sql = f.read()
    with context.resources.source_dwh as conn:
        df = pd.read_sql_query(sql, conn, params=params)
    context.log.info(f'return: {df.to_dict(orient="records")}')
    return df.to_dict(orient="records")

@op(
    required_resource_keys={"source_dwh", "report_utils", "mp_daily_config"},
)
def cross_Day_Shift_statuses_op(context, data):
    params = monthly_params(context)
    params['position_ids'] = [10, 41, 46]
    accum = []
    sql_path = os.path.join(BASE_SQL_PATH, "mp", "get_statuses_duration_without_date.sql")
    with open(sql_path, "r", encoding="utf-8") as f:
        sql = f.read()
    for item in data:
        params['start_date'] = item['session_start']
        params['end_date'] = item['session_end']
        params['date'] = item['date']
        params['login'] = item['login']

        with context.resources.source_dwh as conn:
            statuses = pd.read_sql_query(sql, conn, params=params)

        if not statuses.empty:
            statuses = statuses.to_dict(orient="records")
            accum.extend(statuses)

    context.log.info(f'accum: {accum}')

    groups = defaultdict(list)
    for item in accum:
        key = f"{item['date']}_{item['login']}_{item['code']}"
        groups[key].append(item)

    result = []
    for group in groups.values():
        first = group[0]
        total_seconds = sum(item['seconds'] for item in group)
        total_minutes = round(sum(float(item['minutes']) for item in group), 2)
        total_hours   = round(sum(float(item['hours'])   for item in group), 3)

        result.append({
            'date':    first['date'],
            'login':   first['login'],
            'code':    first['code'],
            'seconds': total_seconds,
            'minutes': total_minutes,
            'hours':   total_hours,
        })

    context.log.info(f'result: {result}')
    return result

@op(required_resource_keys={"source_dwh", "report_utils", "mp_daily_config"},
    )
def all_users_op(context):
    dates = context.resources.report_utils.get_utils()
    params = daily_params(context)
    params['month_start'] = dates.MonthStart
    params['position_ids'] = [10, 41, 46]
    sql = """
        SELECT DISTINCT
            u.login,
            sv.login   AS supervisor,
            d.abbreviation
        FROM bpm.staff_units AS su
        LEFT JOIN bpm.positions AS p
            ON p.id = su.position_id
        LEFT JOIN bpm.departments AS d
            ON d.id = p.department_id
        LEFT JOIN bpm.team_assigns AS ts
            ON ts.staff_unit_id = su.id
           AND ts.month = %(month_start)s
        LEFT JOIN bpm.users AS u
            ON u.id = su.user_id
        LEFT JOIN bpm.staff_units AS sus
            ON ts.staff_unit_team_lead_id = sus.id
        LEFT JOIN bpm.users AS sv
            ON sv.id = sus.user_id
        LEFT JOIN bpm.timetable_data_detailed AS tdd
            ON tdd.staff_unit_id = su.id
        WHERE su.position_id = ANY(%(position_ids)s)
          AND (
                (su.accepted_at < %(start_date)s AND su.dismissed_at >= %(end_date)s)
             OR (su.accepted_at < %(end_date)s     AND su.dismissed_at IS NULL)
          )
          AND su.user_id IS NOT NULL
          AND tdd.date = %(start_date)s
    """
    with context.resources.source_dwh as conn:
        df_all = pd.read_sql_query(sql, conn, params=params)

    context.log.info(f'df_all: {df_all}')
    return df_all.to_dict(orient="records")


