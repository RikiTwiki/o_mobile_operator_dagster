import base64
import os
from collections import defaultdict
from datetime import datetime, date
from io import BytesIO
import matplotlib.image as mpimg
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from utils.templates.tables.header_class import  SectionHeader
from dagster import op, In, Out, AssetKey, Output, MetadataValue
from typing import List, Dict, Any, Optional
import pandas as pd
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.figure import Figure
from dagster import get_dagster_logger
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

from utils.transformers.mp_report import remap_project_titles

from utils.templates.charts.basic_line_graphics import BasicLineGraphics

from utils.pdf_report import PDFReport2


from utils.transformers.mp_report import left_join_array


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

@op(required_resource_keys={"source_dwh", "report_utils", "mp_daily_config"},
    # out=DynamicOut()
    )
def statuses_op(context, normal_Shift_Statuses, cross_Day_Shift_statuses, all_users):
    dates = context.resources.report_utils.get_utils()
    statuses = normal_Shift_Statuses + cross_Day_Shift_statuses
    statuses = left_join_array(statuses, all_users, key='login')
    statuses = list(statuses)
    groups = defaultdict(list)
    context.log.info(f'statuses: {statuses}')
    context.log.info(f'groups: {groups}')

    STATUSES = {
        'absent': 'Отсутствует',
        'lunch': 'Обед',
        'dnd': 'Блокировка по инициативе супервайзера',
        'normal': 'Normal',
        'social_media': 'Social-Media',
        'eshop': 'eshop',
        'coffee': 'Кофе',
        'pre_coffee': 'Pre-Coffee',
    }
    for item in statuses:
        code = item.get('code')
        groups[code].append(item)
    groups = {code: items for code, items in groups.items() if code in STATUSES}
    context.log.info(f'gpoups: {groups}')
    figs = []
    for code, itemss in groups.items():
        for item in itemss:
            raw = item.get("date")
            if isinstance(raw, (datetime, date)):
                # Конвертим datetime/date в строку 'YYYY-MM-DD'
                item["date"] = raw.strftime("%Y-%m-%d")
            else:
                # Если всё-таки строка с временем, обрезаем первые 10 символов
                item["date"] = str(raw)[:10]
        context.log.info(itemss)
        table = PivotTable(
            items=itemss,
            min_date=dates.StartDate,
            max_date=dates.EndDate,
            row_field="login",
            group_field="abbreviation",
            date_field="date",
            value_field="minutes",
            show_total_row=False,
            show_total_column=True,
            enable_cell_coloring=True,
            enable_zebra_striping=True,
            gradient_direction="asc",
            threshold="60%",
            font_size=8,
            column_widths={"login": 0.3, },
            cell_height=0.18,
            column_titles={
                "login": "Логин",
                "abbreviation": "Группа",
                "minutes": "Итого",
                "avg": "Средн.",
                "min": "Мин.",
                "max": "Макс."
            },
            target_resolution=(1920, 1200)
        )
        fig = table.render()
        figs.append(fig)
    return figs
        # buf = BytesIO()
        # fig.savefig(buf, format="png", bbox_inches="tight")
        # buf.seek(0)
        # b64 = base64.b64encode(buf.read()).decode("utf-8")
        # label = STATUSES.get(code, code)
        # yield DynamicOutput(
        #     fig,
        #     mapping_key=str(code),
        #     metadata={
        #
        #         "table_preview": MetadataValue.md(
        #             f"### Статус: {label}\n\n"
        #             f"![{code}](data:image/png;base64,{b64})"
        #         )
        #     }
        # )

@op(
    out=Out(Figure),
)
def render_aggregation_project_data_op(
    context,
    project_data: Dict[str, Any],
        self_service_rate: Optional[List[Dict[str, Any]]] = None,
        month_self_service_rate: Optional[float] = None,
) :
    # 2.2 Инициализируем дефолты внутри
    if self_service_rate is None:
        self_service_rate = []
    if month_self_service_rate is None:
        month_self_service_rate = 0.0

    # 1) Собираем и логируем данные
    full_day_data = project_data["dayData"]

    # 2) Берём последние 8 дней (включая сегодня)
    last8 = sorted(full_day_data, key=lambda x: x["date"], reverse=True)[:8][::-1]

    # 3) Форматируем даты для заголовков
    formatted_dates = []
    for rec in last8:
        # ожидаем формат "YYYY-MM-DD" или "YYYY-MM-DD HH:MM:SS"
        d_str = rec["date"].split(" ")[0]
        dt = datetime.strptime(d_str, "%Y-%m-%d")
        formatted_dates.append(dt.strftime("%d.%m"))


    # 4) Годовые/месячные метрики
    month_rt = project_data.get("monthAverageReactionTime", 0)
    month_sta = project_data.get("monthAverageSpeedToAnswer",  0)

    # 5) Хелпер для строк
    def get_metric_list(key: str) -> List[float]:
        vals = []
        for rec in last8:
            v = rec.get(key, 0) or 0
            try:
                fv = float(v)
            except Exception:
                fv = 0.0
            vals.append(round(fv, 1))
        return vals

    # 6) Заголовки и строки таблицы
    headers = ["Показатель", "План"] + formatted_dates + ["Среднее значение за текущий месяц"]
    table_data = []

    # 6.1 Reaction Time
    rt_vals = get_metric_list("average_reaction_time")
    table_data.append(
        ["Average Reaction Time", "≤ 60 сек"] + rt_vals + [month_rt]
    )

    # 6.2 Speed To Answer
    sta_vals = get_metric_list("average_speed_to_answer")
    table_data.append(
        ["Average Speed To Answer", "≤ 60 сек"] + sta_vals + [month_sta]
    )

    iso_dates = [rec["date"].split(" ")[0] for rec in last8]
    if self_service_rate:
        bsr_values = get_data_in_dates(
            self_service_rate,
            iso_dates,
            date_key="date",
            value_key="self_service_rate",
            precision=1,
        )
        # сразу добавляем готовую строку в table_data
        table_data.append([
            "Bot Self Service Rate (чат-бот)",
            "≥ 10%",
        ] + bsr_values + [round(month_self_service_rate or 0, 1)])

    # 7) Рисуем таблицу
    constructor = MatplotlibTableConstructor2()
    fig = constructor.render_table(
        headers,
        table_data,
        # title="Динамика по показателям за период",
        dpi=100
    )
    # 8) Сохраняем в буфер и отдаем base64 preview
    buf = BytesIO()
    fig.set_size_inches(16, 9)
    fig.savefig(buf, format="png", dpi=100)
    # тот же dpi, иначе будет не 1920x1080
    buf.seek(0)
    b64 = base64.b64encode(buf.read()).decode("utf-8")

    yield Output(
        fig,
        metadata={
            "table_preview": MetadataValue.md(f"![table](data:image/png;base64,{b64})")
        },
    )



@op(ins={"hour_MPData_Messengers_Detailed": In(asset_key=AssetKey(["hour_MPData_Messengers_Detailed"]))}, out=Out(Figure))
def render_Hour_Jivo_Data_Aggregated(hour_MPData_Messengers_Detailed):
    items = sorted(hour_MPData_Messengers_Detailed, key=lambda x: datetime.strptime(x['date'], '%H:%M:%S'))

    table = BasicTable()
    table.Items = items
    table.Fields = [
        {'field': 'date', 'title': 'Часы'},
        {'field': 'total_chats', 'title': 'Поступило \n обращений', 'summary': 'sum'},
        {'field': 'total_handled_chats', 'title': 'Обработано \n обращений', 'summary': 'sum'},
        {'field': 'total_by_operator', 'title': 'Закрыто без \n задержки \n (операторами)', 'summary': 'sum'},
        {'field': 'bot_handled_chats', 'title': 'Закрыто \n с задержкой \n 12 часов (чат-ботом)', 'summary': 'sum'},
        {'field': 'average_reaction_time', 'title': 'Average \n Reaction Time', 'paint': 'asc', 'round': 2},
        {'field': 'average_speed_to_answer', 'title': 'Average \n Speed To Answer', 'paint': 'asc', 'round': 2},
        {'field': 'user_total_reaction_time', 'title': 'Chat Total \n Reaction Time', 'paint': 'asc', 'round': 2},
        {'field': 'user_count_replies', 'title': 'Chat Count \n Replies', 'paint': 'desc', 'round': 2},
    ]

    # Вызов метода, который подготовит данные и построит таблицу
    fig = table.getTable()
    buf = BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    buf.seek(0)
    b64 = base64.b64encode(buf.read()).decode("utf-8")

    yield Output(
        fig,
        metadata={
            "table_preview": MetadataValue.md(f"![table](data:image/png;base64,{b64})")
        },
    )

@op(out=Out(Figure),
)
def render_Mixed_Graph(project_data: Dict[str, Any],
):
    """
    Обёртка для быстрой настройки графика в стиле Jivo,
    аналогична PHP-функции renderMixedGraph.
    Изменить названия легенды, заголовок и параметры можно здесь.
    """
    project_data = project_data["dayData"]

    for item in project_data:
        item['date_dt'] = datetime.strptime(item['date'], '%Y-%m-%d')
    project_data.sort(key=lambda x: x['date_dt'])
    for item in project_data:
        del item['date_dt']

    # Вычисляем максимальное значение
    max_val = 100
    for item in project_data:
        item['bot_handled_chats'] = item.get('bot_handled_chats', 0)
        if item.get('total_chats', 0) > max_val:
            max_val = item['total_chats']

    # Параметры можно менять здесь
    chart = MultiAxisChart(
        graph_title="Данный график показывает кол-во обработанных обращений.",
        x_axes_name="Дата",
        y_axes_name="Кол-во",
        stack=True,
        show_tooltips=False,
        pointer=1,
        y_axes_min=0,
        y_axes_max=max_val,
        fields=[
            {"field": "date", "title": "", "type": "label"},
            {"field": "total", "title": "Кол-во чатов обработанных специалистами", "chartType": "bar",  "round": 1},
            {"field": "bot_handled_chats", "title": "Кол-во чатов обработанных чат-ботом", "chartType": "bar",  "round": 1},
            {"field": "total_chats", "title": "Кол-во поступивших чатов", "chartType": "line", "round": 1},
        ],
        items=project_data
    )
    fig, ax = plt.subplots(figsize=(19.2, 8), dpi=100)
    chart.render_graphics(ax)
    buf = BytesIO()
    fig.savefig(buf, format="png")
    buf.seek(0)
    b64 = base64.b64encode(buf.read()).decode("utf-8")

    yield Output(
        fig,
        metadata={
            "table_preview": MetadataValue.md(f"![table](data:image/png;base64,{b64})")
        },
    )

@op(out = Out(Figure))
def render_Jivo_Total31_DaysDetailed(data):
    data = remap_project_titles(data)
    table = PivotTable(
        items=data,
        row_field="project",
        date_field="date",
        value_field="total",
        show_total_row=True,
        show_total_column=True,
        enable_cell_coloring=True,
        enable_zebra_striping=True,
        gradient_direction="desc",
        threshold="10%",
        font_size=10,
        column_widths={
            "project": 0.3,
        },
        cell_height=0.18,
        column_titles={
            "project": "Период",
            "total": "Итого",
            "avg": "Средн.",
            "min": "Мин.",
            "max": "Макс."
        },
        target_resolution=(1920, 1080)
    )
    fig = table.render()
    buf = BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    buf.seek(0)
    b64 = base64.b64encode(buf.read()).decode("utf-8")

    yield Output(
        fig,
        metadata={
            "table_preview": MetadataValue.md(f"![table](data:image/png;base64,{b64})")
        },
    )

@op(out = Out(Figure))
def render_Jivo_Total31_Days_Users_Detailed(data):
    table = PivotTable(
        items=data,
        row_field="title",
        group_field="abbreviation",
        date_field="date",
        value_field="total",
        show_total_row=True,
        show_total_column=True,
        enable_cell_coloring=True,
        enable_zebra_striping=True,
        gradient_direction="desc",
        threshold="10%",
        font_size=10,
        column_widths={
            "title": 0.3,
            "abbreviation": 0.1
        },
        cell_width=None,
        cell_height=0.25,
        column_titles={
            "title": "Период",
            "abbreviation": "Группа",
            "total": "Итого",
            "avg": "Средн.",
            "min": "Мин.",
            "max": "Макс."
        },
        target_resolution = (1920, 1600)

    )
    fig = table.render()
    buf = BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    buf.seek(0)
    b64 = base64.b64encode(buf.read()).decode("utf-8")

    yield Output(
        fig,
        metadata={
            "table_preview": MetadataValue.md(f"![table](data:image/png;base64,{b64})")
        },
    )

@op(out = Out(Figure))
def render_Total31_Days_Users_Detailed_With_Shifts(data):
    table = PivotTable(
        items=data,
        row_field="title",
        group_field="shifts",
        date_field="date",
        value_field="total",
        show_total_row=True,
        show_total_column=True,
        enable_cell_coloring=True,
        enable_zebra_striping=True,
        gradient_direction="desc",
        threshold="10%",
        split_time=False,
        font_size=10,
        column_widths={
            "title": 0.3,
            "shifts": 0.2
        },
        cell_width=None,
        cell_height=0.25,
        column_titles={
            "title": "Период",
            "shifts": "Смена",
            "total": "Итого",
            "avg": "Средн.",
            "min": "Мин.",
            "max": "Макс."
        },
        target_resolution=(1920, 1920)
    )
    fig = table.render()
    buf = BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    buf.seek(0)
    b64 = base64.b64encode(buf.read()).decode("utf-8")

    plt.close(fig)
    yield Output(
        fig,
        metadata={
            "table_preview": MetadataValue.md(f"![table](data:image/png;base64,{b64})")
        },
    )

@op(out = Out(Figure))
def render_styled_reaction_time_graph(items:  Dict[str, Any]):
    items = items['dayData']

    """
    Функция создаёт стилизованный график среднего времени реакции на первое сообщение.
    Использует объект BasicLineGraphics для подготовки данных и matplotlib для построения графика.
    Показатель Average Reaction Time
    Графики 2.2.1, 2.2.2, 2.2.3, 2.2.4 (Текстовый отчет),
    """
    items = sorted(
        items,
        key=lambda x: datetime.strptime(x['date'], '%Y-%m-%d')
    )
    graphic = BasicLineGraphics(
        graph_title="Данный график показывает среднее время реакции на первое сообщение.",
        x_axes_name="Дни",
        y_axes_name="Секунды",
        target=60,
        stroke='smooth',
        show_tooltips=True,
        y_axes_min=0,
        items=items,
        fields=[
            {'field': 'date', 'title': 'Дни', 'type': 'label'},
            {
                'field': 'average_reaction_time',
                'title': 'Среднее время реакции на первое сообщение',
                'round': 1,
            },
        ],
        width=1920,
        height=400,
    )

    graphic.make_preparation()

    dpi = 100
    fig, ax = plt.subplots(figsize=(graphic.width / dpi, graphic.height / dpi), dpi=dpi)
    fig.patch.set_facecolor('#f8f9fa')   # Цвет фона всей фигуры
    ax.set_facecolor('#ffffff')          # Цвет фона графика

    # Шаг 4: Построение линии графика
    dates = [datetime.strptime(label.split()[0], '%d.%m') for label in graphic.labels]

    for ds in graphic.datasets:
        y = ds['data']
        ax.plot(dates, y, color='black', linewidth=2.5, label=ds['name'])  # Основная линия

        # Добавление точек и аннотаций на линии
        for date, value in zip(dates, y):
            if value is not None:
                ax.plot(date, value, marker='o', markersize=6, markerfacecolor='#2c5282',
                        markeredgecolor='white', markeredgewidth=1)
                ax.annotate(f'{value:.1f}', (date, value),
                            textcoords="offset points", xytext=(0, 0),
                            ha='center', fontsize=8, fontweight='bold',
                            bbox=dict(boxstyle="round,pad=0.3", facecolor='black',
                                      edgecolor='none', alpha=0.8),
                            color='white')

    # Шаг 5: Добавление зелёной горизонтальной линии — "цели"
    if graphic.target is not None:
        ax.axhline(y=graphic.target, color='#38a169', linestyle='-', linewidth=2, alpha=0.8)
        ax.text(dates[0], graphic.target + 2, 'План',
                color='white', fontweight='bold', fontsize=8,
                bbox=dict(boxstyle="round,pad=0.3", facecolor='#38a169', edgecolor='none'))

    # Шаг 6: Добавление горизонтальных цветных полос (для визуального разделения уровней)
    all_values = [val for ds in graphic.datasets for val in ds['data'] if val is not None]
    if all_values:
        min_val = 0
        max_val = max(all_values)
        padding = (max_val - min_val) * 0.15
        ax.set_ylim(min_val, max_val + padding)

        band_height = 20
        start_band = int(min_val // band_height) * band_height
        end_band = int(max_val // band_height + 2) * band_height
        for i in range(start_band, end_band, band_height):
            if (i // band_height) % 2 == 1:
                ax.axhspan(i, i + band_height, alpha=0.05, color='gray', zorder=0)



    # Шаг 7: Настройка осей и сетки
    ax.grid(True, linestyle='-', linewidth=0.5, color='#e2e8f0', alpha=0.7, axis='y')
    ax.set_axisbelow(True)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%d.%m'))  # Формат дат на оси X
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=0, fontsize=9)

    # Шаг 8: Подписи осей и стилизация рамок графика
    ax.set_xlabel(graphic.x_axes_name, fontsize=10, color='#4a5568')
    ax.set_ylabel(graphic.y_axes_name, fontsize=10, color='#4a5568')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_color('#e2e8f0')
    ax.spines['bottom'].set_color('#e2e8f0')
    ax.tick_params(colors='#4a5568', labelsize=9)

    # Шаг 9: Добавление заголовка графика
    ax.text(0.5, 1.08, graphic.graph_title,
            horizontalalignment='center',
            transform=ax.transAxes,
            fontsize=11,
            color='#2d3748',
            wrap=True)

    buf = BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    buf.seek(0)
    b64 = base64.b64encode(buf.read()).decode("utf-8")

    plt.close()
    yield Output(
        fig,
        metadata={
            "table_preview": MetadataValue.md(f"![table](data:image/png;base64,{b64})")
        },
    )


@op(out = Out(Figure))
def render_Jivo_avgreactiontime31_DaysDetailed(data):
    data = remap_project_titles(data)
    table = PivotTable(
        items=data,
        row_field="project",
        date_field="date",
        value_field="average_reaction_time",
        show_total_row=False,
        show_total_column=False,
        enable_cell_coloring=True,
        enable_zebra_striping=True,
        gradient_direction="asc",
        threshold="60%",
        font_size=10,
        column_widths={
            "project": 0.3,
        },
        cell_height=0.25,
        column_titles={
            "project": "Период",
            "avg": "Средн.",
            "min": "Мин.",
            "max": "Макс."
        },
        target_resolution=(1920, 1080)
    )
    fig = table.render()
    buf = BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    buf.seek(0)
    b64 = base64.b64encode(buf.read()).decode("utf-8")

    yield Output(
        fig,
        metadata={
            "table_preview": MetadataValue.md(f"![table](data:image/png;base64,{b64})")
        },
    )

@op(out = Out(Figure))
def render_Jivo_avgreactiontime31_Days_Users_Detailed(data):
    table = PivotTable(
        items=data,
        row_field="title",
        group_field="abbreviation",
        date_field="date",
        value_field="average_reaction_time",
        show_total_row=False,
        show_total_column=False,
        enable_cell_coloring=True,
        enable_zebra_striping=True,
        gradient_direction="asc",
        threshold="60%",
        font_size=10,
        column_widths={
            "title": 0.3,
            "abbreviation": 0.1
        },
        cell_width=None,
        cell_height=0.25,
        column_titles={
            "title": "Период",
            "abbreviation": "Группа",
            "avg": "Средн.",
            "min": "Мин.",
            "max": "Макс."
        },
        target_resolution=(1920, 1600)
    )
    fig = table.render()
    buf = BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    buf.seek(0)
    b64 = base64.b64encode(buf.read()).decode("utf-8")

    yield Output(
        fig,
        metadata={
            "table_preview": MetadataValue.md(f"![table](data:image/png;base64,{b64})")
        },
    )



@op(out = Out(Figure))
def render_styled_speed_answer_time_graph(items:  Dict[str, Any]):
    items = items['dayData']
    items = sorted(
        items,
        key=lambda x: datetime.strptime(x['date'], '%Y-%m-%d')
    )
    graphic = BasicLineGraphics(
        graph_title="Данный график показывает среднее время ответа на первое сообщение.",
        x_axes_name="Дни",
        y_axes_name="Секунды",
        target=30   ,
        stroke='smooth',
        show_tooltips=True,
        y_axes_min=0,
        items=items,
        fields=[
            {'field': 'date', 'title': 'Дни', 'type': 'label'},
            {
                'field': 'average_speed_to_answer',
                'title': 'Среднее время ответа реакции на первое сообщение',
                'round': 1,
            },
        ],
        width=1920,
        height=400,
    )

    graphic.make_preparation()

    dpi = 100
    fig, ax = plt.subplots(figsize=(graphic.width / dpi, graphic.height / dpi), dpi=dpi)
    fig.patch.set_facecolor('#f8f9fa')   # Цвет фона всей фигуры
    ax.set_facecolor('#ffffff')          # Цвет фона графика

    # Шаг 4: Построение линии графика
    dates = [datetime.strptime(label.split()[0], '%d.%m') for label in graphic.labels]

    for ds in graphic.datasets:
        y = ds['data']
        ax.plot(dates, y, color='#e52990', linewidth=2.5, label=ds['name'])  # Основная линия

        # Добавление точек и аннотаций на линии
        for date, value in zip(dates, y):
            if value is not None:
                ax.plot(date, value, marker='o', markersize=6, markerfacecolor='#2c5282',
                        markeredgecolor='white', markeredgewidth=1)
                ax.annotate(f'{value:.1f}', (date, value),
                            textcoords="offset points", xytext=(0, 0),
                            ha='center', fontsize=8, fontweight='bold',
                            bbox=dict(boxstyle="round,pad=0.3", facecolor='#e52990',
                                      edgecolor='none', alpha=0.8),
                            color='white')

    # Шаг 5: Добавление зелёной горизонтальной линии — "цели"
    if graphic.target is not None:
        ax.axhline(y=graphic.target, color='#38a169', linestyle='-', linewidth=2, alpha=0.8)
        ax.text(dates[0], graphic.target + 2, 'План',
                color='white', fontweight='bold', fontsize=8,
                bbox=dict(boxstyle="round,pad=0.3", facecolor='#38a169', edgecolor='none'))

    # Шаг 6: Добавление горизонтальных цветных полос (для визуального разделения уровней)
    all_values = [val for ds in graphic.datasets for val in ds['data'] if val is not None]
    if all_values:
        min_val = 0
        max_val = max(all_values)
        padding = (max_val - min_val) * 0.15
        ax.set_ylim(min_val, max_val + padding)

        band_height = 20
        start_band = int(min_val // band_height) * band_height
        end_band = int(max_val // band_height + 2) * band_height
        for i in range(start_band, end_band, band_height):
            if (i // band_height) % 2 == 1:
                ax.axhspan(i, i + band_height, alpha=0.05, color='gray', zorder=0)



    # Шаг 7: Настройка осей и сетки
    ax.grid(True, linestyle='-', linewidth=0.5, color='#e2e8f0', alpha=0.7, axis='y')
    ax.set_axisbelow(True)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%d.%m'))  # Формат дат на оси X
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=0, fontsize=9)

    # Шаг 8: Подписи осей и стилизация рамок графика
    ax.set_xlabel(graphic.x_axes_name, fontsize=10, color='#4a5568')
    ax.set_ylabel(graphic.y_axes_name, fontsize=10, color='#4a5568')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_color('#e2e8f0')
    ax.spines['bottom'].set_color('#e2e8f0')
    ax.tick_params(colors='#4a5568', labelsize=9)

    # Шаг 9: Добавление заголовка графика
    ax.text(0.5, 1.08, graphic.graph_title,
            horizontalalignment='center',
            transform=ax.transAxes,
            fontsize=11,
            color='#2d3748',
            wrap=True)

    buf = BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    buf.seek(0)
    b64 = base64.b64encode(buf.read()).decode("utf-8")

    plt.close()
    yield Output(
        fig,
        metadata={
            "table_preview": MetadataValue.md(f"![table](data:image/png;base64,{b64})")
        },
    )


@op(out = Out(Figure))
def render_Jivo_speed_answer31_DaysDetailed(data):
    data = remap_project_titles(data)
    table = PivotTable(
        items=data,
        row_field="project",
        date_field="date",
        value_field="average_speed_to_answer",
        show_total_row=False,
        show_total_column=False,
        enable_cell_coloring=True,
        enable_zebra_striping=True,
        gradient_direction="asc",
        threshold="60%",
        font_size=10,
        column_widths={
            "project": 0.3,
        },
        cell_height=0.25,
        column_titles={
            "project": "Период",
            "avg": "Средн.",
            "min": "Мин.",
            "max": "Макс."
        },
        target_resolution=(1920, 1080)
    )
    fig = table.render()
    buf = BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    buf.seek(0)
    b64 = base64.b64encode(buf.read()).decode("utf-8")

    yield Output(
        fig,
        metadata={
            "table_preview": MetadataValue.md(f"![table](data:image/png;base64,{b64})")
        },
    )

@op(out = Out(Figure))
def render_Jivo_speed_answer31_Days_Users_Detailed(data):
    table = PivotTable(
        items=data,
        row_field="title",
        group_field="abbreviation",
        date_field="date",
        value_field="average_speed_to_answer",
        show_total_row=False,
        show_total_column=False,
        enable_cell_coloring=True,
        enable_zebra_striping=True,
        gradient_direction="asc",
        threshold="60%",
        font_size=10,
        column_widths={
            "title": 0.3,
            "abbreviation": 0.1
        },
        cell_width=None,
        cell_height=0.25,
        column_titles={
            "title": "Период",
            "abbreviation": "Группа",
            "avg": "Средн.",
            "min": "Мин.",
            "max": "Макс."
        },
        target_resolution=(1920, 1600)
    )
    fig = table.render()
    buf = BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    buf.seek(0)
    b64 = base64.b64encode(buf.read()).decode("utf-8")

    yield Output(
        fig,
        metadata={
            "table_preview": MetadataValue.md(f"![table](data:image/png;base64,{b64})")
        },
    )


@op(
    description="Merge multiple Matplotlib figures into a timestamped PDF в папке reports/",
    ins={'figs': In(List[Figure])},
    out=Out(str),
)
def merge_to_pdf_op(context, figs: List[Figure]) -> str:
    # 1) Параметры
    output_dir = "reports"
    os.makedirs(output_dir, exist_ok=True)
    dpi = 100
    width_px, height_px = 1920, 1080  # жёстко фиксируем
    width_in, height_in = width_px / dpi, height_px / dpi

    # 2) Имя файла
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"report_{ts}.pdf"
    file_path = os.path.join(output_dir, filename)

    # 3) Сборка PDF
    with PdfPages(file_path) as pdf:
        for fig in figs:
            # 3.1 Жёстко задаём размер холста
            fig.set_size_inches(width_in, height_in)
            # 3.2 Убираем поля вокруг axes
            fig.subplots_adjust(left=0.01, right=0.99, top=0.99, bottom=0.01)
            # 3.3 Сохраняем страницу целиком
            pdf.savefig(fig, dpi=dpi, bbox_inches='tight', pad_inches=0)
            fig.clf()

    # 4) Лог
    context.log.info(f"PDF-отчёт сохранён локально: {file_path}")
    return file_path


def add_section_page(pdf, fig, *headers):
    """
    Добавляет страницу с одним или несколькими заголовками.
    Заголовки передаются либо строками ("2.1", "Описание"), либо словарями:
    {'number': '2.1.1', 'title': 'Все проекты', 'color': '#cccccc', 'bg': '#dddddd'}
    """
    processed_headers = []
    for h in headers:
        if isinstance(h, dict):
            processed_headers.append(h)
        elif isinstance(h, str):
            idx = headers.index(h)
            processed_headers.append({
                'number': h,
                'title': headers[idx + 1] if idx + 1 < len(headers) else '',
                'color': '#e6007e',
                'bg': '#111111',
                'text_color': 'white'
            })
            break  # так как пара уже обработана
    pdf._draw_section_header(fig, processed_headers)
    pdf.add_figure(fig)

@op
def create_pdf_op(
    context,
    fig1, fig2, fig3, fig4, fig5,
    fig6, fig7, fig8, fig9, fig10,
    fig11, fig12, fig13, fig14, fig15,
    fig16, fig17, fig18, fig19, fig20,
    fig21, figs
):
    pdf = PDFReport2(
        output_dir="reports",
        prefix="daily_text"
    )

    # --- Титульная страница ---
    pdf.set_basic_header(
        f"ЕЖЕДНЕВНАЯ СТАТИСТИКА ПО ТЕКСТОВЫМ КАНАЛАМ НА {pdf.formatted_report_date}.",
        "nod",
        120,
        250
    )
    pdf.add_block_header("Статистика")
    pdf.add_module_header("Проект: SAIMA")
    pdf.add_text("Дата генерации: " + pdf.formatted_report_date)
    pdf.add_text_page()

    # --- Тема документа ---
    pdf.subject = f"{pdf.formatted_report_date} | ТЕКСТОВЫЕ | ЕЖЕДНЕВНАЯ СТАТИСТИКА ИСС"

    # --- Раздел 1 ---
    add_section_page(pdf, fig1,
        {'number': '1', 'title': 'Сводка по основным показателям', 'color': '#e6007e', 'bg': '#111111'},
        {'number': '1.1', 'title': 'Динамика за последние 7 дней и предварительные данные за месяц по всем проектам.', 'bg': '#333333'}
    )
    add_section_page(pdf, fig2, "1.2", "Динамика за последние 7 дней и данные за месяц Saima.")
    add_section_page(pdf, fig3, "1.3", "Динамика за 7 дней и данные за месяц (Основная линия, Интернет магазин).")
    add_section_page(pdf, fig4, "1.4", "Динамика за 7 дней и данные за месяц (Акча Булак, О!Банк и др.)")
    add_section_page(pdf, fig5, "1.5", "Динамика по часам за отчетный день")

    # --- Раздел 2.1 ---
    add_section_page(pdf, fig6,
        {'number': '2', 'title': 'Статистика по мессенджерам', 'color': '#e6007e', 'bg': '#111111'},
        {'number': '2.1', 'title': 'Количество обращений', 'color': '#cccccc', 'bg': '#333333'},
        {'number': '2.1.1', 'title': 'Все проекты', 'color': '#cccccc', 'bg': '#dddddd', 'text_color': '#000000'}
    )
    add_section_page(pdf, fig7, "2.1.2", "Saima")
    add_section_page(pdf, fig8, "2.1.3", "Основная линия, Интернет магазин")
    add_section_page(pdf, fig9, "2.1.4", "Акча Булак, О!Банк, О!Агент, O!Терминалы, O!Bank")
    add_section_page(pdf, fig10, "2.1.5", "Сводка по проектам")
    add_section_page(pdf, fig11, "2.1.6", "Сводка по специалистам")
    add_section_page(pdf, fig12, "2.1.7", "Сводка по специалистам по сменам")

    # --- Раздел 2.2 ---
    add_section_page(pdf, fig13, "2.2.1", "Average Reaction Time / Среднее по всем проектам")
    add_section_page(pdf, fig14,
        {'number': '2.2', 'title': 'Показатель Average Reaction Time. План ≤ 60 сек.', 'color': '#e6007e', 'bg': '#111111'},
        {'number': '2.2.2', 'title': 'Среднее по всем проектам', 'color': '#cccccc', 'bg': '#dddddd'}
    )
    add_section_page(pdf, fig15, "2.2.3", "Average Reaction Time / Основная линия, Интернет магазин")
    add_section_page(pdf, fig16, "2.2.4", "Average Reaction Time / Акча Булак, Банк и др.")
    add_section_page(pdf, fig17, "2.2.5", "Сводка по проектам")
    add_section_page(pdf, fig18, "2.2.6", "Сводка по специалистам")

    # --- Раздел 2.3 ---
    add_section_page(pdf, fig19, "2.3.1", "Average Speed To Answer / Среднее по всем проектам")
    add_section_page(pdf, fig20, "2.3.2", "Average Speed To Answer / Среднее по Saima")
    add_section_page(pdf, fig21, "2.3.3", "Average Speed To Answer / Основная линия, Интернет магазин")

    for fig in figs:
        add_section_page(pdf, fig, "3" "Статусы")

    # --- Завершение ---
    pdf.close()




@op(
)
def save_report_locally(context, fig: Figure):
    """
    Сохраняет переданную matplotlib Figure в локальную папку reports/
    в формате PDF с таймстампом.
    """
    output_dir = "reports"
    os.makedirs(output_dir, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"report_{ts}.pdf"
    file_path = os.path.join(output_dir, filename)

    fig.savefig(file_path, format="pdf", bbox_inches="tight")
    context.log.info(f"PDF-отчёт сохранён локально: {file_path}")
