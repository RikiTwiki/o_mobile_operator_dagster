from pathlib import Path

from dagster import asset, AssetIn, get_dagster_logger
from typing import Dict, List
from utils.report_date_utils import ReportDateUtils
from utils.path import BASE_SQL_PATH
import pandas as pd
import os
from dagster import  MaterializeResult, MetadataValue, TableSchema, TableColumn, TableRecord


@asset(
    required_resource_keys={"mp_daily_config", "report_utils"},
    description="Параметры для почасовой агрегации",
)
def hourly_params(context) -> Dict:
    """
    Формирование SQL-параметров для почасовой агрегации
    """
    dates = context.resources.report_utils.get_utils()
    projects = context.resources.mp_daily_config.get_project_ids()
    positions = context.resources.mp_daily_config.get_position_ids()

    return {
        "trunc": "hour",
        "date_format": "HH24:MI:SS",
        "start_date": dates.StartDate,
        "end_date": dates.EndDate,
        "project_ids": projects,
        "timezone_adjustment_date": "2024-08-21",
        "position_ids": positions,
    }

@asset(
    required_resource_keys={"mp_daily_config", "report_utils"},
    description="Параметры для дневной агрегации",
)
def daily_params(context) -> Dict:
    """
    Формирование SQL-параметров для дневной агрегации
    """
    dates = context.resources.report_utils.get_utils()
    projects = context.resources.mp_daily_config.get_project_ids()
    positions = context.resources.mp_daily_config.get_position_ids()


    return {
        "trunc": "day",
        "date_format": "YYYY-MM-DD HH24:MI:SS",
        "start_date": dates.StartDate,
        "end_date": dates.EndDate,
        "project_ids": projects,
        "position_ids": positions,
        "timezone_adjustment_date": "2024-08-21",

    }

@asset(
    required_resource_keys={"mp_daily_config", "report_utils"},
    description="Параметры для месячной агрегации",
)
def monthly_params(context) -> Dict:
    """
    Формирование SQL-параметров для месячной агрегации
    """
    dates = context.resources.report_utils.get_utils()
    projects = context.resources.mp_daily_config.get_project_ids()
    positions = context.resources.mp_daily_config.get_position_ids()

    return {
        "trunc": "month",
        "date_format": "YYYY-MM-DD",
        "start_date": dates.MonthStart,
        "end_date": dates.MonthEnd,
        "project_ids": projects,
        "position_ids" : positions,
        "timezone_adjustment_date": "2024-08-21",

    }

@asset(
    required_resource_keys={"source_dwh"},
    ins={"hourly_params": AssetIn()},
    description="Подробные данные по сообщениям в разрезе часов",
)
def mp_data_messengers_detailed_by_hour(context, hourly_params: Dict):
    """
    Аналог Php кода hourMPDataMessengersDetailed
    """
    sql_path = os.path.join(BASE_SQL_PATH, "mp", "get_full_aggregated.sql")
    with open(sql_path, "r", encoding="utf-8") as f:
        sql = f.read()

    with context.resources.source_dwh as conn:
        df = pd.read_sql_query(sql, conn, params=hourly_params)

    context.log.info(f'return: {df.to_dict(orient="records")}')


    return df.to_dict(orient="records")

@asset(
    required_resource_keys={"source_dwh"},
    ins={"hourly_params": AssetIn()},
    description="Подробные данные по сообщениям в разрезе часов",
)
def mp_data_messengers_enteredchats_by_hour(context, hourly_params):
    """
    Аналог Php кода hourDataMessengersEnteredChats
    """
    sql_path = os.path.join(BASE_SQL_PATH, "mp", "get_entered_chats.sql")
    with open(sql_path, "r", encoding="utf-8") as f:
        sql = f.read()

    with context.resources.source_dwh as conn:
        df = pd.read_sql_query(sql, conn, params=hourly_params)

    return df.to_dict(orient="records")

@asset(
    ins={
        "mp_data_messengers_detailed_by_hour": AssetIn(),
        "mp_data_messengers_enteredchats_by_hour": AssetIn(),
    },
    description="Левый join часовой агрегации и входящих чатов по полю `date`",
)
def hour_MPData_Messengers_Detailed(
    context,
    mp_data_messengers_detailed_by_hour: List[Dict],
    mp_data_messengers_enteredchats_by_hour: List[Dict]
):
    if not mp_data_messengers_detailed_by_hour:
        return []
    df_det = pd.DataFrame(mp_data_messengers_detailed_by_hour)
    df_ent = pd.DataFrame(mp_data_messengers_enteredchats_by_hour)
    df_merged = df_det.merge(df_ent, on="date", how="left")
    numeric_cols = set(df_ent.columns) - {"date"}
    for col in numeric_cols:
        df_merged[col] = df_merged[col].fillna(0)
    context.log.info(f'return: {df_merged.to_dict(orient="records")}')
    return df_merged.to_dict(orient="records")

@asset(
    required_resource_keys={"source_dwh"},
    ins={"daily_params": AssetIn()},
    description="Показатель self-service по проектам Saima и Bank",
)
def all_service_rate_by_day(context, daily_params: Dict):
    """
    Аналог Php кода allSelfServiceRate: загружает данные по self-service rate для проектов 1, 8 и 9
    """
    logger = get_dagster_logger()
    params = daily_params.copy()
    params['project_ids'] = [1,4,5,8,9,10,11,12,15,19,20,21]
    sql_path = Path(BASE_SQL_PATH) / "mp" / "self_service_rate.sql"
    sql_template = sql_path.read_text(encoding="utf-8")
    query = sql_template.format(
        date_field="date"
    )
    with context.resources.source_dwh as conn:
        df = pd.read_sql_query(query, conn, params=params)

    logger.info(f"all_service_rate: {df.to_dict(orient='records')}")
    return df.to_dict(orient="records")


@asset(
    required_resource_keys={"source_dwh"},
    ins={"daily_params": AssetIn()},
    description="Показатель self-service по проектам Saima и Bank",
)
def saima_service_rate_by_day(context, daily_params: Dict):
    """
    Аналог Php кода allSelfServiceRate: загружает данные по self-service rate для проектов 1
    """
    params = daily_params.copy()
    params['project_ids'] = [1]  # По Saima
    sql_path = os.path.join(BASE_SQL_PATH, "mp", "self_service_rate.sql")
    with open(sql_path, "r", encoding="utf-8") as f:
        sql = f.read()
    with context.resources.source_dwh as conn:
        df = pd.read_sql_query(sql, conn, params=params)

    context.log.info(f'return: {df.to_dict(orient="records")}')
    return df.to_dict(orient="records")

@asset(
    required_resource_keys={"source_dwh"},
    ins={"daily_params": AssetIn()},
    description="Показатель self-service по проектам Bank",
)
def bank_service_rate_by_day(context, daily_params: Dict):
    """
    Аналог Php кода allSelfServiceRate: загружает данные по self-service rate для проектов 8 и 9
    """
    params = daily_params.copy()
    params['project_ids'] = [8,9,10,15,20]  # По Bank
    sql_path = os.path.join(BASE_SQL_PATH, "mp", "self_service_rate.sql")
    with open(sql_path, "r", encoding="utf-8") as f:
        sql = f.read()
    with context.resources.source_dwh as conn:
        df = pd.read_sql_query(sql, conn, params=params)

    context.log.info(f'return: {df.to_dict(orient="records")}')

    return df.to_dict(orient="records")

@asset(
    required_resource_keys={"source_dwh"},
    ins={"monthly_params": AssetIn()},
    description="Показатель месяцы self-service по проектам Bank",
)
def service_rate_all_by_month(context, monthly_params: Dict):
    """
    Аналог Php кода allSelfServiceRate: загружает данные по self-service rate для проектов 1
    """
    logger = get_dagster_logger()

    params = monthly_params.copy()
    params['project_ids'] = [1,4,5,8,9,10,11,12,15,20]
    sql_path = Path(BASE_SQL_PATH) / "mp" / "self_service_rate.sql"
    sql_template = sql_path.read_text(encoding="utf-8")
    query = sql_template.format(
        date_field="date"
    )
    with context.resources.source_dwh as conn:
        df = pd.read_sql_query(query, conn, params=params)

    logger.info(f"service_rate_by_month: {df.to_dict(orient='records')}")
    return df.to_dict(orient="records")

@asset(
    required_resource_keys={"source_dwh"},
    ins={"monthly_params": AssetIn()},
    description="Показатель месяцы self-service по проектам Saima",
)
def service_rate_saima_by_month(context, monthly_params: Dict):
    """
    Аналог Php кода allSelfServiceRate: загружает данные по self-service rate для проектов 1
    """
    params = monthly_params.copy()
    params['project_ids'] = [1]  # По Saima
    sql_path = os.path.join(BASE_SQL_PATH, "mp", "self_service_rate.sql")
    with open(sql_path, "r", encoding="utf-8") as f:
        sql = f.read()
    with context.resources.source_dwh as conn:
        df = pd.read_sql_query(sql, conn, params=params)
    context.log.info(f'return: {df.to_dict(orient="records")}')
    return df.to_dict(orient="records")

@asset(
    required_resource_keys={"source_dwh"},
    ins={"monthly_params": AssetIn()},
    description="Показатель месяцы self-service по проектам Bank",
)
def service_rate_bank_by_month(context, monthly_params: Dict):
    """
    Аналог Php кода allSelfServiceRate: загружает данные по self-service rate для проектов 8, 9
    """
    params = monthly_params.copy()
    params['project_ids'] = [8,9,10,15,20]
    sql_path = os.path.join(BASE_SQL_PATH, "mp", "self_service_rate.sql")
    with open(sql_path, "r", encoding="utf-8") as f:
        sql = f.read()
    with context.resources.source_dwh as conn:
        df = pd.read_sql_query(sql, conn, params=params)
    context.log.info(f'return: {df.to_dict(orient="records")}')

    return df.to_dict(orient="records")

@asset(
    required_resource_keys={"source_dwh"},
    ins={"daily_params": AssetIn()},
    description="Агрегированные по дням данные (ALL)",
)
def aggregated_by_day(context, daily_params: Dict):

    SQL_AGG = os.path.join(BASE_SQL_PATH, "mp", "get_aggregated.sql")
    with open(SQL_AGG, "r", encoding="utf-8") as f:
        sql = f.read()
    with context.resources.source_dwh as conn:
        df = pd.read_sql_query(sql, conn, params=daily_params)
    context.log.info(f'return: {df.to_dict(orient="records")}')

    return df.to_dict(orient="records")

@asset(
    required_resource_keys={"source_dwh"},
    ins={"daily_params": AssetIn()},
    description="Агрегированные по дням данные",
)
def entered_by_day(context, daily_params: Dict):

    SQL_ENTERED = os.path.join(BASE_SQL_PATH, "mp", "get_entered_chats.sql")
    with open(SQL_ENTERED, "r", encoding="utf-8") as f:
        sql = f.read()
    with context.resources.source_dwh as conn:
        df = pd.read_sql_query(sql, conn, params=daily_params)

    context.log.info(f'return: {df.to_dict(orient="records")}')

    return df.to_dict(orient="records")

@asset(
    required_resource_keys={"source_dwh"},
    ins={"daily_params": AssetIn()},
    description="Агрегированные по дням данные",
)
def all_handled_by_day(context, daily_params: Dict):

    SQL_ALL_HANDLED = os.path.join(BASE_SQL_PATH, "mp", "get_all_aggregated.sql")
    with open(SQL_ALL_HANDLED, "r", encoding="utf-8") as f:
        sql = f.read()

    context.log.info(f"all_handled_by_day: daily_params = {daily_params}")

    with context.resources.source_dwh as conn:
        df = pd.read_sql_query(sql, conn, params=daily_params)

    context.log.info(f'return: {df.to_dict(orient="records")}')

    return df.to_dict(orient="records")

@asset(
    required_resource_keys={"source_dwh"},
    ins={"daily_params": AssetIn()},
    description="Агрегированные по дням данные",
)
def bot_handled_by_day(context, daily_params: Dict):
    SQL_BOT_HANDLED = os.path.join(BASE_SQL_PATH, "mp", "bot_handled_chats.sql")
    with open(SQL_BOT_HANDLED, "r", encoding="utf-8") as f:
        sql = f.read()
    context.log.info(f"bot_handled_by_day: daily_params = {daily_params}")
    with context.resources.source_dwh as conn:
        df = pd.read_sql_query(sql, conn, params=daily_params)
    context.log.info(f"bot_handled_by_day: rows={len(df)}, columns={df.columns.tolist()}")
    context.log.info(f'return: {df.to_dict(orient="records")}')

    return df.to_dict(orient="records")

@asset(
    required_resource_keys={"source_dwh"},
    ins={"monthly_params": AssetIn()},
    description="Агрегированные за месяц",
)
def aggregated_by_month(context, monthly_params: Dict):

    SQL_AGG = os.path.join(BASE_SQL_PATH, "mp", "get_aggregated.sql")
    with open(SQL_AGG, "r", encoding="utf-8") as f:
        sql = f.read()
    with context.resources.source_dwh as conn:
        df = pd.read_sql_query(sql, conn, params=monthly_params)
    context.log.info(f'return: {df.to_dict(orient="records")}')

    return df.to_dict(orient="records")