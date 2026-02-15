#aggregate_mp_handling_time_data_schedule.py
import math

import pandas as pd
from datetime import datetime
from dagster import OpExecutionContext, asset, AssetIn
from typing import List, Dict, Any

@asset(
    required_resource_keys={"source_dwh"},
    ins={
        "users": AssetIn("load_users"),
        "staff_units": AssetIn("load_staff_units"),
        "transform_mp_data": AssetIn("transform_mp_data"),
    }
)
def aggregate_mp_handling_time_asset(
    context: OpExecutionContext,
    users: pd.DataFrame,
    staff_units: pd.DataFrame,
    transform_mp_data: List[Dict[str, Any]]
):
    """Агрегация и сохранение данных MP handling time"""
    dwh = context.resources.source_dwh

    if not transform_mp_data:
        context.log.info("Нет трансформированных данных для агрегации")
        return None

    try:
        first_date = min(chat["chat_finished_at"] for chat in transform_mp_data)
        start_date = datetime.strptime(first_date, "%Y-%m-%d %H:%M:%S").date()
        end_date = start_date + pd.Timedelta(days=1)
    except Exception as e:
        context.log.error(f"Не удалось определить дату агрегации: {e}")
        return None

    users_by_login = {}
    for _, user in users.iterrows():
        users_by_login.setdefault(user["login"], []).append(user["id"])

    enriched_data = []
    now = datetime.now()

    for idx, row_data in enumerate(transform_mp_data):
        user_ids = users_by_login.get(row_data["user_login"], [])
        staff_unit_id = None

        for _, unit in staff_units.iterrows():
            if unit["user_id"] not in user_ids:
                continue

            accepted_at = pd.to_datetime(unit["accepted_at"]).date()
            dismissed_at = pd.to_datetime(unit["dismissed_at"]).date() if pd.notnull(unit["dismissed_at"]) else None

            if (dismissed_at and accepted_at < start_date < dismissed_at) or (not dismissed_at and accepted_at < start_date):
                staff_unit_id = unit["id"]
                break

        enriched_row = row_data.copy()
        enriched_row["staff_unit_id"] = staff_unit_id
        enriched_row["created_at"] = now
        enriched_row["updated_at"] = now
        enriched_row.setdefault("closed_by_timeout", False)

        enriched_data.append(enriched_row)

    if not enriched_data:
        context.log.info("Нет данных после обогащения")
        return end_date.strftime("%Y-%m-%d")

    enriched_df = pd.DataFrame(enriched_data)
    enriched_df = enriched_df.where(pd.notnull(enriched_df), None)
    context.log.info(f"Дебаг: Размер таблицы для вставки: {len(enriched_df)}")

    # --- ДЕБАГ ---
    context.log.info(f"Дебаг: Размер таблицы для вставки: {len(enriched_df)}")
    if len(enriched_df) > 0:
        sample_row = enriched_df.iloc[0].to_dict()
        context.log.info(f"Дебаг: Пример первой строки для вставки: {sample_row}")

        # Выведем типы данных первой строки
        for col, val in sample_row.items():
            context.log.info(f"Дебаг: Тип колонки '{col}' - {type(val)}; Значение: {val}")

    with dwh as conn:
        with conn.cursor() as cursor:

            if 'rate' in enriched_df.columns:
                enriched_df = enriched_df.drop(columns=['rate'])

            columns = list(enriched_df.columns)
            placeholders = ','.join(['%s'] * len(columns))
            insert_query = f"INSERT INTO stat.aggregation_mp_handling_time_data ({','.join(columns)}) VALUES ({placeholders})"

            def clean_value(v):
                if v is None:
                    return None
                if isinstance(v, float) and math.isnan(v):
                    return None
                return v

            values_list = [
                tuple(clean_value(row[col]) for col in columns)
                for _, row in enriched_df.iterrows()
            ]

            # Логируем первые 5 значений для проверки перед вставкой
            for i, val in enumerate(values_list[:5]):
                context.log.info(f"Дебаг: Значения для вставки, строка {i}: {val}")

            try:
                from psycopg2.extras import execute_batch
                execute_batch(cursor, insert_query, values_list, page_size=100)
                conn.commit()
            except Exception as e:
                for i, row in enumerate(values_list):
                    try:
                        cursor.execute(insert_query, row)
                    except Exception as ex:
                        context.log.error(f"Ошибка на строке {i}: {row}")
                        context.log.error(f"Типы: {[type(val) for val in row]}")
                        raise ex

        context.log.info(f"Вставлено записей: {len(enriched_df)}")

    return end_date.strftime("%Y-%m-%d")

