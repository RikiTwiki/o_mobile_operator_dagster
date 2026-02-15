#user_code/assets/extractor/bot_data.py
import os

import pandas as pd
from dagster import asset

from utils.path import BASE_SQL_PATH


@asset(required_resource_keys={"source_dwh", "mp_daily_config", "report_utils"})
def bot_handling_data(context):
    dates = context.resources.report_utils.get_utils()
    config = context.resources.mp_daily_config

    sql_path = os.path.join(BASE_SQL_PATH, "stat", "all_handled_chats_by_positions.sql")

    with open(sql_path, "r", encoding="utf-8") as f:
        sql_template = f.read()

    with context.resources.source_dwh as conn:
        df = pd.read_sql_query(
            sql_template,
            conn,
            params={
                "start_date": dates.StartDate,
                "end_date": dates.EndDate,
                "project_ids": config.get_project_ids(),
                "position_ids": config.get_position_ids(),
                "trunc": "day",
            }
        )

    df_grouped = df.groupby('date', as_index=False).agg({
        'total_handled_chats': 'sum',
    })

    context.log.info(f"Загружено {len(df)} строк из aggregate_mp")
    context.log.info(f"После группировки: {len(df_grouped)} строк")
    context.log.debug(f"Сгруппированные данные: {df_grouped.head()}")

    return df_grouped.to_dict(orient="records")