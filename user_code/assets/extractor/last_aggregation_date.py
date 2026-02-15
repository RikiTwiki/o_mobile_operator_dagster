import os
from datetime import datetime

from dagster import asset, OpExecutionContext
from utils.load_sql import load_sql_df
from utils.path import BASE_SQL_PATH


@asset(required_resource_keys={"source_dwh"})
def get_last_aggregation_date(context: OpExecutionContext) -> str:
    conn = context.resources.source_dwh
    sql_path = os.path.join(BASE_SQL_PATH, "stat", "get_last_aggregation_date.sql")

    df = load_sql_df(context, conn, sql_path, log_prefix="[get_last_aggregation_date] ")

    if df.empty:
        first_day_of_month = datetime.today().replace(day=1).strftime("%Y-%m-%d")
        print("Первый день текущего месяца:", first_day_of_month)
        return first_day_of_month

    return df.iloc[0, 0]
