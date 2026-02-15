import os
import pandas as pd

from dagster import asset, OpExecutionContext
from utils.load_sql import load_sql_df
from utils.path import BASE_SQL_PATH

@asset(required_resource_keys={"source_dwh"})
def load_users(context: OpExecutionContext) -> pd.DataFrame:
    conn = context.resources.source_dwh

    sql_path = os.path.join(BASE_SQL_PATH, "bpm", "get_users.sql")

    return load_sql_df(context, conn, sql_path, log_prefix="[users] ")

@asset(required_resource_keys={"source_dwh"})
def load_staff_units(context: OpExecutionContext) -> pd.DataFrame:
    conn = context.resources.source_dwh

    sql_path = os.path.join(BASE_SQL_PATH, "bpm", "get_staff_units.sql")

    return load_sql_df(context, conn, sql_path, log_prefix="[staff_units] ")