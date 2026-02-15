import os
import pandas as pd
from dagster import asset, OpExecutionContext, AssetIn
from typing import List
from utils.path import BASE_SQL_PATH
from datetime import datetime, timedelta

@asset(
    required_resource_keys={"source_dwh"},
    ins={
        "get_last_aggregation_date": AssetIn("get_last_aggregation_date")
    }
)
def load_raw_mp_data(
    context: OpExecutionContext,
    get_last_aggregation_date: str
) -> list[dict]:
    """
    Dagster-asset для загрузки сырых данных MP handling time
    """
    conn = context.resources.source_dwh

    # Формируем период: [start_date, end_date)
    start_date = datetime.strptime(get_last_aggregation_date, "%Y-%m-%d").date()
    end_date = start_date + timedelta(days=1)

    sql_path = os.path.join(BASE_SQL_PATH, "aggregate", "aggregate_mp.sql")

    try:
        with open(sql_path, "r", encoding="utf-8") as f:
            sql_template = f.read()

        project_ids: List[int] = [1]
        project_ids_str = ','.join(map(str, project_ids))
        sql = sql_template.replace('{project_ids_str}', project_ids_str)

        df = pd.read_sql_query(
            sql, conn,
            params={"start_date": start_date, "end_date": end_date}
        )

        context.log.info(f"Загружено {len(df)} строк из aggregate_mp за {start_date}")
        return df.to_dict(orient="records")

    except Exception as e:
        context.log.error(f"Ошибка загрузки aggregate_mp: {e}")
        raise
