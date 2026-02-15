import os
import pandas as pd

def load_sql_df(context, conn, sql_path: str, log_prefix: str = "") -> pd.DataFrame:
    try:
        with open(sql_path, "r", encoding="utf-8") as f:
            sql = f.read()

        df = pd.read_sql_query(sql, conn)

        context.log.info(f"{log_prefix}Loaded {len(df)} rows from {os.path.basename(sql_path)}")
        return df

    except Exception as e:
        context.log.error(f"{log_prefix}Error loading from {sql_path}: {e}")
        raise


