from pathlib import Path

import pandas as pd

from utils.path import BASE_SQL_PATH


class TechnicalWorksDataGetter:
    def __init__(self,
                 start_date,
                 end_date,
                 bpm_conn):
        self.start_date = start_date
        self.end_date = end_date
        self.bpm_conn = bpm_conn
        self.base_sql_path = Path(BASE_SQL_PATH)

    def get_actual_technical_works_data(self) -> list[dict]:
        sql_path = self.base_sql_path / "bpm" / "actual_technical_works_data.sql"
        query = sql_path.read_text(encoding="utf-8")

        params = {
            "start_date": self.start_date,
            "end_date": self.end_date,
        }

        df = pd.read_sql_query(query, self.bpm_conn, params=params)
        return df.to_dict("records")

    def get_actual_technical_works_data_by_services(self, services=None):
        if not services:
            return []

        sql_path = self.base_sql_path / "bpm" / "actual_technical_works_data_by_services.sql"
        query = sql_path.read_text(encoding="utf-8")

        params = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "services": services,
        }

        df = pd.read_sql_query(query, self.bpm_conn, params=params)
        return df.to_dict("records")

    def get_actual_technical_works_data_by_groups(self, groups: list[int] | None = None):

        if not groups:
            return []

        sql_path = self.base_sql_path / "bpm" / "actual_technical_works_data_by_groups.sql"
        query = sql_path.read_text(encoding="utf-8")

        params = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "groups": groups,
        }

        df = pd.read_sql_query(query, self.bpm_conn, params=params)
        return df.to_dict("records")
