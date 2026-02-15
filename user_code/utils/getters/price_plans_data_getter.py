from pathlib import Path

import pandas as pd

from utils.path import BASE_SQL_PATH


class PricePlansDataGetter:
    def __init__(self,
                 start_date,
                 end_date,
                 bpm_conn,
                 trunc = None
                 ):

        self.start_date = start_date
        self.end_date = end_date
        self.trunc = trunc
        self.base_sql_path = Path(BASE_SQL_PATH)
        self.bpm_conn = bpm_conn


        if self.trunc == "day":
            self.date_format = "YYYY-MM-DD HH24:MI:SS"
        elif self.trunc == "hour":
            self.date_format = "YYYY-MM-DD HH24:MI:SS"
        elif self.trunc == "month":
            self.date_format = "YYYY-MM-DD"
        else:
            raise ValueError("trunc must be 'hour' | 'day' | 'month'")

    def get_data_with_bad_marks(self):

        sql_path = self.base_sql_path / "stat" / "data_with_bad_marks.sql"
        query = sql_path.read_text(encoding="utf-8")

        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
        }

        df = pd.read_sql_query(query, self.bpm_conn, params=params)
        return df.to_dict("records")

    def set_hours_trunked_data(self):
        """
        Настройка агрегации по часам.
        """
        self.trunc = "hour"
        self.date_format = "YYYY-MM-DD HH24:MI:SS"

    def set_days_trunked_data(self):
        """
        Настройка агрегации по дням.
        """
        self.trunc = "day"
        self.date_format = "YYYY-MM-DD HH24:MI:SS"

    def set_months_trunked_data(self):
        """
        Настройка агрегации по месяцам.
        """
        self.trunc = "month"
        self.date_format = "YYYY-MM-DD"