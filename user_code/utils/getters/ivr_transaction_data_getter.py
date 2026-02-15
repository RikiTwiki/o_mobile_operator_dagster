from pathlib import Path

import pandas as pd

from utils.path import BASE_SQL_PATH


class IVRTransactionDataGetter:
    def __init__(self,
                 start_date,
                 end_date,
                 nod_dinosaur_conn,
                 date_field = None,
                 trunc = None
                 ):


        self.start_date = start_date
        self.end_date = end_date
        self.trunc = trunc
        self.date_field = date_field
        self.base_sql_path = Path(BASE_SQL_PATH)
        self.nod_dinosaur_conn = nod_dinosaur_conn



    def get_level_data(self,
                       callers,
                       parents,
                       execs = None):

        sql_path = self.base_sql_path / "nod_dinosaur" / "level_data.sql"
        sql_template = sql_path.read_text(encoding="utf-8")

        callers_filter = "AND ivr_transitions.called = ANY(%(callers)s::text[])" if callers else ""
        query = sql_template.format(date_field=self.date_field, callers_filter=callers_filter)

        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "parents": parents or [],
            "execs": execs or [],
        }
        if callers:
            params["callers"] = callers

        df = pd.read_sql_query(query, self.nod_dinosaur_conn, params=params)
        return df.to_dict("records")

    def get_other_projects_data(self,
                                callers,
                                execs,
                                parents):
        sql_path = self.base_sql_path / "nod_dinosaur" / "other_projects_data.sql"
        sql_template = sql_path.read_text(encoding="utf-8")

        callers_filter = "AND ivr_transitions.called = ANY(%(callers)s::text[])" if callers else ""
        query = sql_template.format(date_field=self.date_field, callers_filter=callers_filter)

        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "execs": execs or [],
            "parents": parents or [],
        }
        if callers:
            params["callers"] = callers

        df = pd.read_sql_query(query, self.nod_dinosaur_conn, params=params)
        return df.to_dict("records")

    def set_hours_trunked_data(self):
        """
        Настройка агрегации по часам.
        """
        self.trunc = "hour"
        self.date_format = "YYYY-MM-DD HH24:MI:SS"
        self.field_name = 'hour'

    def set_days_trunked_data(self):
        """
        Настройка агрегации по дням.
        """
        self.trunc = "day"
        self.date_format = "YYYY-MM-DD HH24:MI:SS"
        self.date_field = 'date'

    def set_months_trunked_data(self):
        """
        Настройка агрегации по месяцам.
        """
        self.trunc = "month"
        self.date_format = "YYYY-MM-DD"
        self.date_field = 'month'