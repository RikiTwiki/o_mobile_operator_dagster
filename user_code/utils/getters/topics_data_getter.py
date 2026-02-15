import re
from pathlib import Path

import pandas as pd

from utils.path import BASE_SQL_PATH


class TopicsDataGetter:
    def __init__(self,
         conn,
        start_date = None,
        end_date = None,
        trunc = None,
        date_format = None,
        ):

        self.start_date = start_date
        self.end_date = end_date
        self.conn = conn
        self.trunc = trunc
        self.date_format = date_format

        self.base_sql_path = Path(BASE_SQL_PATH)

    def get_main_level_data(self,  view_id, sources = (1, 2), callback=None):
        sql_path = self.base_sql_path / 'bpm' / 'topic_selects_main_level.sql'
        query = sql_path.read_text(encoding='utf-8')

        params = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "trunc": self.trunc,
            "date_format": self.date_format,
            "view_id": view_id,
            "sources": list(sources),
        }

        df = pd.read_sql_query(query, self.conn, params=params)
        if callback is not None:
            df = callback(df.copy())
        return df.to_dict('records')

    def get_general_group_data(self, view_id: int, general_group: str, sources = (1, 2)) -> list[dict]:
        sql_path = self.base_sql_path / "bpm" / "topic_selects_general_group.sql"
        query = sql_path.read_text(encoding="utf-8")
        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "view_id": view_id,
            "general_group": general_group,
            "sources": list(sources),
        }
        df = pd.read_sql_query(query, self.conn, params=params)
        return df.to_dict("records")

    def get_auxiliary_group_data(self, view_id, general_group,
                                 auxiliary_group, sources = (1, 2)):
        sql_path = self.base_sql_path / "bpm" / "topic_selects_auxiliary_group.sql"
        query = sql_path.read_text(encoding="utf-8")
        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "view_id": view_id,
            "general_group": general_group,
            "auxiliary_group": auxiliary_group,
            "sources": list(sources),
        }
        df = pd.read_sql_query(query, self.conn, params=params)
        return df.to_dict("records")

    def get_themes_data(self, view_id, sources = (1, 2),
                        callback = None):
        sql_path = self.base_sql_path / "bpm" / "topic_selects_themes.sql"
        query = sql_path.read_text(encoding="utf-8")
        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "view_id": view_id,
            "sources": list(sources),
        }
        df = pd.read_sql_query(query, self.conn, params=params)
        if callback is not None:
            df = callback(df.copy())
        return df.to_dict("records")

    def get_themes_data_by_ids(
            self,
            view_ids,
            sources = (1, 2),
            callback = None,
    ):
        sql_path = self.base_sql_path / "bpm" / "topic_selects_themes_by_ids.sql"
        query = sql_path.read_text(encoding="utf-8")
        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "view_ids": list(view_ids),
            "sources": list(sources),
        }
        df = pd.read_sql_query(query, self.conn, params=params)
        if callback is not None:
            df = callback(df.copy())
        return df.to_dict("records")

    def get_jivo_main_level_data(self, date_field: str = "date") -> list[dict]:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", date_field):
            raise ValueError(f"Invalid date_field alias: {date_field!r}")

        sql_path = self.base_sql_path / "bpm" / "topic_selects_jivo_main_level.sql"
        sql_template = sql_path.read_text(encoding="utf-8")
        query = sql_template.format(date_field=date_field)

        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
        }
        df = pd.read_sql_query(query, self.conn, params=params)
        return df.to_dict("records")

    def get_jivo_general_group_data(self, general_group: str, date_field: str = "date") -> list[dict]:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", date_field):
            raise ValueError(f"Invalid date_field alias: {date_field!r}")

        sql_path = self.base_sql_path / "bpm" / "topic_selects_jivo_general_group.sql"
        sql_tmpl = sql_path.read_text(encoding="utf-8")
        query = sql_tmpl.format(date_field=date_field)

        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "general_group": general_group,
        }
        df = pd.read_sql_query(query, self.conn, params=params)
        return df.to_dict("records")

    def get_jivo_themes_data(self) -> list[dict]:
        sql_path = self.base_sql_path / "bpm" / "topic_selects_jivo_themes.sql"
        query = sql_path.read_text(encoding="utf-8")
        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
        }
        df = pd.read_sql_query(query, self.conn, params=params)
        return df.to_dict("records")

    def get_jivo_themes_data_with_trunc(self, date_field: str = "date") -> list[dict]:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", date_field):
            raise ValueError(f"Invalid date_field alias: {date_field!r}")

        sql_path = self.base_sql_path / "bpm" / "topic_selects_jivo_themes_with_trunc.sql"
        sql_tmpl = sql_path.read_text(encoding="utf-8")
        query = sql_tmpl.format(date_field=date_field)

        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
        }
        df = pd.read_sql_query(query, self.conn, params=params)
        return df.to_dict("records")

    def get_data_with_bad_marks(self, view_id: int, sources = (1, 2)):
        sql_path = self.base_sql_path / "bpm" / "topic_selects_bad_marks.sql"
        query = sql_path.read_text(encoding="utf-8")
        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "view_id": view_id,
            "sources": list(sources),
        }
        df = pd.read_sql_query(query, self.conn, params=params)
        return df.to_dict("records")

    def get_data_without_trunc(self, view_id: int, sources = (1, 2)):
        sql_path = self.base_sql_path / "bpm" / "topic_selects_without_trunc.sql"
        query = sql_path.read_text(encoding="utf-8")
        params = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "view_id": view_id,
            "sources": list(sources),
        }
        df = pd.read_sql_query(query, self.conn, params=params)
        return df.to_dict("records")

    def set_hours_trunked_data(self) -> None:
        self.trunc = 'hour'
        self.date_format = "YYYY-MM-DD HH24:MI:SS"

    def set_days_trunked_data(self) -> None:
        self.trunc = 'day'
        self.date_format = "YYYY-MM-DD"

    def set_months_trunked_data(self) -> None:
        self.trunc = 'month'
        self.date_format = "YYYY-MM-DD"



