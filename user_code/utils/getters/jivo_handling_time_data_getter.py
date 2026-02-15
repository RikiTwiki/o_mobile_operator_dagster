import datetime as dt
from pathlib import Path
from typing import List
from datetime import datetime, timedelta

import pandas as pd
from dateutil.relativedelta import relativedelta

from utils.path import BASE_SQL_PATH


class JivoHandlingTimeDataGetter:
    def __init__(self,
                 start_date,
                 end_date,
                 conn,
                 trunc = 'day',
                 date_format = 'YYYY-MM-DD HH24:MI:SS',
                 ):
        self.start_date = start_date
        self.end_date = end_date
        self.conn = conn
        self.trunc = trunc
        self.date_format = date_format
        self.base_sql_path = Path(BASE_SQL_PATH)


        self.period = []
        self._rebuild_period()

    def _rebuild_period(self):
        """Строит self.period так, чтобы строки 1-в-1 совпадали с to_char(date_trunc(trunc, hour), date_format)."""
        start = dt.datetime.strptime(self.start_date, "%Y-%m-%d %H:%M:%S")
        end = dt.datetime.strptime(self.end_date,   "%Y-%m-%d %H:%M:%S")

        if self.trunc == "hour":
            keys: List[str] = []
            cur = start
            while cur < end:
                keys.append(cur.strftime("%Y-%m-%d %H:%M:%S"))
                cur += dt.timedelta(hours=1)
            self.period = keys

        elif self.trunc == "day":
            keys = []
            cur = start
            while cur < end:
                keys.append(cur.strftime("%Y-%m-%d 00:00:00"))
                cur += dt.timedelta(days=1)
            self.period = keys

        else:
            keys = []
            cur = dt.datetime(start.year, start.month, 1)
            while cur < end:
                keys.append(cur.strftime("%Y-%m-%d"))
                y, m = (cur.year + 1, 1) if cur.month == 12 else (cur.year, cur.month + 1)
                cur = dt.datetime(y, m, 1)
            self.period = keys


    def get_detailed(
            self,
            start_date,
            end_date,
            responsible = None,
            type_ = None,
            widgets = None,
            groups = None,
            date_field: str = "date",
    ) -> list[dict]:
        """
        Аналог PHP getDetailed.
        Возвращает список словарей с метриками по Jivo-проектам.
        """

        sql_path = self.base_sql_path / "stat" / "jivo_detailed.sql"
        sql_template = sql_path.read_text(encoding="utf-8")

        # Фильтры (подставляем только если списки не пустые)
        responsible_filter = ""
        type_filter = ""
        widgets_filter = ""
        groups_filter = ""

        if responsible:
            responsible_filter = "AND jp.responsible = ANY(%(responsible)s::text[])"
        if type_:
            type_filter = "AND jp.type = ANY(%(type)s::text[])"
        if widgets:
            widgets_filter = "AND jp.widget_id = ANY(%(widgets)s::int[])"
        if groups:
            groups_filter = "AND jp.group = ANY(%(groups)s::text[])"

        query = sql_template.format(
            date_field=date_field,
            responsible_filter=("  " + responsible_filter if responsible_filter else ""),
            type_filter=("  " + type_filter if type_filter else ""),
            widgets_filter=("  " + widgets_filter if widgets_filter else ""),
            groups_filter=("  " + groups_filter if groups_filter else ""),
        )

        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": start_date,
            "end_date": end_date,
        }
        if responsible:
            params["responsible"] = responsible
        if type_:
            params["type"] = type_
        if widgets:
            params["widgets"] = widgets
        if groups:
            params["groups"] = groups

        df = pd.read_sql_query(query, self.conn, params=params)
        return df.to_dict("records")

    def get_bot_dialogs_by_projects(
            self,
            start_date,
            end_date,
            responsible = None,
            type_ = None,
            widgets = None,
            groups = None,
            date_field = "date",
    ):
        """
        Возвращает список словарей с количеством бот-диалогов по проектам.
        """

        sql_path = self.base_sql_path / "stat" / "jivo_bot_dialogs_by_projects.sql"
        sql_template = sql_path.read_text(encoding="utf-8")

        responsible_filter = ""
        type_filter = ""
        widgets_filter = ""
        groups_filter = ""

        if responsible:
            responsible_filter = "AND jp.responsible = ANY(%(responsible)s::text[])"
        if type_:
            type_filter = "AND jp.type = ANY(%(type)s::text[])"
        if widgets:
            widgets_filter = "AND jp.widget_id = ANY(%(widgets)s::int[])"
        if groups:
            groups_filter = "AND jp.group = ANY(%(groups)s::text[])"

        query = sql_template.format(
            date_field=date_field,
            responsible_filter=("  " + responsible_filter if responsible_filter else ""),
            type_filter=("  " + type_filter if type_filter else ""),
            widgets_filter=("  " + widgets_filter if widgets_filter else ""),
            groups_filter=("  " + groups_filter if groups_filter else ""),
        )

        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": start_date,
            "end_date": end_date,
        }
        if responsible:
            params["responsible"] = responsible
        if type_:
            params["type"] = type_
        if widgets:
            params["widgets"] = widgets
        if groups:
            params["groups"] = groups

        df = pd.read_sql_query(query, self.conn, params=params)
        return df.to_dict("records")

    def get_aggregated(
            self,
            start_date: str,
            end_date: str,
            responsible = None,
            type_ = None,
            exclude_dates = None,
            groups = None,
            date_field= "date",
    ):
        """
        Возвращает список словарей с агрегатами по датам.
        """

        sql_path = self.base_sql_path / "stat" / "jivo_aggregated.sql"
        sql_template = sql_path.read_text(encoding="utf-8")

        # собираем опциональные фильтры
        excluded_dates_filter = ""
        responsible_filter = ""
        type_filter = ""
        groups_filter = ""

        if exclude_dates:
            excluded_dates_filter = "AND DATE(ajhtd.chat_finished_at) <> ALL(%(excluded_dates)s::date[])"
        if responsible:
            responsible_filter = "AND jp.responsible = ANY(%(responsible)s::text[])"
        if type_:
            type_filter = "AND jp.type = ANY(%(type)s::text[])"
        if groups:
            groups_filter = "AND jp.group = ANY(%(groups)s::text[])"

        query = sql_template.format(
            date_field=date_field,
            excluded_dates_filter=("  " + excluded_dates_filter if excluded_dates_filter else ""),
            responsible_filter=("  " + responsible_filter if responsible_filter else ""),
            type_filter=("  " + type_filter if type_filter else ""),
            groups_filter=("  " + groups_filter if groups_filter else ""),
        )

        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": start_date,
            "end_date": end_date,
        }
        if exclude_dates:
            params["excluded_dates"] = exclude_dates
        if responsible:
            params["responsible"] = responsible
        if type_:
            params["type"] = type_
        if groups:
            params["groups"] = groups

        df = pd.read_sql_query(query, self.conn, params=params)
        return df.to_dict("records")

    def get_aggregated_main_data(
            self,
            start_date: str,
            end_date: str,
            responsible = None,
            type_ = None,
            exclude_dates = None,
            groups = None,
            date_field: str = "date",
    ):
        """
        Возвращает список словарей с total, bot_only, specialist по датам.
        """

        sql_path = self.base_sql_path / "stat" / "jivo_aggregated_main_data.sql"
        sql_template = sql_path.read_text(encoding="utf-8")

        # опциональные фильтры
        excluded_dates_filter = ""
        responsible_filter = ""
        type_filter = ""
        groups_filter = ""

        if exclude_dates:
            excluded_dates_filter = "AND DATE(ajhtd.chat_finished_at) <> ALL(%(excluded_dates)s::date[])"
        if responsible:
            responsible_filter = "AND jp.responsible = ANY(%(responsible)s::text[])"
        if type_:
            type_filter = "AND jp.type = ANY(%(type)s::text[])"
        if groups:
            groups_filter = "AND jp.group = ANY(%(groups)s::text[])"

        query = sql_template.format(
            date_field=date_field,
            excluded_dates_filter=("  " + excluded_dates_filter if excluded_dates_filter else ""),
            responsible_filter=("  " + responsible_filter if responsible_filter else ""),
            type_filter=("  " + type_filter if type_filter else ""),
            groups_filter=("  " + groups_filter if groups_filter else ""),
        )

        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": start_date,
            "end_date": end_date,
        }
        if exclude_dates:
            params["excluded_dates"] = exclude_dates
        if responsible:
            params["responsible"] = responsible
        if type_:
            params["type"] = type_
        if groups:
            params["groups"] = groups

        df = pd.read_sql_query(query, self.conn, params=params)
        return df.to_dict("records")

    def get_dates_from_range(self,
                             start_date: str,
                             end_date: str,
                             interval: str = "P1M",
                             fmt: str = "%Y-%m-%d %H:%M:%S"
                             ):
        try:
            start = datetime.fromisoformat(start_date)
            end = datetime.fromisoformat(end_date)

            # --- распарсим interval ---
            if interval == "P1D":  # дни
                delta = timedelta(days=1)
            elif interval == "PT1H":  # часы
                delta = timedelta(hours=1)
            elif interval == "P1M":  # месяцы
                delta = relativedelta(months=1)
            else:
                raise ValueError(f"Unsupported interval {interval}")

            # --- собираем список ---
            dates = []
            current = start
            while current < end:
                dates.append(current.strftime(fmt))
                current += delta

            return dates

        except Exception as e:
            import traceback
            print("Exception:", e)
            return traceback.format_exc().splitlines()

    def set_hours_trunked_data(self):
        self.trunc = "hour"
        self.date_format = "YYYY-MM-DD HH24:MI:SS"

    def add_period(self):
        self.trunc = "hour"
        self.period = self.get_dates_from_range(
            self.start_date,
            self.end_date,
            interval="PT1H",
            fmt="%Y-%m-%d %H:%M:%S"
        )
        self.date_format = "YYYY-MM-DD HH24:MI:SS"

    def set_days_trunked_data(self):
        self.trunc = "day"
        self.date_format = "YYYY-MM-DD"

    def set_months_trunked_data(self):
        self.trunc = "month"
        self.date_format = "YYYY-MM-DD"


