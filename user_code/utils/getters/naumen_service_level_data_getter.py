from collections import defaultdict
from datetime import datetime, timedelta, date, time
from pathlib import Path
from typing import Dict, Optional, List, Any
import datetime as dt

import pandas as pd
from dagster import get_dagster_logger
from utils.path import BASE_SQL_PATH

import psycopg2
from utils.getters.naumen_projects_getter import NaumenProjectsGetter

from dateutil.relativedelta import relativedelta

import re

from utils.transformers.mp_report import try_parse_period


class NaumenServiceLevelDataGetter(NaumenProjectsGetter):
    def __init__(
        self,
        conn: psycopg2.extensions.connection,
        start_date: str,
        end_date: str,
        project_name = None,
        project_names: Optional[Dict[str, str]] = None,
        set_date_format: bool = False,
        excluded_dates: Optional[List[str]] = None,
        date_format: str = None,
        trunc = 'day'
    ):
        super().__init__()
        self.conn = conn
        self.start_date = start_date
        self.end_date = end_date
        self.trunc = trunc

        if date_format is None:
            if self.trunc == 'hour':
                self.date_format = "YYYY-MM-DD HH24:MI:SS"
            elif self.trunc == 'day':
                self.date_format = "YYYY-MM-DD HH24:MI:SS"
            elif self.trunc == 'month':
                self.date_format = "YYYY-MM-DD"

        self.set_date_format = set_date_format
        self.project_name = project_name
        self.project_names = project_names or {}
        self.excluded_dates = excluded_dates or []
        self.base_sql_path = Path(BASE_SQL_PATH)

        self.project_array = ["General",
            "Agent",
            "Entrepreneurs",
            "Money",
            "TechSup",
            "Telesales",
            "Terminals",
            "Saima",
            "AkchaBulak",
            "O!Bank"]

        # 1) Парсим границы (терпимо к датам и датам-временам)
        start_dt = self._try_parse_period(start_date)
        end_dt   = self._try_parse_period(end_date)

        if not start_dt or not end_dt:
            raise ValueError("start_date/end_date have unexpected format")

        # 2) Генерим Period в точном соответствии с SQL [start, end)
        self.period = []
        self._rebuild_period()



    # ---------- helpers ----------

    def set_hours_trunked_data(self) -> None:
        """
        Настройка агрегации по часам.
        """
        self.trunc = 'hour'
        self.date_format = "YYYY-MM-DD HH24:MI:SS"
        self._rebuild_period()

    def set_days_trunked_data(self) -> None:
        """
        Настройка агрегации по дням.
        """
        self.trunc = 'day'
        self.date_format = "YYYY-MM-DD HH24:MI:SS"
        self._rebuild_period()

    def set_months_trunked_data(self) -> None:
        """
        Настройка агрегации по месяцам.
        """
        self.trunc = 'month'
        self.date_format = "YYYY-MM-DD"
        self._rebuild_period()

    @staticmethod
    def _pgfmt_to_pyfmt(pg: str) -> str:
        """
        Минимальная конвертация форматов Postgres -> Python strftime.
        Добавьте при необходимости ещё токены.
        """
        mapping = {
            "YYYY": "%Y", "YY": "%y",
            "MM": "%m",
            "DD": "%d",
            "HH24": "%H", "HH12": "%I", "HH": "%I",  # если используете 12ч формат
            "MI": "%M",
            "SS": "%S",
        }
        # Важно: сначала длинные токены (HH24), потом короткие
        order = ["YYYY","HH24","HH12","YY","MM","DD","HH","MI","SS"]
        out = pg
        for token in order:
            out = out.replace(token, mapping.get(token, token))
        return out

    def _rebuild_period(self) -> None:
        """Строит self.period так, чтобы строки 1-в-1 совпадали с to_char(date_trunc(trunc, hour), date_format)."""
        start = try_parse_period(self.start_date)
        end = try_parse_period(self.end_date)

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

        else:  # month
            keys = []
            cur = dt.datetime(start.year, start.month, 1)
            while cur < end:
                keys.append(cur.strftime("%Y-%m-%d"))
                # +1 месяц
                y, m = (cur.year + 1, 1) if cur.month == 12 else (cur.year, cur.month + 1)
                cur = dt.datetime(y, m, 1)
            self.period = keys

    @staticmethod
    def _try_parse_period(period: Any) -> Optional[datetime]:
        """Принимает str | datetime | date | pd.Timestamp и возвращает datetime."""
        if period is None:
            return None
        if isinstance(period, datetime):
            return period
        if isinstance(period, date):
            return datetime(period.year, period.month, period.day)

        try:
            import pandas as pd
            if isinstance(period, pd.Timestamp):
                return period.to_pydatetime()
        except Exception:
            pass
        if hasattr(period, "to_pydatetime"):
            try:
                return period.to_pydatetime()
            except Exception:
                pass

        if not isinstance(period, str):
            period = str(period)

        for fmt in (
                "%Y-%m-%d %H:%M:%S", "%Y-%m-%d",
                "%d.%m.%Y %H:%M:%S", "%d.%m.%Y",
                "%d-%m-%Y %H:%M:%S", "%d-%m-%Y",
                "%d/%m/%Y %H:%M:%S", "%d/%m/%Y",
        ):
            try:
                return datetime.strptime(period, fmt)
            except (ValueError, TypeError):
                pass

        try:
            s = period.replace("T", " ").split(".")[0].rstrip("Z")
            return datetime.fromisoformat(s)
        except Exception:
            return None

    def _fmt_out_date(self, dt: Optional[datetime], fallback: str) -> str:
        if dt is None:
            return fallback
        return dt.strftime("%Y-%m-%d %H:%M:%S") if self.set_date_format else dt.strftime("%d-%m-%Y")

    def _fmt_out_hour(self, dt: Optional[datetime]) -> Optional[str]:
        return dt.strftime("%H:%M:%S") if dt else "00:00:00"

    @staticmethod
    def calculate_indicator(numerator: Optional[float], denominator: Optional[float]) -> Optional[float]:
        n = 0 if numerator in (None, 0) else float(numerator)
        d = 0 if denominator in (None, 0) else float(denominator)

        if n == 0 and d == 0:
            return None
        if n == 0:
            return 0.0
        if d == 0:
            return 100.0
        return (n / d) * 100.0


    # ---------- SQL ----------
    def get_aggregated_main_data(self) -> pd.DataFrame:
        sql_path = self.base_sql_path / "stat" / "get_aggregated_main_data.sql"
        sql_template = sql_path.read_text(encoding="utf-8")

        selected_filter = (
            "\n  AND project_id = ANY(%(project_id)s::text[])"
            if self.SelectedProjects else ""
        )
        excluded_days_filter = (
            "\n  AND NOT (to_char(date_trunc(%(trunc)s::text, hour), %(date_format)s) = ANY(%(excluded_dates)s::text[]))"
            if self.excluded_dates else ""
        )

        query = sql_template.format(
            selected_filter=selected_filter,
            excluded_days_filter=excluded_days_filter,
        )

        params = {
            "start_date":  self.start_date,
            "end_date":    self.end_date,
            "trunc":       self.trunc,
            "date_format": self.date_format,
        }
        if self.SelectedProjects:
            params["project_id"] = self.SelectedProjects
        if self.excluded_dates:
            params["excluded_dates"] = self.excluded_dates


        df = pd.read_sql_query(query, self.conn, params=params)
        get_dagster_logger().info(f"df = {df}")

        get_dagster_logger().info(f"SelectProjects: {self.SelectedProjects}")

        return df

    def aggregated_main_data_detailed(self):
        sql_path = self.base_sql_path / "stat" / "get_aggregated_main_data_detailed.sql"
        sql_template = sql_path.read_text(encoding="utf-8")

        selected_filter = (
            "\n  AND project_id = ANY(%(project_id)s::int[])"
            if self.SelectedProjects else ""
        )
        excluded_days_filter = (
            "\n  AND NOT (to_char(date_trunc(%(trunc)s::text, hour), %(date_format)s) = ANY(%(excluded_dates)s::text[]))"
            if self.excluded_dates else ""
        )

        query = sql_template.format(
            selected_filter=selected_filter,
            excluded_days_filter=excluded_days_filter,
        )

        params = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "trunc": self.trunc,
            "date_format": self.date_format,
        }
        if self.SelectedProjects:
            params["project_id"] = self.SelectedProjects
        if self.excluded_dates:
            params["excluded_dates"] = self.excluded_dates

        df = pd.read_sql_query(query, self.conn, params=params)
        return df

    def aggregated_main_data_filtered_group(self):
        sql_path = self.base_sql_path / "stat" / "get_aggregated_data_filtered_group.sql"
        sql_template = sql_path.read_text(encoding="utf-8")

        selected_filter = (
            "\n  AND project_id = ANY(%(project_id)s)"
            if self.SelectedProjects else ""
        )
        excluded_days_filter = (
            "\n  AND NOT (to_char(date_trunc(%(trunc)s::text, hour), %(date_format)s) = ANY(%(excluded_dates)s::text[]))"
            if self.excluded_dates else ""
        )

        query = sql_template.format(
            selected_filter=selected_filter,
            excluded_days_filter=excluded_days_filter,
        )

        params = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "trunc": self.trunc,
            "date_format": self.date_format,
        }
        if self.SelectedProjects:
            params["project_id"] = self.SelectedProjects
        if self.excluded_dates:
            params["excluded_dates"] = self.excluded_dates

        df = pd.read_sql_query(query, self.conn, params=params)
        return df

    def get_arpu_trunked_data(self):
        sql_path = self.base_sql_path / "stat" / "get_arpu_trunked_data.sql"
        sql_template = sql_path.read_text(encoding="utf-8")
        params = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "trunc": self.trunc,
            "project_ids" : self.SelectedProjects
        }
        get_dagster_logger().info(f"params = {params}")

        df = pd.read_sql_query(sql_template, self.conn, params=params)
        return df.to_dict(orient="records")

    def get_language_trunked_data(self):
        sql_path = self.base_sql_path / "stat" / "get_language_trunked_data.sql"
        sql_template = sql_path.read_text(encoding="utf-8")
        params = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "project_ids": self.SelectedProjects,
        }

        df = pd.read_sql_query(sql_template, self.conn, params=params)
        return df.to_dict(orient="records")



    # ---------- сборка результата ----------

    def get_aggregated_data(self) -> Dict[str, Any]:
        df = self.get_aggregated_main_data()
        rows = df.to_dict("records")
        by_date: Dict[str, Dict[str, Any]] = {r["date"]: r for r in rows}
        items: List[Dict[str, Any]] = []
        for period in self.period:

            dt = self._try_parse_period(period)
            row = by_date.get(period)

            if row is not None:
                lost_val = (row["lost"] / 1000.0) if (self.set_date_format and row["lost"] is not None) else row["lost"]
                answered_val = (row["answered"] / 1000.0) if (self.set_date_format and row["answered"] is not None) else row["answered"]
                items.append({
                    "date": self._fmt_out_date(dt, period),
                    "hour": self._fmt_out_hour(dt),
                    "total": row["total"],
                    "redirected": row["redirected"],
                    "call_project_changed": row["call_project_changed"],
                    "queue": row["queue"],
                    "threshold_queue": row["threshold_queue"],
                    "total_queue": row["total_queue"],
                    "lost": lost_val,
                    "callback_success": row["callback_success"],
                    "callback_disabled": row["callback_disabled"],
                    "callback_unsuccessful": row["callback_unsuccessful"],
                    "total_to_operators": row["total_to_operators"],
                    "answered": answered_val,
                    "sla_answered": row["sla_answered"],
                    "sla_total": row["sla_total"],
                    "summary_waiting_time": row["summary_waiting_time"],
                    "minimum_waiting_time": row["minimum_waiting_time"],
                    "maximum_waiting_time": row["maximum_waiting_time"],
                    "SL": self.calculate_indicator(row["sla_answered"], row["sla_total"]),
                    "ACD": self.calculate_indicator(row["answered"], row["total_to_operators"]),
                    "projectName": self.project_names.get(self.project_name) if self.project_name else None,
                })
            else:
                items.append({
                    "date": self._fmt_out_date(dt, period),
                    "hour": self._fmt_out_hour(dt),
                    "total": None,
                    "redirected": None,
                    "call_project_changed": None,
                    "queue": None,
                    "threshold_queue": None,
                    "total_queue": None,
                    "lost": None,
                    "callback_success": None,
                    "callback_disabled": None,
                    "callback_unsuccessful": None,
                    "total_to_operators": None,
                    "answered": None,
                    "sla_answered": None,
                    "sla_total": None,
                    "summary_waiting_time": None,
                    "minimum_waiting_time": None,
                    "maximum_waiting_time": None,
                    "SL": None,
                    "ACD": None,
                    "projectName": self.project_names.get(self.project_name) if self.project_name else None,
                })

        labels = [
            {"field": "date", "title": "Дата"},
            {"field": "hour", "title": "Час"},
            {"field": "total", "title": "Всего вызовов"},
            {"field": "total_to_operators", "title": "Распределено на операторов"},
            {"field": "answered", "title": "Отвечено операторами"},
            {"field": "sla_answered", "title": "Отвечено в рамках SL"},
            {"field": "sla_total", "title": "Вызовы в рамках SL"},
            {"field": "summary_waiting_time", "title": "Суммарное время ожидания"},
            {"field": "minimum_waiting_time", "title": "Минимальное время ожидания"},
            {"field": "maximum_waiting_time", "title": "Максимальное время ожидания"},
            {"field": "SL", "title": "Service Level"},
            {"field": "ACD", "title": "ACD"},
        ]
        result = {"labels": labels, "items": items}
        return result

    def get_aggregated_data_ivr(self):
        df = self.get_aggregated_main_data()
        rows = df.to_dict("records")
        by_date: Dict[str, Dict[str, Any]] = {r.get("date"): r for r in rows}

        items: List[Dict[str, Any]] = []
        for period in self.period:
            dt: Optional[datetime] = self._try_parse_period(period)
            row = by_date.get(period)

            if row is not None:
                redirected = row.get("redirected")
                ivr_redirected_val = (
                    (redirected / 1000.0) if (self.set_date_format and redirected is not None) else redirected)

                items.append({
                    "date": self._fmt_out_date(dt, period),
                    "hour": self._fmt_out_hour(dt),

                    "ivr_total": row.get("total"),
                    "ivr_ivr": row.get("ivr"),
                    "ivr_redirected": ivr_redirected_val,
                    "ivr_call_project_changed": row.get("call_project_changed"),
                    "ivr_queue": row.get("queue"),
                    "ivr_threshold_queue": row.get("threshold_queue"),
                    "ivr_lost": row.get("lost"),
                    "ivr_callback_success": row.get("callback_success"),
                    "ivr_callback_disabled": row.get("callback_disabled"),
                    "ivr_callback_unsuccessful": row.get("callback_unsuccessful"),
                    "ivr_total_to_operators": row.get("total_to_operators"),
                    "ivr_answered": row.get("answered"),
                    "ivr_sla_answered": row.get("sla_answered"),
                    "ivr_sla_total": row.get("sla_total"),
                    "ivr_summary_waiting_time": row.get("summary_waiting_time"),
                    "ivr_minimum_waiting_time": row.get("minimum_waiting_time"),
                    "ivr_maximum_waiting_time": row.get("maximum_waiting_time"),

                    # SSR = ivr / total
                    "SSR": self.calculate_indicator(row.get("ivr"), row.get("total")),
                })
            else:
                items.append({
                    "date": self._fmt_out_date(dt, period),
                    "hour": self._fmt_out_hour(dt),

                    "ivr_total": None,
                    "ivr_ivr": None,
                    "ivr_redirected": None,
                    "ivr_call_project_changed": None,
                    "ivr_queue": None,
                    "ivr_threshold_queue": None,
                    "ivr_lost": None,
                    "ivr_callback_success": None,
                    "ivr_callback_disabled": None,
                    "ivr_callback_unsuccessful": None,
                    "ivr_total_to_operators": None,
                    "ivr_answered": None,
                    "ivr_sla_answered": None,
                    "ivr_sla_total": None,
                    "ivr_summary_waiting_time": None,
                    "ivr_minimum_waiting_time": None,
                    "ivr_maximum_waiting_time": None,
                    "SSR": None,
                })

        labels = [
            {"field": "date", "title": "Дата"},
            {"field": "hour", "title": "Час"},
            {"field": "ivr_total", "title": "Поступило на IVR"},
            {"field": "ivr_ivr", "title": "Завершены в IVR"},
            {"field": "ivr_redirected", "title": "Направлено на операторские проекты"},
            {"field": "ivr_call_project_changed", "title": "ivr_call_project_changed"},
            {"field": "ivr_queue", "title": "ivr_queue"},
            {"field": "ivr_threshold_queue", "title": "ivr_threshold_queue"},
            {"field": "ivr_lost", "title": "ivr_lost"},
            {"field": "ivr_callback_success", "title": "ivr_callback_success"},
            {"field": "ivr_callback_disabled", "title": "ivr_callback_disabled"},
            {"field": "ivr_callback_unsuccessful", "title": "ivr_callback_unsuccessful"},
            {"field": "ivr_total_to_operators", "title": "ivr_total_to_operators"},
            {"field": "ivr_answered", "title": "ivr_answered"},
            {"field": "ivr_sla_answered", "title": "ivr_sla_answered"},
            {"field": "ivr_sla_total", "title": "ivr_sla_total"},
            {"field": "ivr_summary_waiting_time", "title": "ivr_summary_waiting_time"},
            {"field": "ivr_minimum_waiting_time", "title": "ivr_minimum_waiting_time"},
            {"field": "ivr_maximum_waiting_time", "title": "ivr_maximum_waiting_time"},
            {"field": "SSR", "title": "Self Service Rate"},
        ]
        get_dagster_logger().info(f"items = {items}")
        return {"labels": labels, "items": items}

    def get_aggregated_data_set_date(self):
        df = self.get_aggregated_main_data()
        rows = df.to_dict("records")
        by_date: Dict[str, Dict[str, Any]] = {r.get("date"): r for r in rows}
        items: List[Dict[str, Any]] = []
        for period in self.period:
            dt = self._try_parse_period(period)
            row = by_date.get(period)

            date_out = dt.strftime("%Y-%m-%d %H:%M:%S") if dt else period
            hour_out = self._fmt_out_hour(dt)

            if row is not None:
                lost_val = (row.get("lost") / 1000.0) if (
                            self.set_date_format and row.get("lost") is not None) else row.get("lost")
                answered_val = (row.get("answered") / 1000.0) if (
                            self.set_date_format and row.get("answered") is not None) else row.get("answered")

                items.append({
                    "date": date_out,
                    "hour": hour_out,

                    "total": row.get("total"),
                    "redirected": row.get("redirected"),
                    "call_project_changed": row.get("call_project_changed"),
                    "queue": row.get("queue"),
                    "threshold_queue": row.get("threshold_queue"),
                    "total_queue": row.get("total_queue"),
                    "lost": lost_val,
                    "callback_success": row.get("callback_success"),
                    "callback_disabled": row.get("callback_disabled"),
                    "callback_unsuccessful": row.get("callback_unsuccessful"),
                    "total_to_operators": row.get("total_to_operators"),
                    "answered": answered_val,
                    "sla_answered": row.get("sla_answered"),
                    "sla_total": row.get("sla_total"),
                    "summary_waiting_time": row.get("summary_waiting_time"),
                    "minimum_waiting_time": row.get("minimum_waiting_time"),
                    "maximum_waiting_time": row.get("maximum_waiting_time"),
                    "SL": self.calculate_indicator(row.get("sla_answered"), row.get("sla_total")),
                    "ACD": self.calculate_indicator(row.get("answered"), row.get("total_to_operators")),
                })
            else:
                items.append({
                    "date": date_out,
                    "hour": hour_out,

                    "total": None,
                    "redirected": None,
                    "call_project_changed": None,
                    "queue": None,
                    "threshold_queue": None,
                    "total_queue": None,
                    "lost": None,
                    "callback_success": None,
                    "callback_disabled": None,
                    "callback_unsuccessful": None,
                    "total_to_operators": None,
                    "answered": None,
                    "sla_answered": None,
                    "sla_total": None,
                    "summary_waiting_time": None,
                    "minimum_waiting_time": None,
                    "maximum_waiting_time": None,
                    "SL": None,
                    "ACD": None,
                })

        return items

    def get_aggregated_data_ivr_set_date(self):
        df = self.get_aggregated_main_data()
        rows = df.to_dict("records")
        by_date: Dict[str, Dict[str, Any]] = {r.get("date"): r for r in rows}
        items: List[Dict[str, Any]] = []

        for period in self.period:
            dt = self._try_parse_period(period)
            row = by_date.get(period)
            date_out = dt.strftime("%Y-%m-%d %H:%M:%S") if dt else period
            hour_out = self._fmt_out_hour(dt)


            if row is not None:
                items.append({
                    "date": date_out,
                    "hour": hour_out,

                    "ivr_total": row.get("total"),
                    "ivr_ivr": row.get("ivr"),
                    "ivr_redirected": row.get("redirected"),
                    "ivr_call_project_changed": row.get("call_project_changed"),
                    "ivr_queue": row.get("queue"),
                    "ivr_threshold_queue": row.get("threshold_queue"),
                    "ivr_lost": row.get("lost"),
                    "ivr_callback_success": row.get("callback_success"),
                    "ivr_callback_disabled": row.get("callback_disabled"),
                    "ivr_callback_unsuccessful": row.get("callback_unsuccessful"),
                    "ivr_total_to_operators": row.get("total_to_operators"),
                    "ivr_answered": row.get("answered"),
                    "ivr_sla_answered": row.get("sla_answered"),
                    "ivr_sla_total": row.get("sla_total"),
                    "ivr_summary_waiting_time": row.get("summary_waiting_time"),
                    "ivr_minimum_waiting_time": row.get("minimum_waiting_time"),
                    "ivr_maximum_waiting_time": row.get("maximum_waiting_time"),
                    "SSR": self.calculate_indicator(row.get("ivr"), row.get("total")),
                })
            else:
                items.append({
                    "date": date_out,
                    "hour": hour_out,

                    "ivr_total": None,
                    "ivr_ivr": None,
                    "ivr_redirected": None,
                    "ivr_call_project_changed": None,
                    "ivr_queue": None,
                    "ivr_threshold_queue": None,
                    "ivr_lost": None,
                    "ivr_callback_success": None,
                    "ivr_callback_disabled": None,
                    "ivr_callback_unsuccessful": None,
                    "ivr_total_to_operators": None,
                    "ivr_answered": None,
                    "ivr_sla_answered": None,
                    "ivr_sla_total": None,
                    "ivr_summary_waiting_time": None,
                    "ivr_minimum_waiting_time": None,
                    "ivr_maximum_waiting_time": None,
                    "SSR": None,
                })

        return items

    def get_aggregated_data_ivr_detailed(self):
        df = self.aggregated_main_data_detailed()
        rows = df.to_dict("records")
        items: List[Dict[str, Any]] = []

        for row in rows:
            raw_date = row.get("date")
            dt: Optional[datetime] = self._try_parse_period(raw_date)
            # если set_date_format -> 'YYYY-MM-DD HH:MM:SS', иначе 'DD.MM.YYYY'
            if self.set_date_format:
                date_out = dt.strftime("%Y-%m-%d %H:%M:%S") if dt else raw_date
            else:
                date_out = dt.strftime("%d-%m-%Y") if dt else raw_date

            # час из той же даты (при неудаче — '00:00:00', как date('H:i:s', strtotime(...)))
            hour_out = dt.strftime("%H:%M:%S") if dt else "00:00:00"

            # SSR = ivr / total
            ssr = self.calculate_indicator(row.get("ivr"), row.get("total"))

            items.append({
                "date": date_out,
                "hour": hour_out,
                "SSR": ssr,
                "project_key": row.get("project_key"),
            })

        return items

    def get_aggregated_data_detailed(self):
        df = self.aggregated_main_data_detailed()
        rows = df.to_dict("records")
        items: List[Dict[str, Any]] = []

        for row in rows:
            raw_date = row.get("date")

            dt: Optional[datetime] = self._try_parse_period(raw_date)

            if self.set_date_format:
                date_out = dt.strftime("%Y-%m-%d %H:%M:%S") if dt else raw_date
            else:
                date_out = dt.strftime("%d-%m-%Y") if dt else raw_date

            hour_out = dt.strftime("%H:%M:%S") if dt else "00:00:00"
            sl = self.calculate_indicator(row.get("sla_answered"), row.get("sla_total"))
            hcr = self.calculate_indicator(row.get("answered"), row.get("total_to_operators"))

            items.append({
                "date": date_out,
                "hour": hour_out,
                "SL": sl,
                "HCR": hcr,
                "project_key": row.get("project_key"),
            })

        return items

    def get_aggregated_data_filtered_projects(self):
        df = self.aggregated_main_data_filtered_group()
        rows = df.to_dict("records")

        by_date: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for r in rows:
            by_date[r.get("date")].append(r)

        items: List[Dict[str, Any]] = []

        for period in self.period:
            dt: Optional[datetime] = self._try_parse_period(period)
            rows_for_period = by_date.get(period, [])

            if rows_for_period:
                date_out = dt.strftime("%Y-%m-%d %H:%M:%S") if dt else period
                hour_out = self._fmt_out_hour(dt)

                for row in rows_for_period:
                    items.append({
                        "date": date_out,
                        "hour": hour_out,

                        "total": row.get("total"),
                        "redirected": row.get("redirected"),
                        "call_project_changed": row.get("call_project_changed"),
                        "queue": row.get("queue"),
                        "threshold_queue": row.get("threshold_queue"),
                        "total_queue": row.get("total_queue"),
                        "lost": row.get("lost"),
                        "callback_success": row.get("callback_success"),
                        "callback_disabled": row.get("callback_disabled"),
                        "callback_unsuccessful": row.get("callback_unsuccessful"),
                        "total_to_operators": row.get("total_to_operators"),
                        "answered": row.get("answered"),
                        "sla_answered": row.get("sla_answered"),
                        "sla_total": row.get("sla_total"),
                        "summary_waiting_time": row.get("summary_waiting_time"),
                        "minimum_waiting_time": row.get("minimum_waiting_time"),
                        "maximum_waiting_time": row.get("maximum_waiting_time"),

                        "SL": self.calculate_indicator(row.get("sla_answered"), row.get("sla_total")),
                        "ACD": self.calculate_indicator(row.get("answered"), row.get("total_to_operators")),
                        "projectName": row.get("project"),
                    })
            else:
                items.append({
                    "date": self._fmt_out_date(dt, period),
                    "hour": self._fmt_out_hour(dt),

                    "total": None,
                    "redirected": None,
                    "call_project_changed": None,
                    "queue": None,
                    "threshold_queue": None,
                    "total_queue": None,
                    "lost": None,
                    "callback_success": None,
                    "callback_disabled": None,
                    "callback_unsuccessful": None,
                    "total_to_operators": None,
                    "answered": None,
                    "sla_answered": None,
                    "sla_total": None,
                    "summary_waiting_time": None,
                    "minimum_waiting_time": None,
                    "maximum_waiting_time": None,

                    "SL": None,
                    "ACD": None,
                    "projectName": None,
                })

        return items

    def get_aggregated_data_ivr_filtered_projects(self) -> List[Dict[str, Any]]:
        df = self.aggregated_main_data_filtered_group()
        rows = df.to_dict("records") if hasattr(df, "to_dict") else list(df)

        by_date: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for r in rows:
            by_date[r.get("date")].append(r)

        items: List[Dict[str, Any]] = []

        for period in self.period:
            dt: Optional[datetime] = self._try_parse_period(period)
            matches = by_date.get(period, [])

            if matches:
                date_out = dt.strftime("%Y-%m-%d %H:%M:%S") if dt else period
                hour_out = self._fmt_out_hour(dt)

                for row in matches:
                    items.append({
                        "date": date_out,
                        "hour": hour_out,

                        "ivr_total": row.get("total"),
                        "ivr_ivr": row.get("ivr"),
                        "ivr_redirected": row.get("redirected"),
                        "ivr_call_project_changed": row.get("call_project_changed"),
                        "ivr_queue": row.get("queue"),
                        "ivr_threshold_queue": row.get("threshold_queue"),
                        "ivr_lost": row.get("lost"),
                        "ivr_callback_success": row.get("callback_success"),
                        "ivr_callback_disabled": row.get("callback_disabled"),
                        "ivr_callback_unsuccessful": row.get("callback_unsuccessful"),
                        "ivr_total_to_operators": row.get("total_to_operators"),
                        "ivr_answered": row.get("answered"),
                        "ivr_sla_answered": row.get("sla_answered"),
                        "ivr_sla_total": row.get("sla_total"),
                        "ivr_summary_waiting_time": row.get("summary_waiting_time"),
                        "ivr_minimum_waiting_time": row.get("minimum_waiting_time"),
                        "ivr_maximum_waiting_time": row.get("maximum_waiting_time"),

                        "SSR": self.calculate_indicator(row.get("ivr"), row.get("total")),
                        "projectName": row.get("project"),
                    })
            else:
                items.append({
                    "date": self._fmt_out_date(dt, period),
                    "hour": self._fmt_out_hour(dt),

                    "ivr_total": None,
                    "ivr_ivr": None,
                    "ivr_redirected": None,
                    "ivr_call_project_changed": None,
                    "ivr_queue": None,
                    "ivr_threshold_queue": None,
                    "ivr_lost": None,
                    "ivr_callback_success": None,
                    "ivr_callback_disabled": None,
                    "ivr_callback_unsuccessful": None,
                    "ivr_total_to_operators": None,
                    "ivr_answered": None,
                    "ivr_sla_answered": None,
                    "ivr_sla_total": None,
                    "ivr_summary_waiting_time": None,
                    "ivr_minimum_waiting_time": None,
                    "ivr_maximum_waiting_time": None,

                    "SSR": None,
                    "projectName": None,
                })

        return items

    @staticmethod
    def _php_fmt_to_py(fmt: str) -> str:
        """
        Преобразует популярные токены формата PHP в strftime Python.
        Если в строке уже есть '%', считаем что это python-формат и возвращаем как есть.
        """
        if '%' in fmt:
            return fmt
        return (fmt
                .replace('Y', '%Y')
                .replace('m', '%m')
                .replace('d', '%d')
                .replace('H', '%H')
                .replace('i', '%M')
                .replace('s', '%S'))

    @staticmethod
    def _parse_iso_duration(dur: str) -> relativedelta:
        """
        Простой парсер ISO 8601 duration: PnY nM nW nD T nH nM nS -> relativedelta.
        """
        m = re.fullmatch(
            r'P'
            r'(?:(?P<Y>\d+)Y)?'
            r'(?:(?P<Mo>\d+)M)?'
            r'(?:(?P<W>\d+)W)?'
            r'(?:(?P<D>\d+)D)?'
            r'(?:T'
            r'(?:(?P<H>\d+)H)?'
            r'(?:(?P<Mi>\d+)M)?'
            r'(?:(?P<S>\d+)S)?'
            r')?',
            dur
        )
        if not m:
            raise ValueError(f"Bad ISO 8601 duration: {dur!r}")

        vals = {k: int(v) if v else 0 for k, v in m.groupdict().items()}
        if not any(vals.values()):
            raise ValueError("Zero interval is not allowed")

        return relativedelta(
            years=vals['Y'], months=vals['Mo'], weeks=vals['W'], days=vals['D'],
            hours=vals['H'], minutes=vals['Mi'], seconds=vals['S']
        )

    @staticmethod
    def get_dates_from_range(
            start_date,
            end_date,
            interval: str = 'PT1H',  # для часов
            format: str = 'Y-m-d H:i:s',
            input_dt_format: str = '%Y-%m-%d %H:%M:%S',
    ):
        """
        Аналог PHP DatePeriod: конец исключается.
        Принимает start_date/end_date как str | datetime | date.
        """
        try:
            py_fmt = NaumenServiceLevelDataGetter._php_fmt_to_py(format)
            step = NaumenServiceLevelDataGetter._parse_iso_duration(interval)

            def _coerce(v):
                if isinstance(v, datetime):
                    return v
                if isinstance(v, date):
                    return datetime.combine(v, time(0, 0, 0))
                if isinstance(v, str):
                    # пробуем полный формат, а потом только дату
                    try:
                        return datetime.strptime(v, input_dt_format)
                    except ValueError:
                        return datetime.strptime(v, '%Y-%m-%d')
                raise TypeError(f"Unsupported datetime type: {type(v)} for value {v!r}")

            start = _coerce(start_date)
            end = _coerce(end_date)

            if start >= end:
                raise ValueError(f"start ({start}) must be < end ({end})")

            out = []
            cur = start
            guard = 0
            while cur < end:
                out.append(cur.strftime(py_fmt))
                cur = cur + step
                guard += 1
                if guard > 10_000_000:
                    raise RuntimeError("Too many iterations; check your interval and dates")
            return out
        except Exception as e:
            raise RuntimeError(f"get_dates_from_range failed: {e}") from e