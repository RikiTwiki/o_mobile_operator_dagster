# average_handling_time_data_getter.py
# Python 3.11+
from __future__ import annotations
from datetime import datetime, date, timedelta
import datetime as dt
import re
import traceback
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import psycopg
from dagster import get_dagster_logger
from psycopg.rows import dict_row

from dateutil.relativedelta import relativedelta

from utils.getters.naumen_projects_getter import NaumenProjectsGetter

from utils.path import BASE_SQL_PATH

from pathlib import Path


TABLE = "stat.replication_naumen_handling_time_data"


def _safe_div(n: Optional[float], d: Optional[float]) -> Optional[float]:
    try:
        if n is None or d in (None, 0):
            return None
        return n / d
    except Exception:
        return None

def _as_dt(v: object) -> dt.datetime:
    if isinstance(v, dt.datetime):
        return v
    if isinstance(v, dt.date):
        return dt.datetime.combine(v, dt.time.min)
    if isinstance(v, str):
        # сначала строгий формат, затем ISO
        try:
            return dt.datetime.strptime(v, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return dt.datetime.fromisoformat(v)  # поддержит 'YYYY-MM-DD' и ISO
    # последний шанс — попробовать ISO из str(v)
    return dt.datetime.fromisoformat(str(v))


class AverageHandlingTimeDataGetter(NaumenProjectsGetter):
    """
    Порт PHP AverageHandlingTimeDataGetter.
    Работает поверх psycopg2-коннекта к DWH.
    """

    def __init__(
        self,
        dwh_conn,
        start_date: str,
        end_date: str,
        report_date: str = None,
        project_name = "",
        project_names: Optional[Dict[str, str]] = None,
        set_date_format: bool = False,
        trunc: str = "day",  # "hour" | "day" | "month"
    ):
        super().__init__()
        self.conn = dwh_conn

        self.base_sql_path = Path(BASE_SQL_PATH)

        # входные параметры как строки 'YYYY-MM-DD HH:MM:SS'
        self.start_date = _as_dt(start_date).strftime("%Y-%m-%d %H:%M:%S")
        self.end_date = _as_dt(end_date).strftime("%Y-%m-%d %H:%M:%S")
        self.report_date = report_date

        self.project_name = project_name
        self.project_names: Dict[str, str] = project_names or {}
        self.set_date_format: bool = set_date_format

        # агрегационная гранулярность и формат вывода из БД
        self.trunc: str = trunc  # hour|day|month
        if self.trunc == "day":
            self.date_format = "YYYY-MM-DD HH24:MI:SS"
        elif self.trunc == "hour":
            self.date_format = "YYYY-MM-DD HH:MI:SS"
        elif self.trunc == "month":
            self.date_format = "YYYY-MM-DD"
        else:
            raise ValueError("trunc must be 'hour' | 'day' | 'month'")

        # список ключей периода в строковом виде, совпадающем с to_char(..., date_format)
        self.period: List[str] = []
        self._rebuild_period()

    # ------------------------------ helpers ------------------------------

    def _fetchall(self, sql: str, params: Tuple[Any, ...]) -> List[Dict[str, Any]]:
        # Определяем тип коннектора по модулю класса
        mod = getattr(self.conn, "__class__", type(self.conn)).__module__
        if mod.startswith("psycopg."):  # psycopg v3
            from psycopg.rows import dict_row
            with self.conn.cursor(row_factory=dict_row) as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
            return [dict(r) for r in rows]
        else:
            from psycopg2.extras import RealDictCursor
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
            return [dict(r) for r in rows]

    def _selected_projects_clause(self) -> Tuple[str, Tuple[Any, ...]]:
        """
        Возвращает кусок SQL и параметры для фильтра project_id.
        В PHP было whereIn(project_id::text, SelectedProjects).
        """
        if not getattr(self, "SelectedProjects", None):
            return "", tuple()
        vals = [str(v) for v in self.SelectedProjects]
        placeholders = ", ".join(["%s"] * len(vals))
        return f"AND project_id::text IN ({placeholders})", tuple(vals)

    def _validate_trunc_and_format(self) -> None:
        assert self.trunc in ("hour", "day", "month")

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

    def _rebuild_period(self) -> None:
        """Строит self.period так, чтобы строки 1-в-1 совпадали с to_char(date_trunc(trunc, hour), date_format)."""
        start = dt.datetime.strptime(self.start_date, "%Y-%m-%d %H:%M:%S")
        end = dt.datetime.strptime(self.end_date,   "%Y-%m-%d %H:%M:%S")

        if self.trunc == "hour":
            # ключ вида 'YYYY-MM-DD HH:MI:SS' — 12-часовой, без AM/PM → %I
            keys: List[str] = []
            cur = start
            while cur < end:
                keys.append(cur.strftime("%Y-%m-%d %H:%M:%S"))
                cur += dt.timedelta(hours=1)
            self.period = keys

        elif self.trunc == "day":
            # ключ 'YYYY-MM-DD 00:00:00'
            keys = []
            cur = start
            while cur < end:
                keys.append(cur.strftime("%Y-%m-%d 00:00:00"))
                cur += dt.timedelta(days=1)
            self.period = keys

        else:  # month
            # ключ 'YYYY-MM-DD' (первое число месяца)
            keys = []
            cur = dt.datetime(start.year, start.month, 1)
            while cur < end:
                keys.append(cur.strftime("%Y-%m-%d"))  # YYYY-MM-01
                # +1 месяц
                y, m = (cur.year + 1, 1) if cur.month == 12 else (cur.year, cur.month + 1)
                cur = dt.datetime(y, m, 1)
            self.period = keys

    def _key_to_outputs(self, key: str) -> Tuple[str, str]:
        if self.trunc == "hour":
            if self.set_date_format:
                # безопасно делим: всегда 3 части (до, разделитель, после)
                date_part, _, time_part = key.partition(" ")
                if not time_part:
                    # если время отсутствует — подставим 00:00:00
                    time_part = "00:00:00"
                return date_part, time_part
            else:
                # пытаемся 24ч, затем 12ч; если времени нет — считаем полночь
                for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %I:%M:%S"):
                    try:
                        dt_obj = dt.datetime.strptime(key, fmt)
                        return dt_obj.strftime("%d.%m.%Y"), dt_obj.strftime("%H:%M:%S")
                    except ValueError:
                        pass
                try:
                    dt_obj = dt.datetime.strptime(key, "%Y-%m-%d")
                    return dt_obj.strftime("%d.%m.%Y"), "00:00:00"
                except ValueError:
                    # последний фоллбек
                    return key, "00:00:00"

        if self.trunc == "day":
            if self.set_date_format:
                date_out = key
            else:
                date_out = dt.datetime.strptime(key, "%Y-%m-%d %H:%M:%S").strftime("%d.%m.%Y")
            return date_out, "00:00:00"

        # month
        if self.set_date_format:
            date_out = key
        else:
            date_out = dt.datetime.strptime(key, "%Y-%m-%d").strftime("%d.%m.%Y")
        return date_out, "00:00:00"

    # Удобные сеттеры
    def set_interval(self, start_date: str, end_date: str) -> None:
        self.start_date = _as_dt(start_date).strftime("%Y-%m-%d %H:%M:%S")
        self.end_date = _as_dt(end_date).strftime("%Y-%m-%d %H:%M:%S")
        self._rebuild_period()

    def set_trunc(self, trunc: str) -> None:
        self.trunc = trunc
        if self.trunc == "day":
            self.date_format = "YYYY-MM-DD HH24:MI:SS"
        elif self.trunc == "hour":
            self.date_format = "YYYY-MM-DD HH:MI:SS"
        elif self.trunc == "month":
            self.date_format = "YYYY-MM-DD"
        else:
            raise ValueError("trunc must be 'hour' | 'day' | 'month'")
        self._rebuild_period()

    # ------------------------------ public API ------------------------------

    def get_average_handling_time_by_staff_unit_id(
        self,
        staff_unit_id: int,
        start_date: str,
        end_date: str,
    ) -> Dict[str, Any]:
        """
        Агрегаты по staff_unit_id за интервал, исключая UZ и группы ['Telesales','Money','Saima'].
        """
        try:
            base_sql_path = getattr(self, "base_sql_path", Path("sql"))
            sql_path = base_sql_path / "naumen" / "get_average_handling_time_by_staff_unit_id.sql"
            sql = sql_path.read_text(encoding="utf-8")
            rows = self._fetchall(sql, (staff_unit_id, start_date, end_date))
            if not rows:
                return {
                    "staff_unit_id": staff_unit_id,
                    "user_login": "",
                    "quantity": 0,
                    "average_pickup_time": None,
                    "average_speaking_time": None,
                    "average_wrapup_time": None,
                    "average_handling_time": None,
                    "sum_pickup_time": 0,
                    "sum_speaking_time": 0,
                    "sum_wrapup_time": 0,
                    "sum_holding_time": 0,
                    "max_pickup_time": None,
                    "max_speaking_time": None,
                    "max_wrapup_time": None,
                    "max_holding_time": None,
                }

            r = rows[0]
            q = r["quantity"] or 0
            s_pick = r["sum_pickup_time"] or 0
            s_speak = r["sum_speaking_time"] or 0
            s_wrap = r["sum_wrapup_time"] or 0

            return {
                "staff_unit_id": r["staff_unit_id"],
                "user_login": r["user_login"],
                "quantity": q,
                "average_pickup_time": _safe_div(s_pick, q),
                "average_speaking_time": _safe_div(s_speak, q),
                "average_wrapup_time": _safe_div(s_wrap, q),
                "average_handling_time": _safe_div(s_pick + s_speak + s_wrap, q),
                "sum_pickup_time": s_pick,
                "sum_speaking_time": s_speak,
                "sum_wrapup_time": s_wrap,
                "sum_holding_time": r["sum_holding_time"] or 0,
                "max_pickup_time": r["max_pickup_time"],
                "max_speaking_time": r["max_speaking_time"],
                "max_wrapup_time": r["max_wrapup_time"],
                "max_holding_time": r["max_holding_time"],
            }
        except Exception as e:
            return {
                "error": "Ошибка в логике подсчета",
                "staff_unit_id": staff_unit_id,
                "detail": str(e),
            }

    def get_average_handling_time_construct(
        self,
        start_date: str,
        end_date: str,
        excluded_projects: Iterable[str] = ("Telesales", "Money", "Saima"),
    ) -> List[Dict[str, Any]]:
        """
        Группировка по staff_unit_id, user_login с исключением языков/проектов; добавляет поля average_*.
        """
        base_sql_path = getattr(self, "base_sql_path", Path("sql"))
        sql_path = base_sql_path / "naumen" / "get_average_handling_time_construct.sql"
        sql = sql_path.read_text(encoding="utf-8")

        # 2) подготовить параметры
        excluded = list(excluded_projects)

        rows = self._fetchall(sql, (start_date, end_date, excluded))
        out: List[Dict[str, Any]] = []
        for r in rows:
            q = r["quantity"] or 0
            s_pick = r["sum_pickup_time"] or 0
            s_speak = r["sum_speaking_time"] or 0
            s_wrap = r["sum_wrapup_time"] or 0
            out.append({
                **r,
                "average_handling_time": _safe_div(s_pick + s_speak + s_wrap, q),
                "average_pickup_time": _safe_div(s_pick, q),
                "average_speaking_time": _safe_div(s_speak, q),
                "average_wrapup_time": _safe_div(s_wrap, q),
            })
        return out

    def get_average_handling_time_by_projects(
        self,
        start_date: str,
        end_date: str,
        projects: Iterable[str] = ("Saima",),
    ) -> List[Dict[str, Any]]:
        """
        То же, но фильтр по группе (проекты).
        """
        base_sql_path = getattr(self, "base_sql_path", Path("sql"))
        sql_path = base_sql_path / "naumen" / "get_average_handling_time_by_projects.sql"
        sql = sql_path.read_text(encoding="utf-8")

        # 2) подготовить параметры (ARRAY для = ANY(%s))
        projects_list = list(projects)
        if not projects_list:
            return []
        rows = self._fetchall(sql, (start_date, end_date, projects_list))
        out: List[Dict[str, Any]] = []
        for r in rows:
            q = r["quantity"] or 0
            s_pick = r["sum_pickup_time"] or 0
            s_speak = r["sum_speaking_time"] or 0
            s_wrap = r["sum_wrapup_time"] or 0
            out.append({
                **r,
                "average_handling_time": _safe_div(s_pick + s_speak + s_wrap, q),
                "average_pickup_time": _safe_div(s_pick, q),
                "average_speaking_time": _safe_div(s_speak, q),
                "average_wrapup_time": _safe_div(s_wrap, q),
            })
        return out

    def get_arpu_trunked_data(self) -> List[Dict[str, Any]]:
        """
        SELECT date_trunc(trunc, date), arpu, avg(max_pickup_time)
        WHERE date ∈ [start_date, end_date) AND project_id ∈ SelectedProjects
        GROUP BY 1,2 ORDER BY 1
        """
        self._validate_trunc_and_format()
        where_projects_sql, params_projects = self._selected_projects_clause()

        base_sql_path = getattr(self, "base_sql_path", Path("sql"))
        sql_path = base_sql_path / "naumen" / "get_arpu_trunked_data.sql"
        sql = sql_path.read_text(encoding="utf-8")
        sql = sql.replace(
            "/*WHERE_PROJECTS_CLAUSE*/",
            f"\n  {where_projects_sql}\n" if where_projects_sql else ""
        )
        params: Tuple[Any, ...] = (self.trunc, self.start_date, self.end_date, *params_projects)
        return self._fetchall(sql, params)

    def get_aggregated_data(self) -> Dict[str, Any]:
        """
        Главная агрегированная выборка (аналог PHP getAggregatedData),
        плюс дорисовка пустых интервалов из self.period.
        """
        self._validate_trunc_and_format()
        where_projects_sql, params_projects = self._selected_projects_clause()

        base_sql_path = getattr(self, "base_sql_path", Path("sql"))
        sql_path = base_sql_path / "naumen" / "get_aggregated_data.sql"
        sql = sql_path.read_text(encoding="utf-8")
        sql = sql.replace(
            "/*WHERE_PROJECTS_CLAUSE*/",
            f"\n  {where_projects_sql}\n" if where_projects_sql else ""
        )
        params: Tuple[Any, ...] = (self.trunc, self.date_format, self.start_date, self.end_date, *params_projects)
        db_rows = self._fetchall(sql, params)
        by_key = {r["date"]: r for r in db_rows}

        get_dagster_logger().info(f"period in avg_handling_time = {self.period}")

        items: List[Dict[str, Any]] = []
        for key in self.period:
            src = by_key.get(key)
            date_out, hour_out = self._key_to_outputs(key)
            if src:
                items.append({
                    "date": date_out,
                    "hour": hour_out,
                    **{k: src.get(k) for k in (
                        "quantity", "sum_pickup_time", "min_pickup_time", "max_pickup_time",
                        "sum_speaking_time", "min_speaking_time", "max_speaking_time",
                        "sum_wrapup_time", "min_wrapup_time", "max_wrapup_time",
                        "quantity_hold", "sum_holding_time", "min_holding_time", "max_holding_time",
                        "sum_handling_time", "average_handling_time", "average_ringing_time",
                        "average_holding_time", "average_speaking_time",
                    )},
                    "projectName": self.project_names.get(self.project_name) if self.project_name else None,
                })
            else:
                items.append({
                    "date": date_out,
                    "hour": hour_out,
                    "quantity": None,
                    "sum_pickup_time": None,
                    "min_pickup_time": None,
                    "max_pickup_time": None,
                    "sum_speaking_time": None,
                    "min_speaking_time": None,
                    "max_speaking_time": None,
                    "sum_wrapup_time": None,
                    "min_wrapup_time": None,
                    "max_wrapup_time": None,
                    "quantity_hold": None,
                    "sum_holding_time": None,
                    "min_holding_time": None,
                    "max_holding_time": None,
                    "sum_handling_time": None,
                    "average_handling_time": None,
                    "average_ringing_time": None,
                    "average_holding_time": None,
                    "average_speaking_time": None,
                    "projectName": self.project_names.get(self.project_name) if self.project_name else None,
                })

        return {
            "labels": [
                {"field": "date", "title": "Дата"},
                {"field": "quantity", "title": "Кол-во вызовов"},
                {"field": "sum_pickup_time", "title": "sum_pickup_time"},
                {"field": "min_pickup_time", "title": "min_pickup_time"},
                {"field": "max_pickup_time", "title": "max_pickup_time"},
                {"field": "sum_speaking_time", "title": "sum_speaking_time"},
                {"field": "min_speaking_time", "title": "min_speaking_time"},
                {"field": "max_speaking_time", "title": "max_speaking_time"},
                {"field": "sum_wrapup_time", "title": "sum_wrapup_time"},
                {"field": "min_wrapup_time", "title": "min_wrapup_time"},
                {"field": "max_wrapup_time", "title": "max_wrapup_time"},
                {"field": "quantity_hold", "title": "quantity_hold"},
                {"field": "sum_holding_time", "title": "sum_holding_time"},
                {"field": "min_holding_time", "title": "min_holding_time"},
                {"field": "max_holding_time", "title": "max_holding_time"},
                {"field": "average_handling_time", "title": "Average Handling Time"},
                {"field": "average_ringing_time", "title": "Average Ringing Time"},
                {"field": "average_holding_time", "title": "Average Holding Time"},
                {"field": "average_speaking_time", "title": "Average Speaking Time"},
            ],
            "items": items,
        }

    def get_aggregated_data_detailed(self) -> List[Dict[str, Any]]:
        """
        AHT по периодам и по проектам (Money/Bank → O!Bank),
        плюс дорисовка пустых интервалов.
        """
        self._validate_trunc_and_format()
        where_projects_sql, params_projects = self._selected_projects_clause()

        base_sql_path = getattr(self, "base_sql_path", Path("sql"))
        sql_path = base_sql_path / "naumen" / "get_aggregated_data_detailed.sql"
        sql = sql_path.read_text(encoding="utf-8")
        sql = sql.replace(
            "/*WHERE_PROJECTS_CLAUSE*/",
            f"\n  {where_projects_sql}\n" if where_projects_sql else ""
        )
        params: Tuple[Any, ...] = (self.trunc, self.date_format, self.start_date, self.end_date, *params_projects)
        rows = self._fetchall(sql, params)
        by_key: Dict[str, List[Dict[str, Any]]] = {}
        for r in rows:
            by_key.setdefault(r["date"], []).append(r)

        items: List[Dict[str, Any]] = []
        for key in self.period:
            date_out, hour_out = self._key_to_outputs(key)
            found = by_key.get(key)
            if found:
                for r in found:
                    items.append({
                        "date": date_out,
                        "hour": hour_out,
                        "AHT": r["average_handling_time"],
                        "project_key": r["project_key"],
                    })
            else:
                items.append({
                    "date": date_out,
                    "hour": hour_out,
                    "AHT": None,
                    "project_key": None,
                })
        return items

    def get_aggregated_data_filtered_group(self) -> List[Dict[str, Any]]:
        """
        Полный набор метрик по периодам в разрезе group (project),
        плюс дорисовка пустых интервалов.
        """
        self._validate_trunc_and_format()
        where_projects_sql, params_projects = self._selected_projects_clause()

        base_sql_path = getattr(self, "base_sql_path", Path("sql"))
        sql_path = base_sql_path / "naumen" / "get_aggregated_data_filtered_group.sql"
        sql = sql_path.read_text(encoding="utf-8")
        sql = sql.replace(
            "/*WHERE_PROJECTS_CLAUSE*/",
            f"\n  {where_projects_sql}\n" if where_projects_sql else ""
        )
        params: Tuple[Any, ...] = (self.trunc, self.date_format, self.start_date, self.end_date, *params_projects)
        rows = self._fetchall(sql, params)
        by_key: Dict[str, List[Dict[str, Any]]] = {}
        for r in rows:
            by_key.setdefault(r["date"], []).append(r)

        items: List[Dict[str, Any]] = []
        for key in self.period:
            date_out, hour_out = self._key_to_outputs(key)
            found = by_key.get(key)
            if found:
                for r in found:
                    items.append({
                        "date": date_out,
                        "hour": hour_out,
                        "quantity": r["quantity"],
                        "sum_pickup_time": r["sum_pickup_time"],
                        "min_pickup_time": r["min_pickup_time"],
                        "max_pickup_time": r["max_pickup_time"],
                        "sum_speaking_time": r["sum_speaking_time"],
                        "min_speaking_time": r["min_speaking_time"],
                        "max_speaking_time": r["max_speaking_time"],
                        "sum_wrapup_time": r["sum_wrapup_time"],
                        "min_wrapup_time": r["min_wrapup_time"],
                        "max_wrapup_time": r["max_wrapup_time"],
                        "quantity_hold": r["quantity_hold"],
                        "sum_holding_time": r["sum_holding_time"],
                        "min_holding_time": r["min_holding_time"],
                        "max_holding_time": r["max_holding_time"],
                        "sum_handling_time": r["sum_handling_time"],
                        "average_handling_time": r["average_handling_time"],
                        "average_ringing_time": r["average_ringing_time"],
                        "average_holding_time": r["average_holding_time"],
                        "average_speaking_time": r["average_speaking_time"],
                        "projectName": r["project"],
                    })
            else:
                items.append({
                    "date": date_out,
                    "hour": hour_out,
                    "quantity": None,
                    "sum_pickup_time": None,
                    "min_pickup_time": None,
                    "max_pickup_time": None,
                    "sum_speaking_time": None,
                    "min_speaking_time": None,
                    "max_speaking_time": None,
                    "sum_wrapup_time": None,
                    "min_wrapup_time": None,
                    "max_wrapup_time": None,
                    "quantity_hold": None,
                    "sum_holding_time": None,
                    "min_holding_time": None,
                    "max_holding_time": None,
                    "sum_handling_time": None,
                    "average_handling_time": None,
                    "average_ringing_time": None,
                    "average_holding_time": None,
                    "average_speaking_time": None,
                    "projectName": None,
                })
        return items

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

    def set_hours_trunked_data(self) -> None:
        """
        Настройка агрегации по часам.
        """
        self.trunc = "hour"
        self.period = self.get_dates_from_range(
            self.start_date,
            self.end_date,
            interval="PT1H",  # шаг = 1 час
            fmt="%Y-%m-%d %H:%M:%S"  # формат вывода
        )
        self.date_format = "YYYY-MM-DD HH24:MI:SS"

    def set_days_trunked_data(self) -> None:
        """
        Настройка агрегации по дням.
        """
        self.trunc = "day"
        self.period = self.get_dates_from_range(
            self.start_date,
            self.end_date,
            interval="P1D",  # шаг = 1 день
            fmt="%Y-%m-%d %H:%M:%S"  # формат вывода
        )
        self.date_format = "YYYY-MM-DD HH24:MI:SS"

    def set_months_trunked_data(self) -> None:
        """
        Настройка агрегации по месяцам.
        """
        self.trunc = "month"
        self.period = self.get_dates_from_range(
            self.start_date,
            self.end_date,
            interval="P1M",  # шаг = 1 месяц
            fmt="%Y-%m-01"  # формат: первый день месяца
        )
        self.date_format = "YYYY-MM-DD"