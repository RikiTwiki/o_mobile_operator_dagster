from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Sequence

from dagster import get_dagster_logger
from dateutil.relativedelta import relativedelta

from resources import source_dwh_resource

import os

from utils.path import BASE_SQL_PATH


class RepeatCallsDataGetter:

    # Публичные поля (совместимость с PHP-версией)
    StartDate: str
    EndDate: str
    SetDateFormat: bool
    ProjectName: str
    ProjectNames: Dict[str, str]

    # Внутренние настройки
    _trunc: str
    _date_format: str
    _period: List[str]

    def __init__(
        self,
        start_date: str,
        end_date: str,
        table: str = "stat.replication_repeat_calls_data",
        project_name: str = "",
        project_names: Optional[Dict[str, str]] = None,
        set_date_format: bool = False,
        dwh_conn = source_dwh_resource,
    ) -> None:
        self.dwh_conn = dwh_conn
        self.table = table

        self.StartDate = start_date
        self.EndDate = end_date
        self.SetDateFormat = set_date_format
        self.ProjectName = project_name
        self.ProjectNames = project_names or {}

        self.base_sql_path = BASE_SQL_PATH

        # По умолчанию — дневная агрегация, Period сформируем через set_days_trunked_data()
        self._trunc = "day"
        self._date_format = "YYYY-MM-DD HH24:MI:SS"
        self._period = []

    # --------------------
    # Вспомогательные
    # --------------------

    def _read_sql(self, filename: str, conn: str = "None") -> str:

        if conn == "bpm":
            path = os.path.join(self.base_sql_path, "bpm", filename)
            with open(path, 'r', encoding="utf-8") as f:
                return f.read()

        path = os.path.join(self.base_sql_path, "naumen", filename)
        with open(path, 'r', encoding="utf-8") as f:
            return f.read()

    def _fetch_all(self, sql: str, params: Sequence[Any]) -> List[Dict[str, Any]]:
        with self.dwh_conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    def _fetch_one(self, sql: str, params: Sequence[Any]) -> Optional[Dict[str, Any]]:
        with self.dwh_conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            if not row:
                return None
            cols = [c[0] for c in cur.description]
            return dict(zip(cols, row))

    def _get_staff_unit_start_time(self, staff_unit_id: int, day_iso: str) -> str:
        if not self.dwh_conn:
            return "00:00:00"
        sql = """
              SELECT to_char(ts.start, 'HH24:MI:SS') AS start_time
              FROM bpm.timetable_data_detailed tdd
                       JOIN bpm.timetable_shifts ts ON ts.id = tdd.timetable_shift_id
              WHERE tdd.staff_unit_id = %s
                AND date_trunc('day', tdd.session_start)::date = %s:: date
              ORDER BY tdd.session_start ASC
                  LIMIT 1 \
              """
        with self.dwh_conn.cursor() as cur:
            cur.execute(sql, [staff_unit_id, day_iso])
            row = cur.fetchone()
        return row[0] if row and row[0] else "00:00:00"

    @staticmethod
    def _hour_from_period(period_str: str) -> str:
        return period_str[-8:] if len(period_str) >= 8 else "00:00:00"

    def _fmt_out_date(self, period_str: str, iso_only: bool = False) -> str:
        if self.SetDateFormat or iso_only:
            if self._trunc == "month":  # для месячной агрегации оставляем YYYY-MM-DD
                return period_str[:10]
            return period_str
        # иначе d.m.Y
        try:
            dt = datetime.strptime(period_str, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            dt = datetime.strptime(period_str[:10], "%Y-%m-%d")
        return dt.strftime("%d.%m.%Y")

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

    # --------------------
    # Порты методов из PHP (snake_case)
    # --------------------

    def get_repeat_calls_by_staff_unit_id(
        self,
        staff_unit_id: int,
        start_date: str,
        end_date: str,
        groups: Sequence[str] = ("General",),
    ) -> Dict[str, Any]:
        start_time = self._get_staff_unit_start_time(staff_unit_id, start_date)
        sql = f"""
            SELECT
              user_login,
              staff_unit_id,
              SUM(CASE WHEN resolve_status = 'resolved' THEN 1 ELSE 0 END) AS resolved,
              SUM(CASE WHEN resolve_status = 'not_resolved' THEN 1 ELSE 0 END) AS not_resolved
            FROM {self.table}
            WHERE start_time = %s
              AND repeat_calls_period_date >= %s
              AND repeat_calls_period_date < %s
              AND "group" = ANY(%s)
              AND staff_unit_id = %s
            GROUP BY 1,2
        """
        row = self._fetch_one(sql, [start_time, start_date, end_date, list(groups), staff_unit_id])
        if not row:
            raise Exception(f"Нет данных по RepeatCallsData у сотрудника с ID {staff_unit_id}")
        resolved = int(row.get("resolved") or 0)
        not_resolved = int(row.get("not_resolved") or 0)
        total = resolved + not_resolved
        repeat_calls = (not_resolved / total * 100.0) if total else 0.0
        return {
            "staff_unit_id": row["staff_unit_id"],
            "user_login": row["user_login"],
            "repeat_calls": repeat_calls,
            "resolved": resolved,
            "not_resolved": not_resolved,
            "start_time": start_time,
        }

    def get_repeat_calls_construct(
        self,
        start_date: str,
        end_date: str,
        groups: Sequence[str] = ("General",),
        excluded_dates: Sequence[str] = (),
    ) -> List[Dict[str, Any]]:
        sql = [
            f"SELECT user_login, staff_unit_id, start_time,",
            "SUM(CASE WHEN resolve_status = 'resolved' THEN 1 ELSE 0 END) AS resolved,",
            "SUM(CASE WHEN resolve_status = 'not_resolved' THEN 1 ELSE 0 END) AS not_resolved",
            f"FROM {self.table}",
            "WHERE repeat_calls_period_date >= %s",
            "  AND repeat_calls_period_date < %s",
            "  AND \"group\" = ANY(%s)",
        ]
        params: List[Any] = [start_date, end_date, list(groups)]
        if excluded_dates:
            placeholders = ",".join(["%s"] * len(excluded_dates))
            sql.append(
                f"  AND date_trunc('day', repeat_calls_period_date)::date NOT IN ({placeholders})"
            )
            params.extend(list(excluded_dates))
        sql.append("GROUP BY 1,2,3")
        sql_str = "\n".join(sql)
        return self._fetch_all(sql_str, params)

    def get_repeat_calls_by_unit_id(
        self,
        staff_unit_id: int,
        start_date: str,
        end_date: str,
        groups: Sequence[str] = ("General",),
        excluded_dates: Sequence[str] = (),
    ) -> List[Dict[str, Any]]:
        sql = [
            f"SELECT user_login, staff_unit_id, start_time,",
            "SUM(CASE WHEN resolve_status = 'resolved' THEN 1 ELSE 0 END) AS resolved,",
            "SUM(CASE WHEN resolve_status = 'not_resolved' THEN 1 ELSE 0 END) AS not_resolved",
            f"FROM {self.table}",
            "WHERE repeat_calls_period_date >= %s",
            "  AND repeat_calls_period_date < %s",
            "  AND staff_unit_id = %s",
            "  AND \"group\" = ANY(%s)",
        ]
        params: List[Any] = [start_date, end_date, staff_unit_id, list(groups)]
        if excluded_dates:
            placeholders = ",".join(["%s"] * len(excluded_dates))
            sql.append(
                f"  AND date_trunc('day', repeat_calls_period_date)::date NOT IN ({placeholders})"
            )
            params.extend(list(excluded_dates))
        sql.append("GROUP BY 1,2,3")
        return self._fetch_all("\n".join(sql), params)

    def get_repeat_calls(
        self,
        start_date: str,
        end_date: str,
        groups: Sequence[str] = ("General",),
        excluded_dates: Sequence[str] = (),
    ) -> Dict[str, Any]:
        try:
            sql = [
                f"SELECT",
                "  SUM(CASE WHEN resolve_status = 'resolved' THEN 1 ELSE 0 END) AS resolved,",
                "  SUM(CASE WHEN resolve_status = 'not_resolved' THEN 1 ELSE 0 END) AS not_resolved",
                f"FROM {self.table}",
                "WHERE start_time = %s",
                "  AND repeat_calls_period_date >= %s",
                "  AND repeat_calls_period_date < %s",
                "  AND \"group\" = ANY(%s)",
            ]
            params: List[Any] = ["00:00:00", start_date, end_date, list(groups)]
            if excluded_dates:
                placeholders = ",".join(["%s"] * len(excluded_dates))
                sql.append(
                    f"  AND date_trunc('day', repeat_calls_period_date)::date NOT IN ({placeholders})"
                )
                params.extend(list(excluded_dates))
            row = self._fetch_one("\n".join(sql), params) or {}
            resolved = int(row.get("resolved") or 0)
            not_resolved = int(row.get("not_resolved") or 0)
            total = resolved + not_resolved
            repeat_calls = (not_resolved / total * 100.0) if total else 0.0
            return {
                "repeat_calls": repeat_calls,
                "resolved": resolved,
                "not_resolved": not_resolved,
                "start_time": "00:00:00",
            }
        except Exception as e:
            return {"error": "Ошибка в логике подсчета", "detail": str(e)}

    def get_aggregated_data(
            self,
            table: str = "00:00:00",
            groups: Sequence[str] = ("General",),
    ) -> Dict[str, Any]:
        # читаем SQL-шаблон и подставляем имя таблицы
        sql_tmpl = self._read_sql("repeat_calls_aggregated.sql")  # sql/naumen/...
        query = sql_tmpl.format(table=self.table)

        params = {
            "trunc": self._trunc,  # 'day' | 'hour'
            "date_format": self._date_format,  # 'YYYY-MM-DD HH24:MI:SS' и т.п.
            "start_time": table,  # "00:00:00" и т.д.
            "start_date": self.StartDate,
            "end_date": self.EndDate,
            "groups": list(groups),
        }

        get_dagster_logger().info(f"get_aggregated_data: {query}")
        get_dagster_logger().info(f"params = {params}")
        rows = self._fetch_all(query, params) or []
        by_date = {r["date"]: r for r in rows}

        items: List[Dict[str, Any]] = []
        for period in self._period:
            r = by_date.get(period)
            if r:
                resolved = int(r.get("resolved") or 0)
                not_resolved = int(r.get("not_resolved") or 0)
                total = resolved + not_resolved
                rc = (not_resolved / total * 100.0) if total else 0.0
                items.append({
                    "date": self._fmt_out_date(period),
                    "hour": self._hour_from_period(period),
                    "resolved": resolved,
                    "not_resolved": not_resolved,
                    "repeat_calls": rc,
                    "projectName": self.ProjectNames.get(self.ProjectName) if self.ProjectName else None,
                })
            else:
                items.append({
                    "date": self._fmt_out_date(period),
                    "hour": self._hour_from_period(period),
                    "resolved": None,
                    "not_resolved": None,
                    "repeat_calls": None,
                    "projectName": self.ProjectNames.get(self.ProjectName) if self.ProjectName else None,
                })

        return {
            "labels": [
                {"field": "date", "title": "Дата"},
                {"field": "resolved", "title": "resolved"},
                {"field": "not_resolved", "title": "not_resolved"},
                {"field": "repeat_calls", "title": "repeat_calls"},
            ],
            "items": items,
        }

    def get_aggregated_data_detailed(
        self,
        table: str = "00:00:00",
        groups: Sequence[str] = ("General",),
    ) -> List[Dict[str, Any]]:
        sql = [
            "SELECT",
            "  to_char(date_trunc(%s, repeat_calls_period_date), %s) AS date,",
            "  \"group\" AS project_key,",
            "  SUM(CASE WHEN resolve_status = 'resolved' THEN 1 ELSE 0 END) AS resolved,",
            "  SUM(CASE WHEN resolve_status = 'not_resolved' THEN 1 ELSE 0 END) AS not_resolved",
            f"FROM {self.table}",
            "WHERE start_time = %s",
            "  AND repeat_calls_period_date >= %s",
            "  AND repeat_calls_period_date < %s",
            "  AND \"group\" = ANY(%s)",
            "GROUP BY 1,2",
        ]
        rows = self._fetch_all("\n".join(sql), [self._trunc, self._date_format, table, self.StartDate, self.EndDate, list(groups)])

        by_date: Dict[str, List[Dict[str, Any]]] = {}
        for r in rows:
            by_date.setdefault(r["date"], []).append(r)

        items: List[Dict[str, Any]] = []
        for period in self._period:
            lst = by_date.get(period)
            if lst:
                for r in lst:
                    resolved = int(r.get("resolved") or 0)
                    not_resolved = int(r.get("not_resolved") or 0)
                    total = resolved + not_resolved
                    rc = (not_resolved / total * 100.0) if total else 0.0
                    items.append({
                        "date": self._fmt_out_date(period),
                        "repeat_calls": rc,
                        "projectName": r["project_key"],
                    })
            else:
                items.append({
                    "date": self._fmt_out_date(period),
                    "hour": self._hour_from_period(period),
                    "repeat_calls": None,
                    "projectName": None,
                })
        return items

    def get_aggregated_data_filtered(
            self,
            table: str = "00:00:00",
            groups: Sequence[str] = ("General",),
    ) -> List[Dict[str, Any]]:
        # читаем SQL-шаблон и подставляем фактическое имя таблицы
        sql_tmpl = self._read_sql("repeat_calls_aggregated_filtered.sql")  # sql/naumen/...
        query = sql_tmpl.format(table=self.table)

        params = {
            "trunc": self._trunc,  # 'day' | 'hour'
            "date_format": self._date_format,  # 'YYYY-MM-DD HH24:MI:SS' и т.п.
            "start_time": table,
            "start_date": self.StartDate,
            "end_date": self.EndDate,
            "groups": list(groups),
        }

        rows = self._fetch_all(query, params) or []

        by_date: Dict[str, List[Dict[str, Any]]] = {}
        for r in rows:
            by_date.setdefault(r["date"], []).append(r)

        items: List[Dict[str, Any]] = []
        for period in self._period:
            for r in by_date.get(period, []):
                resolved = int(r.get("resolved") or 0)
                not_resolved = int(r.get("not_resolved") or 0)
                total = resolved + not_resolved
                rc = (not_resolved / total * 100.0) if total else 0.0
                items.append({
                    "date": self._fmt_out_date(period, iso_only=True)[:10],  # 'YYYY-MM-DD'
                    "repeat_calls": rc,
                    "projectName": r["project_key"],
                })

        return items

    # --------------------
    # Настройка агрегации (как в примере — и алиасы под PHP)
    # --------------------

    def set_hours_trunked_data(self) -> None:
        """
        Настройка агрегации по часам.
        """
        self._trunc = "hour"
        self._period = self.get_dates_from_range(
            self.StartDate,
            self.EndDate,
            interval="PT1H",  # шаг = 1 час
            fmt="%Y-%m-%d %H:%M:%S"  # формат вывода
        )
        self._date_format = "YYYY-MM-DD HH24:MI:SS"

    def set_days_trunked_data(self) -> None:
        """
        Настройка агрегации по дням.
        """
        self._trunc = "day"
        self._period = self.get_dates_from_range(
            self.StartDate,
            self.EndDate,
            interval="P1D",  # шаг = 1 день
            fmt="%Y-%m-%d %H:%M:%S"  # формат вывода
        )
        self._date_format = "YYYY-MM-DD HH24:MI:SS"

    def set_months_trunked_data(self) -> None:
        """
        Настройка агрегации по месяцам.
        """
        self._trunc = "month"
        self._period = self.get_dates_from_range(
            self.StartDate,
            self.EndDate,
            interval="P1M",  # шаг = 1 месяц
            fmt="%Y-%m-01"  # формат: первый день месяца
        )
        self._date_format = "YYYY-MM-DD"
