import datetime as dt
from pathlib import Path
from typing import List
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

import pandas as pd

from utils.array_operations import _parse_period
from utils.path import BASE_SQL_PATH
from utils.transformers.mp_report import try_parse_period


class TaskSolvingSpeedDataGetter:
    def __init__(self,
                 start_date,
                 end_date,
                 bpm_conn,
                 trunc='day'):
        self.start_date = start_date
        self.end_date = end_date
        self.trunc = trunc
        self.bpm_conn = bpm_conn
        self.base_sql_path = Path(BASE_SQL_PATH)

        if self.trunc == "day":
            self.date_format = "YYYY-MM-DD HH24:MI:SS"
        elif self.trunc == "hour":
            self.date_format = "YYYY-MM-DD HH:MI:SS"
        elif self.trunc == "month":
            self.date_format = "YYYY-MM-DD"
        else:
            raise ValueError("trunc must be 'hour' | 'day' | 'month'")
        self.period = []
        self._rebuild_period()

    def _rebuild_period(self):
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
            # ключ 'YYYY-MM-DD 00:00:00'
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

    def get_tasks_sla_data(self, data):
        """
        Аналог PHP getTasksSLAData($data).
        Добавляет поле 'sla' в каждую запись.
        """
        out = []
        for item in data:
            sla_reached = int(item.get("sla_reached", 0) or 0)
            sla_not_reached = int(item.get("sla_not_reached", 0) or 0)

            if sla_reached == 0 and sla_not_reached == 0:
                sla = None
            elif sla_reached == 0:
                sla = 0
            elif sla_not_reached == 0:
                sla = 100
            else:
                sla = sla_reached / (sla_reached + sla_not_reached) * 100

            # добавляем новое поле
            new_item = dict(item)
            new_item["sla"] = sla
            out.append(new_item)

        return out

    def get_data_by_staff_unit_id(
        self, start_date: str, end_date: str, staff_unit_id: int
    ):
        """
        Аналог PHP getDataByStaffUnitID.
        Берёт SLA-данные по staff_unit_id за период.
        """

        sql_path = self.base_sql_path / "bpm" / "tasks_sla_data.sql"
        query = sql_path.read_text(encoding="utf-8")

        params = {
            "start_date": start_date,
            "end_date": end_date,
            "staff_unit_id": staff_unit_id,
        }

        df = pd.read_sql_query(query, self.bpm_conn, params=params)

        kpi_data_old = self.get_tasks_sla_data(df.to_dict("records"))

        if not kpi_data_old:
            return {}

        return kpi_data_old[0]

    def get_data(self, start_date: str, end_date: str):
        sql_path = self.base_sql_path / "bpm" / "tasks_sla_data_all.sql"
        query = sql_path.read_text(encoding="utf-8")

        params = {
            "start_date": start_date,
            "end_date": end_date,
        }

        df = pd.read_sql_query(query, self.bpm_conn, params=params)

        kpi_data_old = self.get_tasks_sla_data(df.to_dict("records"))

        if not kpi_data_old:
            return {}

        return kpi_data_old[0]

    def get_closed_tasks_data_general_dynamic(self, group: str | None = None) -> dict:
        sql_path = self.base_sql_path / "bpm" / "closed_tasks_data_general_dynamic.sql"
        sql_template = sql_path.read_text(encoding="utf-8")

        group_filter = "AND data->'project'->>'group_title' = %(group)s" if group else ""
        query = sql_template.format(group_filter=group_filter)

        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
        }
        if group:
            params["group"] = group

        df = pd.read_sql_query(query, self.bpm_conn, params=params)

        data = self.get_tasks_sla_data(df.to_dict("records"))

        items = []
        for period in self.period:
            dt = _parse_period(period)
            matches = [row for row in data if row["date"] == period]

            if matches:
                for row in matches:
                    items.append({
                        "date": dt.strftime("%d.%m.%Y"),
                        "hour": dt.strftime("%H:%M:%S"),
                        "total": row.get("total"),
                        "sla_reached": row.get("sla_reached"),
                        "sla_not_reached": row.get("sla_not_reached"),
                        "sla": row.get("sla"),
                    })
            else:
                items.append({
                    "date": dt.strftime("%d.%m.%Y"),
                    "hour": dt.strftime("%H:%M:%S"),
                    "total": None,
                    "sla_reached": None,
                    "sla_not_reached": None,
                    "sla": None,
                })

        return {
            "labels": [
                {"field": "date", "title": "Дата"},
                {"field": "hour", "title": "Час"},
                {"field": "total", "title": "Всего"},
                {"field": "sla_reached", "title": "Выполнено в срок"},
                {"field": "sla_not_reached", "title": "Выполнено не в срок"},
                {"field": "sla", "title": "Процент выполненных в срок"},
            ],
            "items": items,
        }


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
            interval="PT1H",         # шаг = 1 час
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
            interval="P1D",          # шаг = 1 день
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
            fmt="%Y-%m-01"   # формат: первый день месяца
        )
        self.date_format = "YYYY-MM-DD"
