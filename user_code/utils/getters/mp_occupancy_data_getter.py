from __future__ import annotations
from pathlib import Path
from typing import Optional, List, Union
from datetime import datetime, timedelta
from typing import Union
import re, traceback
from dateutil.relativedelta import relativedelta
import pandas as pd
from dagster import get_dagster_logger
from sqlalchemy.orm import Session
from typing import List, Dict, Any

import psycopg2
from utils.getters.naumen_projects_getter import NaumenProjectsGetter

from utils.models.staff_unit import StaffUnit
from utils.path import BASE_SQL_PATH


class MPOccupancyDataGetter(NaumenProjectsGetter):
    def __init__(
        self,
        trunc: str,
        conn: psycopg2.extensions.connection,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        report_date: datetime | None = None,
        date_format: Optional[str] = None,
        month_start: datetime | None = None,
        pst_positions: Optional[List[int]] = None,
        ):

        super().__init__()
        self.trunc = trunc
        self.start_date = start_date
        self.end_date = end_date
        self.report_date = report_date
        self.date_format = date_format
        self.month_start = month_start
        self.pst_positions = pst_positions or [10, 41, 46]

        self.conn = conn
        self.base_sql_path = Path(BASE_SQL_PATH)

    def get_occupancy_by_staff_unit_id(self, staff_unit_id: int, session: Session) -> Dict[str, Any]:
        """
        :param staff_unit_id: ID штатной единицы
        :param session: SQLAlchemy Session
        :return: словарь с суммами по кодам и product_time
        """
        # 1) Находим штатную единицу
        unit = session.get(StaffUnit, staff_unit_id)
        if unit is None:
            raise LookupError(f"StaffUnit(id={staff_unit_id}) not found")
        login = unit.login

        # 2) Берём исходные данные по логину
        data = self.get_main_db_data([login])

        # 3) Аккумуляторы
        product_time = 0
        normal = 0
        eshop = 0
        social_media = 0
        pre_coffee = 0
        coffee = 0

        # 4) Суммируем по правилам как в PHP
        for item in data:
            code = item.get("code")
            duration = item.get("duration") or 0

            if code == "normal":
                product_time += duration
                normal += duration
            if code == "social_media":
                product_time += duration
                social_media += duration
            if code == "eshop":
                product_time += duration
                eshop += duration
            if code == "pre_coffee":
                product_time += duration
                pre_coffee += duration
            if code == "coffee":
                coffee += duration

        # 5) Возвращаем результат с теми же ключами
        return {
            "staff_unit_id": staff_unit_id,
            "user_login": login,
            "product_time": product_time,
            "normal": normal,
            "eshop": eshop,
            "socialMedia": social_media,
            "pre_coffee": pre_coffee,
            "coffee": coffee,
        }

    def get_daily_occupancy_by_staff_unit_id_without_date(
        self,
        staff_unit_id: int,
        session: Session,
    ) -> Dict[str, float]:
        """
        Возвращает словарь вида:
        {
          "productTime": <сумма по продуктивным кодам>,
          "<code1>": <сумма>,
          "<code2>": <сумма>,
          ...
        }
        """
        # 1) Находим штатную единицу и берём её login
        unit = session.get(StaffUnit, staff_unit_id)
        if unit is None:
            raise LookupError(f"StaffUnit(id={staff_unit_id}) not found")
        login = unit.login

        # 2) Берём данные без привязки к датам (как в PHP: getDataWithoutDate([$login]))
        data = self.get_data_without_date([login])

        # 3) Аккумулятор: productTime всегда есть
        daily_data: Dict[str, float] = {"productTime": 0.0}

        # 4) Суммируем
        for item in data:
            code = item.get('code')
            if not code:
                continue
            duration = float(item.get('duration') or 0)

            if code in ['normal', 'social_media', 'eshop', 'pre_coffee']:
                daily_data["productTime"] += duration

            daily_data[code] = daily_data.get(code, 0.0) + duration

        return daily_data

    def get_main_db_data(self, logins):
        sql_path = self.base_sql_path / "mp" / "get_main_db_data.sql"
        query = sql_path.read_text(encoding="utf-8")

        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "logins": list(logins),
        }
        df = pd.read_sql_query(query, self.conn, params=params)
        get_dagster_logger().info(f"get_main_db_data: {df.to_dict(orient='records')}")
        return df.to_dict(orient="records")

    def get_data_without_date(self, logins):
        sql_path = self.base_sql_path / "mp" / "get_data_without_date.sql"
        query = sql_path.read_text(encoding="utf-8")

        params = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "logins": list(logins),
        }
        df = pd.read_sql_query(query, self.conn, params=params)
        get_dagster_logger().info(f"get_data_without_date: {df.to_dict(orient='records')}")
        return df.to_dict(orient="records")

    def get_user_statuses_by_positions(self, logins):
        if not logins:
            return []

        sql_path = self.base_sql_path / "mp" / "get_user_statuses_by_positions.sql"
        query = sql_path.read_text(encoding="utf-8")

        params = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "logins": list(logins),
        }

        df = pd.read_sql_query(query, self.conn, params=params)
        get_dagster_logger().info(
            "get_user_statuses_by_positions: %s",
            df.to_dict(orient="records"),
        )
        return df.to_dict(orient="records")

    def get_active_user_logins(self, excluded_shifts = None):
        sql_path = self.base_sql_path / "mp" / "get_active_user_logins.sql"
        sql_template = sql_path.read_text(encoding="utf-8")

        if excluded_shifts:  # True только для непустых списков
            excluded_shifts_filter = "AND tdd.timetable_shift_id = ANY(%(excluded_shifts)s::int[])"
        else:
            excluded_shifts_filter = ""

        query = sql_template.format(
            excluded_shifts_filter=excluded_shifts_filter
        )

        params = {
            "month_start" : self.month_start,
            "start_date" : self.start_date,
            "end_date" : self.end_date,
            "position_ids": self.pst_positions,
        }

        if excluded_shifts:
            params["excluded_shifts"] = list(excluded_shifts)

        df = pd.read_sql_query(query, self.conn, params=params)
        get_dagster_logger().info(f"get_active_user_logins: {df.to_dict(orient='records')}")
        return df.to_dict(orient="records")

    def get_active_user_sheets(self):
        sql_path = self.base_sql_path / "mp" / "get_active_user_sheets.sql"
        query = sql_path.read_text(encoding="utf-8")
        params = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "position_ids": self.pst_positions,
            "month_start": self.month_start,
        }

        df = pd.read_sql_query(query, self.conn, params=params)
        get_dagster_logger().info(f"get_active_user_sheets: {df.to_dict(orient='records')}")
        return df.to_dict(orient="records")

    def get_sheet_statuses_by_positions(
            self,
            data,
    ) -> List[Dict[str, Any]]:
        """
        Для каждого элемента data:
          - выставляет self.start_date / self.end_date из session_start / session_end
          - вызывает self.set_months_trunked_data() (аналог setMonthsTrunkedData)
          - тянет статусы через get_statuses_duration_without_date(...)
          - накапливает все строки в один список и возвращает его
        """
        accum: List[Dict[str, Any]] = []

        for item in data:
            session_start = item.get("session_start") if isinstance(item, dict) else getattr(item, "session_start",
                                                                                             None)
            session_end = item.get("session_end") if isinstance(item, dict) else getattr(item, "session_end", None)
            login = item.get("login") if isinstance(item, dict) else getattr(item, "login", None)
            date_ = item.get("date") if isinstance(item, dict) else getattr(item, "date", None)

            self.start_date = session_start
            self.end_date = session_end
            self.set_months_trunked_data()

            if not login:
                continue

            statuses = self.get_statuses_duration_without_date(login, date_)

            if statuses:
                accum.extend(statuses)

        return accum

    def get_statuses_duration_without_date(self, login, date_):
        sql_path = self.base_sql_path / "mp" / "get_statuses_duration_without_date.sql"
        query = sql_path.read_text(encoding="utf-8")

        params = {
            "date": date_,
            "login": login,
        }

        df = pd.read_sql_query(query, self.conn, params=params)
        get_dagster_logger().info(
            "get_statuses_duration_without_date(login=%s, date=%s): %s",
            login,
            date_,
            df.to_dict(orient="records"),
        )
        return df.to_dict(orient="records")

    @staticmethod
    def _phpfmt_to_strftime(fmt: str) -> str:
        return (fmt
                .replace('Y', '%Y')
                .replace('m', '%m')
                .replace('d', '%d')
                .replace('H', '%H')
                .replace('i', '%M')
                .replace('s', '%S'))

    @staticmethod
    def _parse_dt(x: Union[str, datetime]) -> datetime:
        if isinstance(x, datetime):
            return x
        s = x.strip()
        # ISO 8601 с возможным Z/offset
        try:
            return datetime.fromisoformat(s.replace('Z', '+00:00')).replace(tzinfo=None)
        except Exception:
            pass
        for f in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d'):
            try:
                return datetime.strptime(s, f)
            except ValueError:
                continue
        raise ValueError(f"Unrecognized datetime format: {x!r}")

    _ISO_DUR_RE = re.compile(
        r'^P'
        r'(?:(?P<years>\d+)Y)?'
        r'(?:(?P<months>\d+)M)?'
        r'(?:(?P<weeks>\d+)W)?'
        r'(?:(?P<days>\d+)D)?'
        r'(?:T'
        r'(?:(?P<hours>\d+)H)?'
        r'(?:(?P<minutes>\d+)M)?'
        r'(?:(?P<seconds>\d+)S)?'
        r')?$'
    )

    @classmethod
    def _parse_iso_duration(cls, dur: str) -> Union[timedelta, relativedelta]:
        m = cls._ISO_DUR_RE.match(dur)
        if not m:
            raise ValueError(f"Invalid ISO 8601 duration: {dur!r}")
        p = {k: int(v) if v else 0 for k, v in m.groupdict().items()}
        if p['years'] or p['months']:
            return relativedelta(years=p['years'], months=p['months'],
                                 weeks=p['weeks'], days=p['days'],
                                 hours=p['hours'], minutes=p['minutes'], seconds=p['seconds'])
        return timedelta(days=p['weeks'] * 7 + p['days'],
                         hours=p['hours'], minutes=p['minutes'], seconds=p['seconds'])

    def get_dates_from_range(
        self,
        start_date: Union[str, datetime],
        end_date: Union[str, datetime],
        interval: str = 'P1M',
        fmt: str = 'Y-m-d H:i:s',
    ) -> List[str]:
        """
        Аналог PHP DatePeriod(start, interval, end): конец диапазона исключён.
        Возвращает список строк, отформатированных в PHP-нотации fmt.
        При ошибке — список строк трейсбэка (как e->getTrace()).
        """
        try:
            start = self._parse_dt(start_date)
            end   = self._parse_dt(end_date)
            if end <= start:
                return []

            step = self._parse_iso_duration(interval)
            py_fmt = self._phpfmt_to_strftime(fmt) if '%' not in fmt else fmt

            out: List[str] = []
            cur = start
            while cur < end:
                out.append(cur.strftime(py_fmt))
                cur = cur + step
            return out
        except Exception:
            return traceback.format_exc().strip().splitlines()

    def set_hours_trunked_data(self) -> None:
        self.trunc = 'hour'
        self.period = self.get_dates_from_range(self.start_date, self.end_date, 'PT1H', 'Y-m-d H:i:s')
        self.date_format = 'YYYY-MM-DD HH24:MI:SS'

    def set_days_trunked_data(self) -> None:
        self.trunc = 'day'
        self.period = self.get_dates_from_range(self.start_date, self.end_date, 'P1D', 'Y-m-d H:i:s')
        self.date_format = 'YYYY-MM-DD HH24:MI:SS'

    def set_months_trunked_data(self) -> None:
        self.trunc = 'month'
        self.period = self.get_dates_from_range(self.start_date, self.end_date, 'P1M', 'Y-m-01')
        self.date_format = 'YYYY-MM-DD'

