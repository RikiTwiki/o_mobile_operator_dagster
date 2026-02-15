import json
from datetime import datetime, time, date, timedelta
from pathlib import Path
from typing import Dict
from datetime import datetime, date as date_cls, time, timedelta

from dagster import get_dagster_logger

from utils.array_operations import get_difference, left_join_array
from utils.path import BASE_SQL_PATH
import pandas as pd
import psycopg2

from utils.transformers.mp_report import _to_dt


class Operatorscountgetter:
    def __init__(self,
                 report_day,
                 start_date,
                 end_date,
                 naumen_conn_1: psycopg2.extensions.connection,
                 naumen_conn_3: psycopg2.extensions.connection,

                 conn: psycopg2.extensions.connection,

                 bpm_conn: psycopg2.extensions.connection,
                 position_id=None,
                 view_ids=None,
                 ):
        self.view_ids = [8, 9, 14, 16] if view_ids is None else [int(v) for v in (
            view_ids if isinstance(view_ids, (list, tuple, set)) else [view_ids])]
        self.position_id = [35, 36, 37, 39] if position_id is None else [int(p) for p in (
            position_id if isinstance(position_id, (list, tuple, set)) else [position_id])]


        self.report_day = report_day
        self.start_date = start_date
        self.end_date = end_date
        self.naumen_conn_1 = naumen_conn_1
        self.naumen_conn_3 = naumen_conn_3

        self.conn = conn

        self.bpm_conn = bpm_conn
        self.base_sql_path = Path(BASE_SQL_PATH)

    @staticmethod
    def _parse_hms(hms: str) -> time:
        # ожидаем 'HH:MM:SS'
        return datetime.strptime(hms, "%H:%M:%S").time()

    @staticmethod
    def _hms_str(t: time) -> str:
        return t.strftime("%H:%M:%S")

    @staticmethod
    def _add_hours_hms(hms: str, hours: int) -> str:
        dt = datetime.combine(date.today(), Operatorscountgetter._parse_hms(hms)) + timedelta(hours=hours)
        return dt.time().strftime("%H:%M:%S")

    @staticmethod
    def _hour_in_shift(hour_hms: str, start_hms: str, end_hms: str) -> bool:
        """
        Логика «входит ли час в смену» в терминах времени суток.
        Если смена не пересекает полночь: [start, end)
        Если пересекает: [start, 24:00) U [00:00, end)
        """
        h = Operatorscountgetter._parse_hms(hour_hms)
        s = Operatorscountgetter._parse_hms(start_hms)
        e = Operatorscountgetter._parse_hms(end_hms)
        if s < e:
            return s <= h < e
        else:
            return (h >= s) or (h < e)

    @staticmethod
    def _first_day_of_month(s):
        if isinstance(s, datetime):
            d = s.date()
        elif isinstance(s, date):
            d = s
        else:
            d = datetime.strptime(s, "%Y-%m-%d").date()
        return d.replace(day=1)



    def get_labor_data(self):
        # Читается json
        report_month = self._first_day_of_month(self.report_day)
        sql_path = self.base_sql_path / "bpm" / "operators_shifts_json.sql"
        query = sql_path.read_text(encoding="utf-8")
        params = {"view_ids": self.view_ids, "report_month": report_month}
        td_df = pd.read_sql_query(query, self.bpm_conn, params=params)

        # Сбор Эрланга
        erlang_items = []
        if not td_df.empty:
            get_dagster_logger().info(f"td_df has {len(td_df)} rows")
            for row in td_df.itertuples(index=False):
                raw = row.data
                get_dagster_logger().info(f"raw = {raw}")
                if isinstance(raw, str):
                    try:
                        payload = json.loads(raw)
                    except Exception:
                        payload = None
                else:
                    payload = raw

                if not payload:
                    continue
                for item in (payload or []):
                    # в plan -> report_day -> erlang
                    plan = item.get("plan") if isinstance(item, dict) else None

                    if not isinstance(plan, dict):
                        continue
                    report_ymd = self._report_day_ymd()
                    day_plan = plan.get(report_ymd) if isinstance(plan, dict) else None

                    if isinstance(day_plan, dict) and "erlang" in day_plan:
                        erlang_val = day_plan.get("erlang") or 0
                        shift_id = item.get("shift_id")
                        if shift_id is not None:
                            erlang_items.append({"erlang": float(erlang_val), "shift_id": int(shift_id)})



        sql_path = self.base_sql_path / "bpm" / "facts.sql"
        query = sql_path.read_text(encoding="utf-8")
        report_ymd = self._report_day_ymd()
        params = {"report_day": report_ymd, "view_ids": self.view_ids}
        facts = pd.read_sql_query(query, self.bpm_conn, params=params)

        shift_ids = sorted({int(x["shift_id"]) for x in erlang_items})
        if shift_ids:
            sql_path = self.base_sql_path / "bpm" / "shifts.sql"
            shifts_sql = sql_path.read_text(encoding="utf-8")
            shifts = pd.read_sql_query(shifts_sql, self.bpm_conn, params={"ids": shift_ids})
        else:
            return [{"hour": f"{i:02d}:00:00", "erlang": 0, "fact_labor": 0} for i in range(24)]

        # 6) Основной цикл по часам
        response = []
        erlang_lunches = {}  # ключ: shift.start -> {'lunch': 'HH:MM:SS', 'erlang': val}
        fact_lunches = {}  # ключ: час обеда -> [логины]

        for i in range(24):
            hour_str = f"{i:02d}:00:00"

            # 6.1 какие смены активны сейчас
            shifts_now = []
            for sh in shifts.itertuples(index=False):
                sh_start, sh_end = str(sh.start), str(sh.end)
                if self._hour_in_shift(hour_str, sh_start, sh_end):
                    shifts_now.append(int(sh.id))

            # 6.2 erlang за час
            erlang_sum: int = 0
            for item in erlang_items:
                if int(item["shift_id"]) in shifts_now:
                    # найдём смену (для has_lunch)
                    sh = shifts.loc[shifts["id"] == int(item["shift_id"])]
                    if not sh.empty and bool(sh.iloc[0]["has_lunch"]):
                        sh_start_str = str(sh.iloc[0]["start"])
                        if sh_start_str not in erlang_lunches:
                            # lunch = текущий час + 4 часа (а не «start смены + 4»)
                            erlang_lunches[sh_start_str] = {
                                "lunch": self._add_hours_hms(hour_str, 4),
                                "erlang": int(item["erlang"]),
                            }
                    erlang_sum += int(item["erlang"])

            # вычитаем erlang на «час обеда»
            for lunch in erlang_lunches.values():
                if hour_str == lunch["lunch"]:
                    erlang_sum -= int(lunch["erlang"])

            # 6.3 fact_labor за час
            fact_sum: int = 0
            if not facts.empty and shifts_now:
                # считаем факты по сменам
                in_now = facts[facts["id"].isin(shifts_now)]
                fact_sum = int(len(in_now))

                # расписание обедов от фактов: если смена с обедом и start_time == текущему часу
                # lunch = текущий час + 4 часа
                for row in in_now.itertuples(index=False):
                    sh = shifts.loc[shifts["id"] == int(row.id)]
                    if not sh.empty and bool(sh.iloc[0]["has_lunch"]) and str(row.start_time) == hour_str:
                        lunch_h = self._add_hours_hms(hour_str, 4)
                        fact_lunches.setdefault(lunch_h, []).append(str(row.login))

            response.append({"hour": hour_str, "erlang": erlang_sum, "fact_labor": fact_sum})

        # 7) вычитаем людей на обеде из fact_labor на соответствующем часу
        for rec in response:
            h = rec["hour"]
            if h in fact_lunches:
                rec["fact_labor"] = int(rec["fact_labor"]) - len(fact_lunches[h])

        return response

    def get_fact_operators_by_connection(self, start_date, end_date, conn=None):
        if conn is None:
            conn = self.naumen_conn_1
        sql_path = self.base_sql_path / "naumen" / "get_fact_operators_by_connection.sql"
        query = sql_path.read_text(encoding="utf-8")

        params = {
            "start_date" : start_date,
            "end_date": end_date,
        }

        df = pd.read_sql_query(query, conn, params=params)
        get_dagster_logger().info(f"df = {df}")
        return df

    def get_fact_operatorts(self):
        if isinstance(self.report_day, str):
            base = datetime.strptime(self.report_day, '%Y-%m-%d')
        elif isinstance(self.report_day, date) and not isinstance(self.report_day, datetime):
            base = datetime.combine(self.report_day, time.min)
        elif isinstance(self.report_day, datetime):
            base = datetime(self.report_day.year, self.report_day.month, self.report_day.day)
        else:
            raise TypeError(f"Unsupported type for report_day: {type(self.report_day)}")

        end = base + timedelta(days=1)
        naumen = self.get_fact_operators_by_connection(conn=self.naumen_conn_1, start_date=self.report_day, end_date=end)
        naumen_3 = self.get_fact_operators_by_connection(conn=self.naumen_conn_3, start_date=self.report_day, end_date=end)
        data = pd.concat([naumen, naumen_3], ignore_index=True)

        return self.fact_operators_calculate(data, self.report_day, end)


    def get_monthly_fact_operators(self, start_date, end_date):
        get_dagster_logger().info(f"start_date = {start_date}, end_date = {end_date}")
        naumen = self.get_fact_operators_by_connection(conn=self.naumen_conn_1, start_date=start_date, end_date=end_date)
        naumen_3 = self.get_fact_operators_by_connection(conn=self.naumen_conn_3, start_date=start_date,end_date=end_date)
        data = pd.concat([naumen, naumen_3], ignore_index=True)

        return self.fact_operators_calculate(data, start_date, end_date)

    def get_gpo_fact_operators(self):
        start = self.report_day
        end = (datetime.strptime(self.report_day, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        sql_path = self.base_sql_path / "naumen" / "get_fact_operators.sql"
        query = sql_path.read_text(encoding="utf-8")
        params = {"start_date":start, "end_date": end}

        df = pd.read_sql_query(query, self.conn, params=params)
        df["entered"] = pd.to_datetime(df["entered"])
        df["leaved"]  = pd.to_datetime(df["leaved"])
        df["duration"] = df["duration"].astype(float)

        sql_path = self.base_sql_path / "bpm" / "staff_units_gpo.sql"
        query = sql_path.read_text(encoding="utf-8")
        get_dagster_logger().info(f"get gpo fact operators {self.view_ids}")
        params = {"start_date": start, "end_date": end, "view_ids": self.view_ids}

        df_gpo = pd.read_sql_query(query, self.bpm_conn, params=params)
        df_gpo = set(df_gpo["user_id"].dropna().astype(int).unique().tolist())


        df = df[df["user_id"].isin(df_gpo) & (df["duration"] >= 900)]

        hours_map: Dict[str, set] = {}

        for row in df.itertuples(index=False):
            user_id = int(row.user_id)
            t = row.entered
            last = row.leaved
            if pd.isna(t) or pd.isna(last):
                continue
            if last < t:
                t, last = last, t
            cur = t.replace(minute=0, second=0, microsecond=0)
            while cur <= last:
                key = cur.strftime("%H:00:00")
                hours_map.setdefault(key, set()).add(user_id)
                cur += timedelta(hours=1)

        hours_of_day = [
            (_to_dt(self.report_day) + timedelta(hours=i)).strftime("%H:00:00")
            for i in range(24)
        ]

        final_data = []
        for h in hours_of_day:
            count = len(hours_map.get(h, set()))
            final_data.append({"hour": h, "fact_operators": count})

        return final_data

    def fact_operators_calculate(self,data, start, date_end):
        def to_midnight(d) -> datetime:
            if isinstance(d, datetime):
                return datetime(d.year, d.month, d.day)
            if isinstance(d, date_cls):
                return datetime.combine(d, time.min)
            return datetime.strptime(str(d), "%Y-%m-%d")

        start_dt = to_midnight(start)
        end_dt = to_midnight(date_end)

        sql_path = self.base_sql_path / "bpm" / "get_staff_logins_for_views.sql"
        query = sql_path.read_text(encoding="utf-8")

        params = {
            "view_ids" : self.view_ids,
            "start_date": start_dt,
            "end_date": end_dt,
        }

        df = pd.read_sql_query(query, self.bpm_conn, params=params)
        staff_logins = set(df["login"].dropna().astype(str).str.strip().drop_duplicates().tolist())

        if not {"date", "login", "duration"}.issubset(data.columns):
            raise ValueError("data must contain columns: 'date', 'login', 'duration'")

        df = data.copy()
        df["date"] = pd.to_datetime(df["date"])  # уже округлённый до часа timestamp
        df["login"] = df["login"].astype(str).str.strip()
        df["duration"] = pd.to_numeric(df["duration"], errors="coerce").fillna(0).astype(int)

        df = df[df["login"].isin(staff_logins)]
        result = []
        cur = start_dt
        while cur < end_dt:
            hour_start = cur
            hour_end = cur + timedelta(hours=1)

            need = df[(df["date"] >= hour_start) & (df["date"] < hour_end)]

            fact_count = 0
            for row in need.itertuples(index=False):
                entered_time: datetime = row.date
                exit_time: datetime = entered_time + timedelta(seconds=int(row.duration))

                duration_first_sec = (min(hour_end, exit_time) - entered_time).total_seconds()
                duration_second_sec = (exit_time - hour_end).total_seconds() if exit_time > hour_end else 0.0

                if duration_first_sec >= 900 or duration_second_sec >= 900:
                    fact_count += 1

            result.append(
                {
                    "hour": hour_start.strftime("%H:%M:%S"),
                    "date": hour_start.strftime("%Y-%m-%d"),
                    "fact_operators": fact_count,
                }
            )
            cur = hour_end
        get_dagster_logger().info(f"fact_operators_calculate = {result}")
        return result

    def _report_day_ymd(self) -> str:
        rd = self.report_day
        if isinstance(rd, datetime):
            dt = rd
        elif isinstance(rd, date):
            dt = datetime.combine(rd, time(0, 0, 0))
        elif isinstance(rd, str):
            try:
                dt = datetime.fromisoformat(rd.strip())
            except ValueError:
                dt = datetime.strptime(rd.strip()[:10], "%Y-%m-%d")
        else:
            raise TypeError(f"Unsupported report_day type: {type(rd)}")
        return dt.strftime("%Y-%m-%d")