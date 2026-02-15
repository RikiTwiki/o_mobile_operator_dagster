import traceback
from collections import defaultdict
from datetime import datetime, date, timedelta, time
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Union, Optional, Dict, List, Any, Sequence

import datetime as _dt

from sqlalchemy import select
from sqlalchemy.orm import Session

import pandas as pd
import psycopg2

from psycopg2.extras import RealDictCursor

from dagster import get_dagster_logger
from reportlab.graphics.charts.textlabels import NoneOrInstanceOfLabelOffset

from utils.getters.naumen_projects_getter import NaumenProjectsGetter
from utils.path import BASE_SQL_PATH

from utils.models.staff_unit import StaffUnit

from utils.array_operations import left_join_array, left_join_and_sum_values

from dateutil.relativedelta import relativedelta

import re

from utils.transformers.mp_report import try_parse_period, _to_dt, _parse_dt_any


class OccupancyDataGetter(NaumenProjectsGetter):
    def __init__(self,
                bpm_conn: psycopg2.extensions.connection,
                 start_date=None,
                end_date=None,
                conn_naumen_1: psycopg2.extensions.connection = None,
                conn_naumen_3: psycopg2.extensions.connection = None,
                position_ids = 0,
                project_names = None,
                project_position_names: dict[int, str] | None = None,
                set_date_format = False,
                trunc: str = 'day',
                 ):

        super().__init__()

        self.trunc = trunc

        if self.trunc == "day":
            self.date_format = "YYYY-MM-DD HH24:MI:SS"
        elif self.trunc == "hour":
            self.date_format = "YYYY-MM-DD HH:MI:SS"
        elif self.trunc == "month":
            self.date_format = "YYYY-MM-DD"
        self.start_date = (start_date or _dt.datetime.now().strftime("%Y-%m-%d 00:00:00"))
        self.end_date = (end_date or _dt.datetime.now().strftime("%Y-%m-%d 23:59:59"))
        self.conn_naumen_1= conn_naumen_1
        self.bpm_conn = bpm_conn
        self.conn_naumen_3 = conn_naumen_3
        self.position_ids = position_ids
        self.project_names = project_names if project_names is not None else []
        self.project_position_names = project_position_names or {}

        self.set_date_format = set_date_format

        self.period = []

        self.base_sql_path = Path(BASE_SQL_PATH)

        self._rebuild_period()

    def set_interval(self, start_date: str, end_date: str) -> None:
        self.start_date = start_date
        self.end_date = end_date
        self._rebuild_period()

    def set_trunc(self, trunc: str) -> None:
        self.trunc = trunc
        self.date_format = (
            "YYYY-MM-DD HH24:MI:SS" if trunc == "hour"
            else "YYYY-MM-DD 00:00:00" if trunc == "day"
            else "YYYY-MM-DD"
        )
        self._rebuild_period()

    def _rebuild_period(self) -> None:
        """Строит self.Period: список ключей интервала (end-exclusive)."""

        def _to_dt(v: str) -> _dt.datetime:
            if isinstance(v, _dt.datetime):
                return v
            if isinstance(v, str):
                for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                    try:
                        return _dt.datetime.strptime(v, fmt)
                    except ValueError:
                        pass
            raise ValueError(f"Unsupported date format: {v!r}")

        start = _to_dt(self.start_date)
        end = _to_dt(self.end_date)

        keys = []
        if self.trunc == "hour":
            cur = start
            while cur < end:
                keys.append(cur.strftime("%Y-%m-%d %H:%M:%S"))  # 24h формат
                cur += _dt.timedelta(hours=1)
        elif self.trunc == "day":
            cur = start.replace(hour=0, minute=0, second=0, microsecond=0)
            while cur < end:
                keys.append(cur.strftime("%Y-%m-%d 00:00:00"))
                cur += _dt.timedelta(days=1)
        else:  # month
            cur = start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            while cur < end:
                keys.append(cur.strftime("%Y-%m-%d"))
                # +1 месяц
                if cur.month == 12:
                    cur = cur.replace(year=cur.year + 1, month=1)
                else:
                    cur = cur.replace(month=cur.month + 1)

        self.period = keys


    def get_shift(self, date_str: str, hour_ts: Union[str, datetime]) -> Union[str, bool]:
        """
        - night   : [00:00:00, 08:00:00)
        - day     : [08:00:00, 18:00:00)
        - evening : [18:00:00, 23:59:59]  (обрати внимание: верхняя граница включительна)
        Возвращает 'night' | 'day' | 'evening' или False, если не попало в эти интервалы.
        """
        # 1) Нормализуем hour_ts в datetime
        if isinstance(hour_ts, datetime):
            hour_dt = hour_ts
        elif isinstance(hour_ts, str):
            try:
                # ISO 8601 с 'T'/'Z'/таймзоной
                if "T" in hour_ts or "Z" in hour_ts or "+" in hour_ts:
                    hour_dt = datetime.fromisoformat(hour_ts.replace("Z", "+00:00"))
                else:
                    hour_dt = datetime.strptime(hour_ts, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                hour_dt = datetime.strptime(hour_ts, "%Y-%m-%d %H:%M")
        else:
            try:
                hour_dt = pd.to_datetime(hour_ts).to_pydatetime()
            except Exception:
                return False

        # 2) Убираем tzinfo (PHP strtotime сравнивает без таймзоны)
        if hour_dt.tzinfo is not None:
            hour_dt = hour_dt.replace(tzinfo=None)

        # 3) Границы на тот же день, что и date_str
        day_0000 = datetime.strptime(f"{date_str} 00:00:00", "%Y-%m-%d %H:%M:%S")
        day_0800 = datetime.strptime(f"{date_str} 08:00:00", "%Y-%m-%d %H:%M:%S")
        day_1800 = datetime.strptime(f"{date_str} 18:00:00", "%Y-%m-%d %H:%M:%S")
        day_235959 = datetime.strptime(f"{date_str} 23:59:59", "%Y-%m-%d %H:%M:%S")

        # 4) Точно такие же полуинтервалы, как в PHP:
        if day_0000 <= hour_dt < day_0800:
            return "night"
        if day_0800 <= hour_dt < day_1800:
            return "day"
        if day_1800 <= hour_dt <= day_235959:
            return "evening"
        return False

    def get_main_db_data(self, logins, conn, excluded_days=None):
        sql_path = self.base_sql_path / "naumen" / "get_main_db_data.sql"
        sql_template = sql_path.read_text(encoding='utf-8')
        excluded_days_filter = (
            " AND to_char(date_trunc('day', entered), 'YYYY-MM-DD') <> ALL(%(excluded_days)s::text[])"
            if excluded_days else ""
        )
        query = sql_template.format(excluded_days_filter=excluded_days_filter)

        params = {
            "trunc": self.trunc,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "date_format": self.date_format,  # убедись: для hour = 'YYYY-MM-DD HH24:MI:SS'
        }

        if excluded_days is not None:
            params["excluded_days"] = excluded_days
        if isinstance(logins, (list, tuple, set)):
            logins_param = [str(x).strip() for x in logins if x]
        else:
            logins_param = [str(logins)]
        params["logins"] = logins_param

        df = pd.read_sql_query(query, conn, params=params)

        get_dagster_logger().info(f"df: {df}")

        # === НОРМАЛИЗАЦИЯ ДАТЫ ПОД self.period ===
        # self.period для 'hour' у тебя в формате '%Y-%m-%d %H:%M:%S' (24h).
        from datetime import datetime as _dt
        def _coerce_date_to_str(v):
            if isinstance(v, (pd.Timestamp, _dt)):
                return v.strftime('%Y-%m-%d %H:%M:%S') if self.trunc == 'hour' else (
                    (v.strftime('%Y-%m-%d 00:00:00') if self.trunc == 'day' else v.strftime('%Y-%m-%d'))
                )
            # Если SQL уже отдал строку, доверимся ей — но лучше привести дню к '... 00:00:00'
            if isinstance(v, str):
                if self.trunc == 'day' and len(v) == 10:  # 'YYYY-MM-DD'
                    return f"{v} 00:00:00"
                return v
            return str(v)

        if 'date' in df.columns:
            df['date'] = df['date'].map(_coerce_date_to_str)
        else:
            # на всякий — пустой DF с нужными колонками
            df = pd.DataFrame(columns=['date', 'status', 'duration'])

        # === НОРМАЛИЗАЦИЯ СТАТУСОВ ПОД КЛЮЧИ statuses_map ===
        statuses_map = {
            'redirect': 'redirect',
            'normal': 'normal',
            'ringing': 'ringing',
            'speaking': 'speaking',
            'standoff': 'standoff',
            'away': 'away_other',
            'Dinner': 'away_dinner',
            'TechnicalBreak': 'away_technical_break',
            'dnd': 'dnd',
            'accident': 'accident',
            'custom1': 'custom1',
            'custom2': 'custom2',
            'custom3': 'custom3',
            'wrapup': 'wrapup',
        }

        # inbound → canonical ключи из statuses_map
        def _norm_status(s):
            if s is None:
                return None
            t = str(s).strip()
            t_l = t.lower().replace(' ', '').replace('-', '').replace('_', '')
            # самые частые варианты:
            if t_l in ('redirect',): return 'redirect'
            if t_l in ('normal',): return 'normal'
            if t_l in ('ringing',): return 'ringing'
            if t_l in ('speaking',): return 'speaking'
            if t_l in ('standoff',): return 'standoff'
            if t_l in ('away', 'awayother', 'other'): return 'away'
            if t_l in ('dinner', 'обед'): return 'Dinner'
            if t_l in ('technicalbreak', 'technicalbreaks', 'technicalbreak_', 'technical', 'technical_break',
                       'technicalbreak ', 'technicalpause', 'перерыв'):
                return 'TechnicalBreak'
            if t_l in ('dnd', 'donotdisturb'): return 'dnd'
            if t_l in ('accident',): return 'accident'
            if t_l in ('custom1', 'custom01', 'c1'): return 'custom1'
            if t_l in ('custom2', 'custom02', 'c2'): return 'custom2'
            if t_l in ('custom3', 'custom03', 'c3'): return 'custom3'
            if t_l in ('wrapup', 'wrap', 'wrap-up'): return 'wrapup'
            # по умолчанию вернём исходное — чтобы ты увидел это в списке уникальных
            return t

        if 'status' in df.columns:
            df['status'] = df['status'].map(_norm_status)
        else:
            df['status'] = None

        if 'duration' in df.columns:
            df['duration'] = pd.to_numeric(df['duration'], errors='coerce').fillna(0)
        else:
            df['duration'] = 0

        # === ГРУППИРОВКА ===
        grouped = df.groupby(['date', 'status'], dropna=False)['duration'].sum()
        grouped_dict = grouped.to_dict()  # ключ: (date(str), status(str)) -> сумма секунд

        # === СБОРКА ПО self.period ===
        if not hasattr(self, 'period') or not isinstance(self.period, list):
            raise AttributeError("self.period must be a list of strings matching 'date' format from SQL")

        db_data_main = []
        for p in self.period:
            item = {'date': p}
            # инициализируем все выходные поля нулём
            for out_field in statuses_map.values():
                item[out_field] = 0.0

            for status_key, out_field in statuses_map.items():
                dur = grouped_dict.get((p, status_key), 0.0)
                try:
                    item[out_field] += float(dur or 0)
                except Exception:
                    item[out_field] += int(float(dur) if pd.notna(dur) else 0)

            # wrapup += away_technical_break
            item['wrapup'] = float(item.get('wrapup', 0.0)) + float(item.get('away_technical_break', 0.0))
            db_data_main.append(item)

        # Диагностика результата
        nonzero = sum(1 for r in db_data_main if (r['normal'] + r['ringing'] + r['speaking'] + r['wrapup']) > 0)
        return db_data_main

    def get_csf_data(self, position_ids, conn):
        sql_path_1 = self.base_sql_path / "bpm" / "get_active_users_logins.sql"
        query_1 = sql_path_1.read_text(encoding="utf-8")
        params = {
            "start_date" : self.start_date,
            "end_date": self.end_date,
            "position_ids": position_ids,
        }
        df_logins = pd.read_sql_query(query_1, self.bpm_conn, params=params)
        logins = df_logins["login"].dropna().astype(str).tolist()
        if not logins:
            return {"occupancy": [], "detailed": []}


        sql_path = self.base_sql_path / "naumen" / "get_csf_data.sql"
        query = sql_path.read_text(encoding="utf-8")
        params = {
            "logins": logins,
            "start_date": self.start_date,
            "end_date": self.end_date,
        }
        df = pd.read_sql_query(query, conn, params=params)

        # === 1) привести числа и гарантировать наличие колонок ===
        for col in ("normal", "ringing", "speaking", "wrapup"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("int64")
            else:
                df[col] = 0

        # === 2) аккумулируем
        occupancy = {}  # {date: {shift: {'productive_time': sec, 'handling_time': sec}}}
        detailed = {}  # {date: {'productive_time': sec, 'handling_time': sec}}

        for _, row in df.iterrows():
            date_str = row["date"]  # 'YYYY-MM-DD'
            hour_ts = row["hour"]  # timestamptz/ts
            ringing = int(row["ringing"])
            speaking = int(row["speaking"])
            wrapup = int(row["wrapup"])
            normal = int(row["normal"])

            call_handling_time = ringing + speaking + wrapup
            product_time = normal + call_handling_time

            # определить смену (должна существовать self.get_shift(date_str, hour_ts) -> 'day'|'evening'|'night')
            shift = self.get_shift(date_str, hour_ts)
            if shift:
                # заполнить/увеличить по смене
                occupancy.setdefault(date_str, {}).setdefault(shift, {"productive_time": 0, "handling_time": 0})
                occupancy[date_str][shift]["productive_time"] += product_time
                occupancy[date_str][shift]["handling_time"] += call_handling_time

            # заполнить/увеличить суммарно по дате
            detailed.setdefault(date_str, {"productive_time": 0, "handling_time": 0})
            detailed[date_str]["productive_time"] += product_time
            detailed[date_str]["handling_time"] += call_handling_time

        # === 3) PHP-round (без банковского округления) ===
        def php_round(x: float) -> int:
            return int(x + 0.5) if x >= 0 else int(x - 0.5)

        # === 4) собрать occupancy: проценты по сменам + date ===
        occupancy_data = []
        for date_str, shifts in occupancy.items():
            res = {}
            for sh, vals in shifts.items():
                prod = vals["productive_time"]
                hand = vals["handling_time"]
                res[sh] = php_round((hand / prod) * 100.0) if prod > 0 else 0
            res["date"] = date_str
            occupancy_data.append(res)

        occupancy_data.sort(key=lambda x: x["date"])

        # === 5) собрать detailed
        detailed_data = []
        for date_str, vals in detailed.items():
            prod_min = vals["productive_time"] / 60.0
            hand_min = vals["handling_time"] / 60.0
            detailed_data.append({
                "date": date_str,
                "productive_time": prod_min,
                "handling_time": hand_min,
                "difference": (prod_min - hand_min),
            })
        detailed_data.sort(key=lambda x: x["date"])
        get_dagster_logger().info(f"occupancy_csf = {occupancy} detailed_csf = {detailed_data}")
        return {"occupancy": occupancy_data, "detailed": detailed_data}

    def get_occupancy_by_staff_unit_id(self, staff_unit_id, session: Session, excluded_days=None):
        unit = session.get(StaffUnit, staff_unit_id)
        if unit is None:
            raise LookupError(f"StaffUnit(id={staff_unit_id}) not found")
        login = unit.login


        selected_data_naumen1 = self.get_main_db_data([login], conn=self.conn_naumen_1, excluded_days=excluded_days)
        selected_data_naumen3 = self.get_main_db_data([login], conn=self.conn_naumen_3, excluded_days=excluded_days)
        merged_data = selected_data_naumen1 + selected_data_naumen3

        totals = defaultdict(float)
        for item in merged_data:
            for key, value in item.items():
                if key == "date":
                    continue
                totals[key] += float(value or 0.0)

        # 5) расчёт productTime / handlingTime и occupancy
        product_time = (
                totals["normal"]
                + totals["ringing"]
                + totals["speaking"]
                + totals["custom2"]
                + totals["wrapup"]
        )
        handling_time = (
                totals["ringing"]
                + totals["speaking"]
                + totals["custom2"]
                + totals["wrapup"]
        )

        if product_time == 0 or handling_time == 0:
            occupancy = 0.0
        else:
            occupancy = (handling_time / product_time) * 100.0

        # 6) собрать ответ (как в PHP, все поля явно)
        return {
            "staff_unit_id": staff_unit_id,
            "user_login": login,
            "occupancy": occupancy,
            "product_time": product_time,
            "handling_time": handling_time,
            "redirect": totals["redirect"],
            "normal": totals["normal"],
            "ringing": totals["ringing"],
            "speaking": totals["speaking"],
            "wrapup": totals["wrapup"],
            "standoff": totals["standoff"],
            "away_other": totals["away_other"],
            "away_dinner": totals["away_dinner"],
            "away_technical_break": totals["away_technical_break"],
            "dnd": totals["dnd"],
            "accident": totals["accident"],
            "custom1": totals["custom1"],
            "custom2": totals["custom2"],
            "custom3": totals["custom3"],
        }

    def get_occupancy_by_position_ids(self, position_ids, start_date, end_date):
        sql_path = self.base_sql_path / "bpm" / "occupancy_by_position_ids.sql"
        query = sql_path.read_text(encoding="utf-8")
        params = {
            "end_date": end_date,
            "position_ids": position_ids,
            "start_date" : start_date,
        }
        df = pd.read_sql_query(query, self.bpm_conn, params=params)
        logins = df["login"].tolist()

        selected_data = self.get_main_db_data([logins], conn=self.conn_naumen_1)
        selected_data = selected_data[0]

        # 3) считаем productTime / handlingTime
        normal = float(selected_data.get('normal', 0))
        ringing = float(selected_data.get('ringing', 0))
        speaking = float(selected_data.get('speaking', 0))
        wrapup = float(selected_data.get('wrapup', 0))

        product_time = normal + ringing + speaking + wrapup
        handling_time = ringing + speaking + wrapup

        occupancy = (handling_time / product_time) * 100 if product_time > 0 else 0.0

        # 4) полный ответ
        return {
            "employees_count_hr": len(logins),
            "employees_count_emp": len(logins),
            "occupancy": occupancy,
            "product_time": product_time,
            "handling_time": handling_time,
            "redirect": float(selected_data.get('redirect', 0)),
            "normal": normal,
            "ringing": ringing,
            "speaking": speaking,
            "wrapup": wrapup,
            "standoff": float(selected_data.get('standoff', 0)),
            "away_other": float(selected_data.get('away_other', 0)),
            "away_dinner": float(selected_data.get('away_dinner', 0)),
            "away_technical_break": float(selected_data.get('away_technical_break', 0)),
            "dnd": float(selected_data.get('dnd', 0)),
            "accident": float(selected_data.get('accident', 0)),
            "custom1": float(selected_data.get('custom1', 0)),
            "custom2": float(selected_data.get('custom2', 0)),
            "custom3": float(selected_data.get('custom3', 0)),
        }

    def get_aggregated_data_filtered(self, position_ids, view_ids=None, project_name=None):
        # 1) view_ids обязателен для текущего SQL
        if not view_ids:
            # используем дефолт из объекта, если есть, иначе бросаем ошибку
            view_ids = getattr(self, "view_ids", None)
        if not view_ids:
            raise ValueError("view_ids обязателен для occupancy_by_position_ids.sql")

        # 2) читаем SQL как есть (без .format)
        sql_path = self.base_sql_path / "bpm" / "occupancy_by_position_ids.sql"
        query = sql_path.read_text(encoding="utf-8")

        params = {
            "position_ids": position_ids,  # list[int]
            "start_date": self.start_date,  # 'YYYY-MM-DD'
            "end_date": self.end_date,  # 'YYYY-MM-DD'
            "view_ids": view_ids,  # list[int]
        }

        # 3) выполняем запрос без pandas
        with self.bpm_conn.cursor() as cur:
            cur.execute(query, params)
            # SELECT DISTINCT u.login -> одна колонка
            rows = cur.fetchall()
        logins = [r[0] for r in rows if r and r[0]]

        logins = [str(x).strip().lower() for x in logins if x]

        # 4) тянем данные из Naumen 1/3 и объединяем
        naumen_data = self.get_main_db_data(logins, conn=self.conn_naumen_1)
        naumen_data_3 = self.get_main_db_data(logins, conn=self.conn_naumen_3)

        def _sum_merge_by_date(a, b):
            # суммирующий merge по ключу 'date'
            idx = {row["date"]: row.copy() for row in a}
            for row in b:
                d = row.get("date")
                if not d:
                    continue
                base = idx.setdefault(d, {"date": d})
                for k, v in row.items():
                    if k == "date":
                        continue
                    av = base.get(k, 0) or 0
                    bv = v or 0
                    # суммируем только числа
                    if isinstance(av, (int, float)) or isinstance(bv, (int, float)):
                        try:
                            base[k] = float(av) + float(bv)
                        except Exception:
                            # если что-то некастуемое — оставим исходное
                            base[k] = av
                    else:
                        base[k] = base.get(k, 0)
            return list(idx.values())

        db_data_main = _sum_merge_by_date(naumen_data, naumen_data_3)

        by_date = {row["date"]: row for row in db_data_main if "date" in row}

        # 5) correctionDate
        from datetime import datetime
        correction_date_str = "2020-11-01" if self.trunc == "month" else "2020-11-02"
        correction_date = datetime.strptime(correction_date_str, "%Y-%m-%d").date()


        items = []
        for period in self.period:
            date_str, hour_str = self._to_date_hour_strings(period)
            row = by_date.get(period) or by_date.get(date_str)

            if row is not None:
                item_day_str, _ = self._to_date_hour_strings(row["date"])
                try:
                    item_day = datetime.strptime(item_day_str, "%Y-%m-%d").date()
                except ValueError:
                    item_day = correction_date

                if item_day >= correction_date:
                    normal = float(row.get("normal", 0) or 0)
                    ringing = float(row.get("ringing", 0) or 0)
                    speaking = float(row.get("speaking", 0) or 0)
                    wrapup = float(row.get("wrapup", 0) or 0)

                    product_time = normal + ringing + speaking + wrapup
                    handling_time = ringing + speaking + wrapup

                    if product_time == 0 and handling_time == 0:
                        occupancy = None
                    elif handling_time == 0:
                        occupancy = 0.0
                    else:
                        occupancy = (handling_time / product_time) * 100.0

                    items.append({
                        "date": date_str,
                        "hour": hour_str,
                        "product_time": product_time,
                        "handling_time": handling_time,
                        "occupancy": occupancy,
                        "projectName": project_name,
                    })
                    continue

            # нет данных/не проходит фильтр
            items.append({
                "date": date_str,
                "hour": hour_str,
                "product_time": None,
                "handling_time": None,
                "occupancy": None,
                "projectName": project_name,
            })

        return {
            "labels": [
                {"field": "date", "title": "Дата"},
                {"field": "product_time", "title": "Продуктивное время"},
                {"field": "handling_time", "title": "Время в обработке"},
                {"field": "occupancy", "title": "Occupancy"},
            ],
            "items": items,
        }

    def _to_date_hour_strings(self, s: str) -> (str, str):
        """
        Разбивает строковый момент времени на ('YYYY-MM-DD', 'HH:MM:SS').
        Если в строке только дата — час = '00:00:00'.
        """
        # пробуем с datetime
        fmts = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"]
        dt = None
        for f in fmts:
            try:
                dt = datetime.strptime(s, f)
                break
            except ValueError:
                continue
        if dt is None:
            # как fallback — возьмём только дату
            return s[:10], "00:00:00"
        return dt.strftime("%Y-%m-%d"), dt.strftime("%H:%M:%S")


    def _fmt_date_output(self, period_str: str) -> str:
        """PHP: $this->SetDateFormat ? 'Y-m-d H:i:s' : 'd.m.Y'"""
        # Пытаемся распарсить разные кейсы:
        fmts = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"]
        dt = None
        for f in fmts:
            try:
                dt = datetime.strptime(period_str, f)
                break
            except ValueError:
                pass
        if dt is None:
            # fallback: возьмём только первые 10 символов как дату
            try:
                dt = datetime.strptime(period_str[:10], "%Y-%m-%d")
            except Exception:
                return period_str  # отдаём как есть

        return dt.strftime("%Y-%m-%d %H:%M:%S") if self.set_date_format else dt.strftime("%d.%m.%Y")

    def _fmt_hour_output(self, period_str: str) -> str:
        """PHP: date('H:i:s', strtotime($period))"""
        fmts = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"]
        for f in fmts:
            try:
                return datetime.strptime(period_str, f).strftime("%H:%M:%S")
            except ValueError:
                pass
        # если была только дата — как в PHP, это даст 00:00:00
        try:
            datetime.strptime(period_str[:10], "%Y-%m-%d")
            return "00:00:00"
        except Exception:
            return "00:00:00"

    def position_planned_time(self, position_ids, start_date, end_date):
        if not position_ids:
            return 0
        sql_path = self.base_sql_path / "bpm" / "position_planned_time.sql"
        sql_template = sql_path.read_text(encoding="utf-8")
        params = {"start_date" : start_date, "end_date" : end_date}
        df = pd.read_sql_query(sql_template, self.bpm_conn, params=params)
        return int(df.iloc[0]["planned_seconds"] or 0)

    def position_planned_time_day(self, position_ids: List[int], start_date: str, end_date: str):
        if not position_ids:
            return 0
        sql_path = self.base_sql_path / "bpm" / "position_planned_time_day.sql"
        sql = sql_path.read_text(encoding="utf-8")
        params = {
            "position_ids": list(position_ids),
            "start_date": start_date,
            "end_date": end_date,
        }

        df = pd.read_sql_query(sql, self.bpm_conn, params=params)
        if df.empty:
            return 0
        return int(df.iloc[0]["planned_seconds"] or 0)

    def get_aggregated_data(self, position_ids, view_ids = None):
        if view_ids is None:
            view_ids = []
        sql_path = self.base_sql_path / "bpm" / "occupancy_by_position_ids_new.sql"
        sql_template = sql_path.read_text(encoding="utf-8")
        if view_ids:
            extra = "AND tdd.timetable_view_id = ANY(%(view_ids)s::int[])"
        else:
            extra = ""

        get_dagster_logger().info(f"project_position_names_in_method: {self.project_position_names}")

        get_dagster_logger().info(f"start_date: {self.start_date}")
        get_dagster_logger().info(f"end_date: {self.end_date}")

        query = sql_template.format(extra_view_filter=extra)
        params = {
            "position_ids": position_ids,
            "start_date" : self.start_date,
            "end_date" : self.end_date
        }
        if view_ids:
            params["view_ids"] = view_ids


        df = pd.read_sql_query(query, self.bpm_conn, params=params)

        get_dagster_logger().info(f"df: {df}")

        logins = df["login"].dropna().tolist()

        get_dagster_logger().info(f"logins: {logins}")

        naumen_data = self.get_main_db_data(logins, conn=self.conn_naumen_1)

        get_dagster_logger().info(f"naumen_data: {naumen_data}")

        naumen_data_3 = self.get_main_db_data(logins, conn=self.conn_naumen_3)

        get_dagster_logger().info(f"naumen_data_3: {naumen_data_3}")

        db_data_main = left_join_and_sum_values(naumen_data, naumen_data_3, left_join_on='date')

        items: List[Dict[str, Any]] = []


        by_date: Dict[str, Dict[str, Any]] = {row["date"]: row for row in db_data_main if "date" in row}

        # correctionDate
        correction_date_str = "2020-11-01" if self.trunc == "month" else "2020-11-02"
        correction_date = datetime.strptime(correction_date_str, "%Y-%m-%d").date()

        pid = None
        if position_ids:
            pid = position_ids[0] if isinstance(position_ids, (list, tuple, set)) else position_ids
        elif getattr(self, "position_ids", None):
            pid = self.position_ids[0] if isinstance(self.position_ids, (list, tuple, set)) else self.position_ids

        position_name = self.project_position_names.get(int(pid)) if pid is not None else None
        get_dagster_logger().info(f"position_name_in_method: {position_name}")

        get_dagster_logger().info(f"position_name_in_method: {position_name}")

        for period in self.period:
            data_exist = False

            row = by_date.get(period)
            if row is not None:
                try:
                    row_day = datetime.strptime(str(row["date"])[:10], "%Y-%m-%d").date()
                except Exception:
                    row_day = None

                if row_day is not None and row_day >= correction_date:
                    normal = float(row.get("normal", 0) or 0)
                    ringing = float(row.get("ringing", 0) or 0)
                    speaking = float(row.get("speaking", 0) or 0)
                    wrapup = float(row.get("wrapup", 0) or 0)

                    product_time = normal + ringing + speaking + wrapup
                    handling_time = ringing + speaking + wrapup

                    if product_time == 0 and handling_time == 0:
                        occupancy: Optional[float] = None
                    elif handling_time == 0:
                        occupancy = 0.0
                    else:
                        occupancy = (handling_time / product_time) * 100.0

                    items.append({
                        "date": self._fmt_date_output(period),
                        "hour": self._fmt_hour_output(period),
                        "product_time": product_time,
                        "handling_time": handling_time,
                        "occupancy": occupancy,
                        "PositionName": position_name,
                    })
                    data_exist = True

            if not data_exist:
                items.append({
                    "date": self._fmt_date_output(period),
                    "hour": self._fmt_hour_output(period),
                    "product_time": None,
                    "handling_time": None,
                    "occupancy": None,
                    "PositionName": None,
                })

        return {
            "labels": [
                {"field": "date", "title": "Дата"},
                {"field": "product_time", "title": "Продуктивное время"},
                {"field": "handling_time", "title": "Время в обработке"},
                {"field": "occupancy", "title": "Occupancy"},
            ],
            "items": items,
        }

    def get_aggregated_data_day(self, position_ids, view_ids=None):
        sql_path = self.base_sql_path / "bpm" / "occupancy_by_position_ids.sql"
        sql_template = sql_path.read_text(encoding="utf-8")
        if view_ids:
            extra = "AND tdd.timetable_view_id = ANY(%(view_ids)s::int[])"
        else:
            extra = ""

        query = sql_template.format(extra_view_filter=extra)
        params = {
            "position_ids": position_ids,
            "start_date": self.start_date,
            "end_date": self.end_date
        }
        if view_ids:
            params["view_ids"] = view_ids

        df = pd.read_sql_query(query, self.bpm_conn, params=params)
        logins = (df['login'].dropna().drop_duplicates().tolist())
        naumen_data = self.get_main_db_data(logins, conn=self.conn_naumen_1)
        position_name = (
            self.project_position_names[self.position_ids]
            if getattr(self, "position_id", None) else None
        )
        correction_date = datetime.strptime("2020-11-02", "%Y-%m-%d").date()

        by_date = {row["date"]: row for row in naumen_data if "date" in row}

        items: List[Dict[str, Any]] = []

        # проход по периодам
        for period in self.period:
            data_exist = False

            row = by_date.get(period)
            if row is not None:
                row_day_str, _ = self._to_date_hour_strings(row["date"])
                try:
                    row_day = datetime.strptime(row_day_str, "%Y-%m-%d").date()
                except ValueError:
                    row_day = correction_date

                if row_day >= correction_date:
                    # productTime / handlingTime / occupancy
                    normal = float(row.get("normal", 0) or 0)
                    ringing = float(row.get("ringing", 0) or 0)
                    speaking = float(row.get("speaking", 0) or 0)
                    wrapup = float(row.get("wrapup", 0) or 0)

                    product_time = normal + ringing + speaking + wrapup
                    handling_time = ringing + speaking + wrapup

                    if product_time == 0 and handling_time == 0:
                        occupancy = None
                    elif handling_time == 0:
                        occupancy = 0.0
                    else:
                        occupancy = (handling_time / product_time) * 100.0

                    items.append({
                        "date": self._fmt_date_output(period),
                        "hour": self._fmt_hour_output(period),
                        "product_time": product_time,
                        "handling_time": handling_time,
                        "occupancy": occupancy,
                        "PositionName": position_name,
                    })
                    data_exist = True

            if not data_exist:
                items.append({
                    "date": self._fmt_date_output(period),
                    "hour": self._fmt_hour_output(period),
                    "product_time": None,
                    "handling_time": None,
                    "occupancy": None,
                    "PositionName": None,
                })

        return {
            "labels": [
                {"field": "date", "title": "Дата"},
                {"field": "product_time", "title": "Продуктивное время"},
                {"field": "handling_time", "title": "Время в обработке"},
                {"field": "occupancy", "title": "Occupancy"},
            ],
            "items": items,
        }

    def get_utilization_rate_data(self, position_ids):
        occupancy_data = self.get_aggregated_data_day(position_ids)
        items = occupancy_data.get("items", [])

        result = []

        for datum in items:
            # 2) Определяем границы месяца: startDate = YYYY-MM-01, endDate = 1-е число следующего месяца
            d_str, _ = self._to_date_hour_strings(str(datum.get("date", "")))
            try:
                dt = datetime.strptime(d_str, "%Y-%m-%d").date()
            except ValueError:
                dt = datetime.strptime(d_str, "%d.%m.%Y").date()

            start_date = dt.replace(day=1)
            year = start_date.year + (1 if start_date.month == 12 else 0)
            month = 1 if start_date.month == 12 else start_date.month + 1
            end_date = date(year, month, 1)

            start_s = start_date.strftime("%Y-%m-01")
            end_s = end_date.strftime("%Y-%m-01")

            # 3) Плановое время
            planned_time = self.position_planned_time(position_ids, start_s, end_s)

            # 4) utilization_rate = product_time / planned_time * 100
            product_time = float(datum.get("product_time") or 0)
            utilization_rate = (product_time / planned_time * 100) if planned_time else None

            result.append({
                "date": start_s,
                "product_time": product_time,
                "planned_time": planned_time,
                "utilization_rate": utilization_rate,
            })

        return result

    def get_utilization_rate_data_day(self, position_ids):
        # 1) Берём «дневную» агрегацию
        occupancy_data = self.get_aggregated_data_day(position_ids)
        items = occupancy_data.get("items", [])

        def php_round_to_int(x: float) -> int:
            return int(Decimal(x).quantize(Decimal("1"), rounding=ROUND_HALF_UP))

        out = []

        for datum in items:
            # 2) Нормализуем дату к 'YYYY-MM-DD'
            raw_date = str(datum.get("date", "")).strip()
            try:
                ymd = _parse_dt_any(raw_date).strftime("%Y-%m-%d")
            except Exception:
                # запасной путь: только первые 10 символов и несколько форматов
                s10 = raw_date[:10]
                for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d-%m-%Y"):
                    try:
                        ymd = datetime.strptime(s10, fmt).strftime("%Y-%m-%d")
                        break
                    except ValueError:
                        pass
                else:
                    raise ValueError(f"Unsupported date format: {raw_date!r}")

            # 3) Следующий день
            d = datetime.strptime(ymd, "%Y-%m-%d").date()
            next_ymd = (d + timedelta(days=1)).strftime("%Y-%m-%d")

            # 4–5) План / Utilization
            planned_time = self.position_planned_time_day(position_ids, ymd, next_ymd)
            product_time = float(datum.get("product_time") or 0)
            utilization_rate = php_round_to_int(product_time / planned_time * 100) if planned_time else 0

            out.append({
                "date": ymd,
                "product_time": product_time,
                "planned_time": planned_time,
                "value": utilization_rate,
                "unit": "%",
            })

        return out

    def get_utilization_rate_data_with_projects(self, position_ids, view_ids, period, project_name ):
        occupancy_data = self.get_aggregated_data(position_ids, view_ids or [])
        if isinstance(occupancy_data, dict):
            items = occupancy_data.get("items", [])
        else:
            items = occupancy_data

        sum_planned_time = 0.0
        sum_product_time = 0.0
        out: List[Dict[str, Any]] = []

        for datum in items:
            # 2) Нормализуем дату к 'YYYY-MM-DD'
            raw_date = str(datum.get("date", ""))
            ymd, _ = self._to_date_hour_strings(raw_date)
            if "-" not in ymd:
                # если пришло 'DD.MM.YYYY', конвертируем
                try:
                    ymd = try_parse_period(raw_date)
                except Exception:
                    ymd = try_parse_period(raw_date)

            if isinstance(ymd, (datetime, date)):
                ymd_str = ymd.strftime("%Y-%m-%d")
            else:
                ymd_str = str(ymd)

            d = datetime.strptime(ymd_str, "%Y-%m-%d").date()
            next_ymd = (d + timedelta(days=1)).strftime("%Y-%m-%d")

            # 3) Плановое время на день (секунды)
            planned_time = self.position_planned_time_day(position_ids, ymd, next_ymd)

            # 4) Коэффициент использования
            product_time = float(datum.get("product_time") or 0.0)
            utilization_rate = (product_time / planned_time * 100.0) if planned_time else 0.0

            # 5) Сбор сумм
            sum_planned_time += float(planned_time or 0.0)
            sum_product_time += product_time

            out.append({
                "date": ymd,
                "product_time": product_time,
                "planned_time": planned_time,
                "utilization_rate": utilization_rate,
                "projectName": project_name,
            })

        # 6) Если нужен итог за месяц
        if period == "month":
            overall = (sum_product_time / sum_planned_time * 100.0) if sum_planned_time else 0.0
            return {"utilization_rate": overall}

        return out

    def get_daily_utilization_rate_data(
            self,
            position_ids,
            period = None,
    ):
        occupancy_data = self.get_aggregated_data(position_ids)
        get_dagster_logger().info(f"occupancy = {occupancy_data}")
        if isinstance(occupancy_data, dict):
            items = occupancy_data.get("items", [])
        else:
            items = occupancy_data

        position_name = None
        pos_id = getattr(self, "position_id", None)
        if pos_id is not None:
            position_name = self.project_position_names.get(pos_id)

        sum_planned_time = 0.0
        sum_product_time = 0.0
        out: List[Dict[str, Any]] = []

        for datum in items:
            raw_date = str(datum.get("date", ""))
            ymd, _ = self._to_date_hour_strings(raw_date)
            if "-" not in ymd:  # если пришло как 'DD.MM.YYYY'
                dt = try_parse_period(raw_date)
                if not dt:
                    raise ValueError(f"Не удалось распарсить дату: {raw_date!r}")
                ymd = dt.strftime("%Y-%m-%d")

            d = datetime.strptime(ymd, "%Y-%m-%d").date()
            next_ymd = (d + timedelta(days=1)).strftime("%Y-%m-%d")

            # Плановое время на день (секунды)
            planned_time = self.position_planned_time_day(position_ids, ymd, next_ymd)

            get_dagster_logger().info(f"planned_time: {planned_time}")

            # Фактическое продуктивное время
            product_time_raw = datum.get("product_time")
            product_time = float(product_time_raw or 0.0)

            # Коэффициент использования
            utilization_rate = (product_time / planned_time * 100.0) if planned_time else 0.0

            # Суммы для общего процента
            sum_planned_time += float(planned_time or 0.0)
            sum_product_time += product_time

            get_dagster_logger().info(f"sum_planned_time and sum_product_time: {sum_planned_time, sum_product_time}")

            out.append({
                "date": datum.get("date"),
                "product_time": product_time_raw,
                "planned_time": planned_time,
                "utilization_rate": utilization_rate,
                "PositionName": position_name,
            })

        if period == "Month":
            overall = (sum_product_time / sum_planned_time * 100.0) if sum_planned_time else 0.0
            return {"utilization_rate": overall}

        return out

    def get_daily_occupancy_by_staff_unitId_without_date(self, staff_unit_id, session:Session, excluded_days=None):
        unit = session.get(StaffUnit, staff_unit_id)
        if unit is None:
            raise LookupError(f"StaffUnit(id={staff_unit_id}) not found")
        login = unit.login

        all_statuses = [
            'normal', 'ringing', 'speaking', 'wrapup', 'standoff',
            'away', 'dnd', 'accident', 'custom1', 'custom2',
            'custom3', 'away_technical_break'
        ]

        sql_path = self.base_sql_path / "naumen" / "get_daily_occupancy_by_staff_unitId_without_date.sql"
        sql_template = sql_path.read_text(encoding="utf-8")
        if excluded_days:
            excluded_clause = """
                      AND NOT (
                            to_char(date_trunc('day', entered), 'YYYY-MM-DD')
                            = ANY(%(excluded_days)s::text[])
                      )
                    """
        else:
            excluded_clause = ""


        query = sql_template.format(extra_view_filter=excluded_clause)
        params = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "login": login,
            "all_statuses": all_statuses,
        }
        if excluded_days:
            params["excluded_days"] = excluded_days

        df = pd.read_sql_query(query, self.conn_naumen_1, params=params)
        daily = {s: 0 for s in all_statuses}
        daily["productTime"] = 0

        if not df.empty:
            # Складываем по статусам (вдруг один и тот же статус есть в обеих таблицах)
            by_status = df.groupby("status", as_index=False)["duration"].sum()
            for row in by_status.itertuples(index=False):
                status = row.status
                duration = int(row.duration or 0)

                if status in ("normal", "ringing", "speaking", "wrapup"):
                    daily["productTime"] += duration

                if status in daily:
                    daily[status] += duration

        return daily

    def get_product_time_by_position_ids(self, position_ids: Sequence[int], end_date: str) -> List[Dict[str, Any]]:
        """
        1) Берём логины из BPM.staff_units, активные на end_date, по position_ids.
        2) Считаем duration (сек) из Naumen.status_changes_ms по (date, login):
           entered ∈ [self.start_date, self.end_date), status ∈ {ringing,speaking,wrapup},
           дата — date_trunc(self.trunc, entered).
        Возвращает: [{"date":"YYYY-MM-DD", "login":str, "duration": int}, ...]
        """
        # --- 1) Логины из BPM ---
        sql_bpm_path = self.base_sql_path / "bpm" / "occupancy_by_position_ids_old.sql"
        sql_bpm = sql_bpm_path.read_text(encoding="utf-8")

        params_bpm = {
            "position_ids": list(position_ids),
            "end_date": end_date,  # ВАЖНО: активность на end_date (как в PHP-оригинале)
        }

        with self.bpm_conn.cursor() as cur:
            cur.execute(sql_bpm, params_bpm)
            logins = [row[0] for row in cur.fetchall()]

        if not logins:
            return []

        # --- 2) Продуктивное время из Naumen ---
        product_statuses = ["ringing", "speaking", "wrapup"]  # как в оригинале

        sql_naumen_path = self.base_sql_path / "naumen" / "product_time_by_logins.sql"
        sql_naumen = sql_naumen_path.read_text(encoding="utf-8")

        params_naumen = {
            "trunc": getattr(self, "trunc", "day"),  # set_days_trunked_data() должен был выставить self.trunc
            "start_date": self.start_date,  # !!! как в оригинале: StartDate/EndDate из объекта
            "end_date": self.end_date,  # а не параметр end_date из аргументов
            "product_statuses": product_statuses,
            "logins": logins,
        }

        conn = getattr(self, "conn_naumen_1", None)
        if conn is None:
            raise RuntimeError("conn_naumen_1 is not set")

        with conn.cursor() as cur:
            cur.execute(sql_naumen, params_naumen)
            cols = [c[0] for c in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]

        # На одном источнике дубликатов почти не будет, но сгруппируем на всякий случай
        agg = {}
        for r in rows:
            k = (r["date"], r["login"])
            agg[k] = agg.get(k, 0) + int(r.get("duration") or 0)

        out = [{"date": d, "login": l, "duration": dur} for (d, l), dur in agg.items()]
        out.sort(key=lambda x: (x["date"], x["login"]))
        return out

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

    def get_dates_from_range(self,
                             start_date: str,
                             end_date: str,
                             interval: str = "P1M",
                             fmt: str = "%Y-%m-%d %H:%M:%S"
                             ):
        try:
            start = _to_dt(start_date)
            end = _to_dt(end_date)

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
