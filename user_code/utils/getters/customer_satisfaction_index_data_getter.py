from pathlib import Path
import datetime as dt
from typing import List
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd

from utils.array_operations import _parse_period
from utils.getters.naumen_projects_getter import NaumenProjectsGetter
from utils.path import BASE_SQL_PATH
from utils.transformers.mp_report import try_parse_period


class CustomerSatisfactionIndexDataGetter (NaumenProjectsGetter):
    def __init__(self, bpm_conn, start_date, end_date, trunc=None, project_name="", project_names=None, set_date_format=False):
        super().__init__()
        self.bpm_conn = bpm_conn
        self.start_date = start_date
        self.end_date = end_date
        self.trunc = trunc

        self.base_sql_path = Path(BASE_SQL_PATH)
        self.project_name = project_name
        if project_names is None:
            self.project_names = []

        self.set_date_format = set_date_format
        self.period = []
        self._rebuild_period()
        if self.trunc == "day":
            self.date_format = "YYYY-MM-DD HH24:MI:SS"
        elif self.trunc == "hour":
            self.date_format = "YYYY-MM-DD HH:MI:SS"
        elif self.trunc == "month":
            self.date_format = "YYYY-MM-DD"
        else:
            raise ValueError("trunc must be 'hour' | 'day' | 'month'")


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


    def get_customer_satisfaction_index_by_staff_unit_id(self, staff_unit_id, start_date, end_date, excluded_dates):
        sql_path = self.base_sql_path / "stat" / "replication_slr_employees_rating_data.sql"
        query_template = sql_path.read_text(encoding="utf-8")

        # если есть исключённые даты
        if excluded_dates:
            excluded_filter = "AND date_trunc('day', rserd.first_answer_received_at) NOT IN %(excluded_dates)s"
        else:
            excluded_filter = ""

        query = query_template.format(excluded_filter=excluded_filter)

        params = {
            "staff_unit_id": staff_unit_id,
            "start_date": start_date,
            "end_date": end_date,
        }
        if excluded_dates:
            params["excluded_dates"] = tuple(excluded_dates)
        df = pd.read_sql_query(query, self.bpm_conn, params=params)
        data = df.to_dict("records")

        good_marks = 0
        neutral_marks = 0
        bad_marks = 0
        form_fill_answers = 0
        sms_answers = 0

        for item in data:
            mark = item.get("mark")
            fixed_answer_type = item.get("fixed_answer_type")

            if mark in (5, 4):
                good_marks += 1
            if mark == 3:
                neutral_marks += 1
            if mark in (2, 1, 0):
                bad_marks += 1

            if fixed_answer_type == "form_fill" and mark is not None:
                form_fill_answers += 1
            if fixed_answer_type == "sms" and mark is not None:
                sms_answers += 1

        # Customer Satisfaction Index: good / (good + bad) * 100
        if good_marks > 0 and (good_marks + bad_marks) > 0:
            customer_satisfaction_index = (good_marks / (good_marks + bad_marks)) * 100
        else:
            customer_satisfaction_index = 0

        # Если есть хотя бы одна запись → берём user_login из первой
        if data:
            return {
                "staff_unit_id": staff_unit_id,
                "user_login": data[0].get("user_login"),
                "customer_satisfaction_index": customer_satisfaction_index,
                "good_marks": good_marks,
                "neutral_marks": neutral_marks,
                "bad_marks": bad_marks,
                "form_fill_answers": form_fill_answers,
                "sms_answers": sms_answers,
            }

        # Иначе — все значения null
        return {
            "staff_unit_id": staff_unit_id,
            "user_login": None,
            "customer_satisfaction_index": None,
            "good_marks": None,
            "neutral_marks": None,
            "bad_marks": None,
            "form_fill_answers": None,
            "sms_answers": None,
        }

    def get_customer_satisfaction_index(
            self,
            start_date: str,
            end_date: str,
            excluded_dates: List[str] | None = None,
    ):
        sql_path = self.base_sql_path / "stat" / "customer_satisfaction_index.sql"
        query_template = sql_path.read_text(encoding="utf-8")

        if excluded_dates:
            excluded_filter = "AND date_trunc('day', rserd.first_answer_received_at) NOT IN %(excluded_dates)s"
        else:
            excluded_filter = ""

        query = query_template.format(excluded_filter=excluded_filter)

        params = {
            "start_date": start_date,
            "end_date": end_date,
        }
        if excluded_dates:
            params["excluded_dates"] = tuple(excluded_dates)

        df = pd.read_sql_query(query, self.bpm_conn, params=params)
        data = df.to_dict("records")

        good_marks = 0
        neutral_marks = 0
        bad_marks = 0
        form_fill_answers = 0
        sms_answers = 0

        for item in data:
            mark = item.get("mark")
            fixed_answer_type = item.get("fixed_answer_type")

            if mark in (5, 4):
                good_marks += 1
            if mark == 3:
                neutral_marks += 1
            if mark in (2, 1, 0):
                bad_marks += 1

            if fixed_answer_type == "form_fill" and mark is not None:
                form_fill_answers += 1
            if fixed_answer_type == "sms" and mark is not None:
                sms_answers += 1

        customer_satisfaction_index = (good_marks / (good_marks + bad_marks)) * 100 if (good_marks + bad_marks) > 0 else 0

        return {
            "customer_satisfaction_index": customer_satisfaction_index,
            "good_marks": good_marks,
            "neutral_marks": neutral_marks,
            "bad_marks": bad_marks,
            "form_fill_answers": form_fill_answers,
            "sms_answers": sms_answers,
        }

    def get_aggregated_csi_slr_data(
            self,
            channel_ids: List[int] | None = None,
            excluded_dates: List[str] | None = None,
    ):
        sql_path = self.base_sql_path / "stat" / "aggregated_csi_slr_data.sql"
        query_tmpl = sql_path.read_text(encoding="utf-8")

        if excluded_dates:
            excluded_filter = "AND date_trunc('day', rserd.first_answer_received_at) NOT IN %(excluded_dates)s"
        else:
            excluded_filter = ""

        query = query_tmpl.replace("{excluded_filter}", excluded_filter)

        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "channel_ids": channel_ids or [205],
        }
        if excluded_dates:
            params["excluded_dates"] = tuple(excluded_dates)

        df = pd.read_sql_query(query, self.bpm_conn, params=params)
        return df.to_dict("records")

    def get_aggregated_data(self):
        """
        Аналог PHP getAggregatedData():
        - агрегирует по дате (trunc + to_char)
        - считает all_contacts/with_marks/good/neutral/bad
        - досчитывает customer_satisfaction_index, conversion, all_marks
        - формирует items по self.period
        Требует у self:
          - start_date, end_date, trunc, date_format
          - period: List[str]  (строки дат как в SQL 'date')
          - set_date_format: bool  (форматировать 'date' в выдаче как в PHP)
          - project_name: Optional[str], project_names: Dict[str, str] (для поля projectName)
          - selected_projects: List[int]
          - bpm_conn
          - хелпер _parse_period(period: str)
        """

        # 1) читаем запрос
        sql_path = self.base_sql_path / "stat" / "omni_inbound_aggregated.sql"
        query = sql_path.read_text(encoding="utf-8")

        params = {
            "trunc": self.trunc,  # 'hour' | 'day' | 'month'
            "date_format": self.date_format,  # 'YYYY-MM-DD HH24:MI:SS' | 'YYYY-MM-DD'
            "start_date": self.start_date,
            "end_date": self.end_date,
            "directions": ["inbound", "in"],
            "selected_projects": self.SelectedProjects or [],
        }

        df = pd.read_sql_query(query, self.bpm_conn, params=params)

        if not df.empty:
            df["all_marks"] = (df["good_marks"].fillna(0).astype(int)
                               + df["bad_marks"].fillna(0).astype(int))

            g = df["good_marks"].fillna(0).astype(int)
            b = df["bad_marks"].fillna(0).astype(int)
            denom = (g + b)

            csi = []
            for gi, bi, di in zip(g.tolist(), b.tolist(), denom.tolist()):
                if gi == 0 and bi == 0:
                    csi.append(None)
                elif gi == 0:
                    csi.append(0.0)
                elif bi == 0:
                    csi.append(100.0)
                else:
                    csi.append(gi / di * 100.0)
            df["customer_satisfaction_index"] = csi
            n = df["neutral_marks"].fillna(0).astype(int)
            all_contacts = df["all_contacts"].fillna(0).astype(int)

            conv = []
            for gi, bi, ni, ac in zip(g.tolist(), b.tolist(), n.tolist(), all_contacts.tolist()):
                if ac == 0:
                    conv.append(None)
                elif gi == 0 and bi == 0 and ni == 0:
                    conv.append(0.0)
                else:
                    conv.append((gi + bi + ni) / ac * 100.0)
            df["conversion"] = conv

        records = df.to_dict("records")
        by_date = {r["date"]: r for r in records}

        items = []
        for period in self.period:
            row = by_date.get(period)
            dt = _parse_period(period)

            date_str = dt.strftime("%Y-%m-%d %H:%M:%S") if self.set_date_format else dt.strftime("%d.%m.%Y")

            base = {
                "date": date_str,
                "hour": dt.strftime("%H:%M:%S"),
                "projectName": self.project_names.get(self.project_name) if getattr(self, "project_name",
                                                                                    None) else None,
            }

            if row is not None:
                items.append({
                    **base,
                    "all_contacts": int(row.get("all_contacts", 0)) if row.get("all_contacts") is not None else None,
                    "with_marks": int(row.get("with_marks", 0)) if row.get("with_marks") is not None else None,
                    "good_marks": int(row.get("good_marks", 0)) if row.get("good_marks") is not None else None,
                    "all_marks": int(row.get("all_marks", 0)) if row.get("all_marks") is not None else None,
                    "neutral_marks": int(row.get("neutral_marks", 0)) if row.get("neutral_marks") is not None else None,
                    "bad_marks": int(row.get("bad_marks", 0)) if row.get("bad_marks") is not None else None,
                    "customer_satisfaction_index": row.get("customer_satisfaction_index", None),
                    "conversion": row.get("conversion", None),
                })
            else:
                items.append({
                    **base,
                    "all_contacts": None,
                    "with_marks": None,
                    "good_marks": None,
                    "all_marks": None,
                    "neutral_marks": None,
                    "bad_marks": None,
                    "customer_satisfaction_index": None,
                    "conversion": None,
                })

        return {
            "labels": [
                {"field": "date", "title": "Дата"},
                {"field": "all_contacts", "title": "all_contacts"},
                {"field": "with_marks", "title": "with_marks"},
                {"field": "good_marks", "title": "good_marks"},
                {"field": "all_marks", "title": "all_marks"},
                {"field": "neutral_marks", "title": "neutral_marks"},
                {"field": "bad_marks", "title": "bad_marks"},
                {"field": "customer_satisfaction_index", "title": "customer_satisfaction_index"},
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
        self.date_format = "YYYY-MM-DD HH24:MI:SS"  # для SQL

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

