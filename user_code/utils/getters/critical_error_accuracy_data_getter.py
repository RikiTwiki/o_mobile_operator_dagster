from pathlib import Path
from typing import List, Optional, Dict, Any
import datetime as dt
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

import pandas as pd
from dagster import get_dagster_logger
from sqlalchemy.orm import Session


from resources import source_dwh_resource
from utils.array_operations import df_select_one, _parse_period
from utils.models.staff_unit import StaffUnit
from utils.path import BASE_SQL_PATH
from utils.transformers.mp_report import _to_dt


class CriticalErrorAccuracyDataGetter:
    def __init__(self,
                 start_date,
                 end_date,
                 bpm_conn,
                 trunc = 'day',
                 project_name = "",
                 project_names = None,
                 set_date_format: bool = False,
                 date_field = 'date',
                 conn_jira_nur = None,
                 projects = None
                 ):

        self.start_date = start_date
        self.end_date = end_date
        self.projects = projects
        self.bpm_conn = bpm_conn
        self.conn = source_dwh_resource
        if self.conn == self.bpm_conn:
            get_dagster_logger().info("True")
        else:
            get_dagster_logger().info("False")
        self.trunc = trunc
        self.project_name = project_name
        self.project_names = project_names
        self.set_date_format = set_date_format
        self.date_field = date_field
        self.base_sql_path = Path(BASE_SQL_PATH)
        self.staff_unit_id = []
        self.conn_jira_nur = conn_jira_nur

        self.trunc: str = trunc


        self.period = []
        self._rebuild_period()

    def _rebuild_period(self) -> None:
        start = _to_dt(self.start_date)
        end = _to_dt(self.end_date)

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

    def get_critical_error_accuracy_data_by_staff_unit_id(self, staff_unit_id: int, session: Session):
        unit = session.get(StaffUnit, staff_unit_id)
        if unit is None:
            raise LookupError(f"StaffUnit(id={staff_unit_id}) not found")
        login = unit.login
        get_dagster_logger().info(f"logins staff_unit_id {login}")

        sql_path_complaints = self.base_sql_path / "bpm" / "complaints_data.sql"
        query_complaints = sql_path_complaints.read_text(encoding="utf-8")
        params_complaints = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "staff_unit_id": staff_unit_id,
        }

        complaints_data = pd.read_sql_query(query_complaints, self.bpm_conn, params=params_complaints)
        complaints_data = df_select_one(complaints_data)

        sql_path_jira = self.base_sql_path / "jira" / "jira_query.sql"
        query_jira = sql_path_jira.read_text(encoding="utf-8")

        params_jira = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "login": login,
        }

        jira_mistakes_data = pd.read_sql_query(query_jira, self.conn_jira_nur, params=params_jira)
        jira_mistakes_data = df_select_one(jira_mistakes_data)

        sql_path_qc = self.base_sql_path / "bpm" / "quality_control_data.sql"
        query_qc = sql_path_qc.read_text(encoding="utf-8")

        params_qc = {
            "staff_unit_id": staff_unit_id,
            "start_date": self.start_date,
            "end_date": self.end_date,
        }

        quality_control_data = pd.read_sql_query(query_qc, self.bpm_conn, params=params_qc)
        quality_control_data = df_select_one(quality_control_data)

        all_listened = int(quality_control_data.get("all_listened", 0) or 0)
        fixed_mistakes_listened = int(quality_control_data.get("mistakes", 0) or 0)

        fixed_mistakes_jira = int(jira_mistakes_data.get("confirmed_jira_mistakes", 0) or 0)

        confirmed_complaints = int(complaints_data.get("confirmed_complaints", 0) or 0)
        not_confirmed_complaints = int(complaints_data.get("not_confirmed_complaints", 0) or 0)

        all_checked = (
                all_listened
                + not_confirmed_complaints
                + confirmed_complaints
                + fixed_mistakes_jira
        )

        all_mistakes = (
                fixed_mistakes_listened
                + fixed_mistakes_jira
                + confirmed_complaints
        )

        if all_checked < 30:
            all_checked = 30

        critical_error_accuracy = 100.0 if all_mistakes == 0 else 100.0 - (all_mistakes / all_checked) * 100.0

        result = {
            "unit_id": staff_unit_id,
            "unit_login": login,
            "critical_error_accuracy": critical_error_accuracy,
            "total_mistakes": all_mistakes,
            "fixed_mistakes_jira": fixed_mistakes_jira,
            "fixed_mistakes_listened": fixed_mistakes_listened,
            "confirmed_complaints": confirmed_complaints,
            "not_confirmed_complaints": not_confirmed_complaints,
        }

        get_dagster_logger().info(f"result {result}")
        return result


    def get_aggregated_data(self):
        sql_path = self.base_sql_path / "bpm" / "daily_control_accuracy.sql"
        query = sql_path.read_text(encoding="utf-8")

        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
        }

        df = pd.read_sql_query(query, self.bpm_conn, params=params)

        # 2) приводим к list[dict] и строим индекс по 'date'
        records = df.to_dict("records")
        by_date = {row["date"]: row for row in records}

        items = []
        for period in self.period:
            row = by_date.get(period)  # есть ли запись с такой датой
            dt = _parse_period(period)
            date_str = dt.strftime("%Y-%m-%d %H:%M:%S") if self.set_date_format else dt.strftime("%d.%m.%Y")

            if row is not None:
                items.append({
                    "date": date_str,
                    "hour": dt.strftime("%H:%M:%S"),
                    "all_listened": row.get("all_listened"),
                    "mistakes_count": row.get("mistakes_count"),
                    "critical_error_accuracy": row.get("critical_error_accuracy"),
                    "projectName": self.project_names.get(self.project_name) if self.project_name else None,
                })
            else:
                items.append({
                    "date": date_str,
                    "hour": dt.strftime("%H:%M:%S"),
                    "all_listened": None,
                    "mistakes_count": None,
                    "critical_error_accuracy": None,
                    "projectName": self.project_names.get(self.project_name) if self.project_name else None,
                })

        return {
            "labels": [
                {"field": "date", "title": "Дата"},
                {"field": "all_listened", "title": "Всего оценено"},
                {"field": "mistakes_count", "title": "Всего зафиксировано ошибок"},
                {"field": "critical_error_accuracy", "title": "Critical Error Accuracy"},
            ],
            "items": items,
        }

    def get_aggregated_data_by_project(self):
        sql_path = self.base_sql_path / "bpm" / "daily_control_accuracy_by_project.sql"
        query = sql_path.read_text(encoding="utf-8")

        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
        }

        df = pd.read_sql_query(query, self.bpm_conn, params=params)

        items = []
        for period in self.period:
            dt = datetime.strptime(period, "%Y-%m-%d %H:%M:%S")

            period_rows = df.loc[(df["date"] == period) & (df["group_title"].notna())]

            data_exist = not period_rows.empty


            any_added = False
            if data_exist:
                for _, row in period_rows.iterrows():
                    gt = row["group_title"]
                    if gt in self.project_names:
                        items.append({
                            "date": dt.strftime("%Y-%m-%d %H:%M:%S"),
                            "all_listened": int(row["all_listened"]),
                            "mistakes_count": int(row["mistakes_count"]),
                            "critical_error_accuracy": float(row["critical_error_accuracy"]),
                            "group_title": self.project_names[gt],
                        })
                        any_added = True


            if not data_exist:
                items.append({
                    "date": dt.strftime("%d.%m.%Y"),
                    "hour": dt.strftime("%H:%M:%S"),
                    "all_listened": None,
                    "mistakes_count": None,
                    "critical_error_accuracy": None,
                    "group_title": None,
                })

        return {
            "labels": [
                {"field": "date", "title": "Дата"},
                {"field": "all_listened", "title": "Всего оценено"},
                {"field": "mistakes_count", "title": "Всго зафиксировано ошибок"},
                {"field": "critical_error_accuracy", "title": "Critical Error Accuracy"},
            ],
            "items": items,
        }

    def get_aggregated_data_by_group(self, project_ids=[51, 50, 49, 29, 55, 56, 12]):
        sql_path = self.base_sql_path / "bpm" / "daily_control_accuracy_by_group.sql"
        sql_template = sql_path.read_text(encoding="utf-8")
        projects_filter = (
            "    AND su.position_id = ANY(%(project_ids)s::int[])"
            if project_ids else ""
        )

        query_template = sql_template.format(projects_filter=projects_filter)

        params = {"trunc": self.trunc,
        "date_format": self.date_format,
        "start_date": self.start_date,
        "end_date": self.end_date}

        if project_ids:
            params["project_ids"] = project_ids

        df = pd.read_sql_query(query_template, self.bpm_conn, params=params)
        get_dagster_logger().info(f"result {df.to_dict('records')}")
        return df.to_dict("records")

    def get_aggregated_data_by_project_title(self):
        sql_path = self.base_sql_path / "bpm" / "daily_control_accuracy_by_project_title.sql"
        query = sql_path.read_text(encoding="utf-8")

        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "projects": self.projects,
        }

        df = pd.read_sql_query(query, self.bpm_conn, params=params)
        return df.to_dict("records")


    def get_aggregated_data_by_position(self, position_ids) -> Dict[str, Any]:
        sql_path = self.base_sql_path / "bpm" / "daily_control_accuracy_by_position.sql"
        query = sql_path.read_text(encoding="utf-8")
        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "position_ids": position_ids or [],
        }

        get_dagster_logger().info(f"start_date: {self.start_date}")
        get_dagster_logger().info(f"end_date: {self.end_date}")

        df = pd.read_sql_query(query, self.bpm_conn, params=params)

        items: List[Dict[str, Any]] = []
        for period in self.period:
            dt = _parse_period(period)
            matches = df.loc[df["date"] == period]

            if not matches.empty:
                row = matches.iloc[0]
                items.append({
                    "date": dt.strftime("%d.%m.%Y"),
                    "hour": dt.strftime("%H:%M:%S"),
                    "all_listened": int(row["all_listened"]) if pd.notna(row["all_listened"]) else None,
                    "mistakes_count": int(row["mistakes_count"]) if pd.notna(row["mistakes_count"]) else None,
                    "critical_error_accuracy": float(row["critical_error_accuracy"]) if pd.notna(
                        row["critical_error_accuracy"]) else None,
                })
            else:
                items.append({
                    "date": dt.strftime("%d.%m.%Y"),
                    "hour": dt.strftime("%H:%M:%S"),
                    "all_listened": None,
                    "mistakes_count": None,
                    "critical_error_accuracy": None,
                })

        return {
            "labels": [
                {"field": "date", "title": "Дата"},
                {"field": "all_listened", "title": "Всего оценено"},
                {"field": "mistakes_count", "title": "Всго зафиксировано ошибок"},
                {"field": "critical_error_accuracy", "title": "Critical Error Accuracy"},
            ],
            "items": items,
        }

    def get_aggregated_data_by_staff_unit(self, project_ids= [51, 50, 49, 29, 55, 56, 12]):


        sql_path = self.base_sql_path / "bpm" / "daily_control_accuracy_by_staff_unit.sql"
        sql_tmpl = sql_path.read_text(encoding="utf-8")

        projects_filter = ""
        if project_ids:
            projects_filter = "\n  AND su.position_id = ANY(%(project_ids)s::int[])\n"

        query = sql_tmpl.replace("{projects_filter}", projects_filter)

        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "staff_unit_id": self.staff_unit_id,
            "projects": self.projects or [],
        }
        if project_ids:
            params["project_ids"] = project_ids

        df = pd.read_sql_query(query, self.bpm_conn, params=params)

        items: List[Dict[str, Any]] = []
        for period in self.period:
            dt = _parse_period(period)

            # строки на эту дату (GROUP BY 1 → максимум одна)
            row = df.loc[df["date"] == period]
            if not row.empty:
                r = row.iloc[0]
                date_str = dt.strftime("%Y-%m-%d %H:%M:%S") if self.set_date_format else dt.strftime("%d.%m.%Y")
                items.append({
                    "date": date_str,
                    "hour": dt.strftime("%H:%M:%S"),
                    "all_listened": int(r["all_listened"]) if pd.notna(r["all_listened"]) else None,
                    "mistakes_count": int(r["mistakes_count"]) if pd.notna(r["mistakes_count"]) else None,
                    "critical_error_accuracy": float(r["critical_error_accuracy"]) if pd.notna(
                        r["critical_error_accuracy"]) else None,
                })
            else:
                date_str = dt.strftime("%Y-%m-%d %H:%M:%S") if self.set_date_format else dt.strftime("%d.%m.%Y")
                items.append({
                    "date": date_str,
                    "hour": dt.strftime("%H:%M:%S"),
                    "all_listened": None,
                    "mistakes_count": None,
                    "critical_error_accuracy": None,
                })

        return {
            "labels": [
                {"field": "date", "title": "Дата"},
                {"field": "all_listened", "title": "Всего оценено"},
                {"field": "mistakes_count", "title": "Всeго зафиксировано ошибок"},
                {"field": "critical_error_accuracy", "title": "Critical Error Accuracy"},
            ],
            "items": items,
        }

    def get_has_error_daily_control_data(self, project_ids=None):

        if project_ids is None:
            project_ids = [51, 50, 49, 29, 55, 56, 12]
        sql_path = self.base_sql_path / "bpm" / "has_error_daily_control_data.sql"
        sql_template = sql_path.read_text(encoding="utf-8")

        projects_filter = ""
        if project_ids:
            projects_filter = "\n  AND su.position_id = ANY(%(project_ids)s::int[])\n"

        query = sql_template.format(projects_filter=projects_filter)

        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "staff_unit_id": self.staff_unit_id,
            "projects": self.projects or [],
        }
        if project_ids:
            params["project_ids"] = project_ids

        df = pd.read_sql_query(query, self.bpm_conn, params=params)
        return df.to_dict("records")

    def get_has_note_daily_control_data(self):
        sql_path = self.base_sql_path / "bpm" / "has_note_daily_control_data.sql"
        query = sql_path.read_text(encoding="utf-8")

        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "staff_unit_id": self.staff_unit_id,
            "projects": self.projects,
        }

        df = pd.read_sql_query(query, self.bpm_conn, params=params)
        return df.to_dict("records")

    def get_bot_critical_error_accuracy(
            self,
            staff_unit_id: int,
            project_ids: List[int],
            start_date: str,
            end_date: str,
            session):

        unit = session.get(StaffUnit, staff_unit_id)
        if unit is None:
            raise LookupError(f"StaffUnit(id={staff_unit_id}) not found")
        login = unit.login

        # 2) Если список проектов пуст — как в PHP IN (), данных не будет → вернём нули безопасно
        if not project_ids:
            return {
                "unit_id": staff_unit_id,
                "unit_login": login,
                "critical_error_accuracy": 100.0,
                "total_mistakes": 0,
                "all_checked": 0,
            }

        # 3) SQL с параметрами
        sql_path = self.base_sql_path / "bpm" / "bot_critical_error_accuracy.sql"
        query = sql_path.read_text(encoding="utf-8")
        params = {
            "staff_unit_id": staff_unit_id,
            "start_date": start_date,
            "end_date": end_date,
            "project_ids": project_ids,
        }

        df = pd.read_sql_query(query, self.bpm_conn, params=params)

        # 4) Берём одну строку как selectOne (и NaN → 0)
        row = df_select_one(df)  # ← твой хелпер
        all_checked = int(row.get("all_listened", 0) or 0)
        all_mistakes = int(row.get("mistakes", 0) or 0)

        if all_mistakes == 0:
            critical_error_accuracy = 100.0
        else:
            critical_error_accuracy = 100.0 - (all_mistakes / all_checked) * 100.0

        return {
            "unit_id": staff_unit_id,
            "unit_login": login,
            "critical_error_accuracy": critical_error_accuracy,
            "total_mistakes": all_mistakes,
            "all_checked": all_checked,
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
        get_dagster_logger().info(f"self.date_format: {self.date_format}")

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