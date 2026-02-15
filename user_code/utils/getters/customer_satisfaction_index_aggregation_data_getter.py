import datetime
import pandas as pd
import psycopg2.extensions
from pathlib import Path
from typing import Union, Optional, Dict, List, Any
from dagster import get_dagster_logger
from utils.getters.naumen_projects_getter import NaumenProjectsGetter
from utils.path import BASE_SQL_PATH

from dateutil.relativedelta import relativedelta

import re

from datetime import datetime, date, timedelta

import traceback


class CustomerSatisfactionIndexAggregationDataGetter(NaumenProjectsGetter):
    def __init__(
            self,
            conn: psycopg2.extensions.connection,
            start_date: str,
            end_date: str,
            channels: List[str] = None,
            date_format: str = None,
            set_date_format: bool = False,
            project_name: str = "",
            project_names: Dict[str, str] = None,
            trunc: str = 'day',
            date_field: str = 'date',
            excluded_dates: List[str] = None,
            selected_projects: List[str] = None,
    ):
        super().__init__()
        self.conn = conn
        self.start_date = start_date
        self.end_date = end_date
        self.trunc = trunc

        # Устанавливаем date_format в зависимости от trunc, если не передан
        if date_format is None:
            if self.trunc == 'hour':
                self.date_format = "HH24:MI:SS"
            elif self.trunc == 'day':
                self.date_format = "YYYY-MM-DD HH24:MI:SS"
            elif self.trunc == 'month':
                self.date_format = "YYYY-MM-DD"
            else:
                self.date_format = "YYYY-MM-DD HH24:MI:SS"
        else:
            self.date_format = date_format

        self.set_date_format = set_date_format
        self.project_name = project_name
        self.project_names = project_names or {}
        self.date_field = date_field
        self.channels = channels or []
        self.excluded_dates = excluded_dates or []
        self.selected_projects = selected_projects or []
        self.base_sql_path = Path(BASE_SQL_PATH)


    def get_aggregated_data(self, sender: List[str] = None) -> Dict[str, Any]:
        """
        Получает агрегированные данные по индексу удовлетворенности клиентов.

        :param sender: Список отправителей для фильтрации
        :return: Словарь с метками и данными
        """
        if sender is None:
            sender = []

        sql_path = self.base_sql_path / "stat" / "get_aggregated_data.sql"
        sql_template = sql_path.read_text(encoding="utf-8")

        # Условные фильтры
        channels_filter = (
            "\n  AND channel_title = ANY(%(channels)s::text[])"
            if self.channels else ""
        )

        sender_filter = (
            "\n  AND aqerd.sender = ANY(%(sender)s::text[])"
            if sender else ""
        )

        excluded_dates_filter = (
            "\n  AND NOT (to_char(date_trunc('day', omni_inbound_subscribers_requests.request_ended_at), %(date_format)s) = ANY(%(excluded_dates)s::text[]))"
            if self.excluded_dates else ""
        )

        query = sql_template.format(
            channels_filter=channels_filter,
            sender_filter=sender_filter,
            excluded_dates_filter=excluded_dates_filter,
        )

        params = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "trunc": self.trunc,
            "date_format": self.date_format,
        }

        if self.channels:
            params["channels"] = self.channels
        if sender:
            params["sender"] = sender
        if self.excluded_dates:
            params["excluded_dates"] = self.excluded_dates

        get_dagster_logger().info(f"Channels: {self.channels}")
        get_dagster_logger().info(f"Sender: {sender}")
        get_dagster_logger().info(f"Excluded dates: {self.excluded_dates}")

        df = pd.read_sql_query(query, self.conn, params=params)

        # Обработка результатов
        data = []
        for _, row in df.iterrows():
            item = {
                'date': self._format_date(row['date']) if self.set_date_format else row['date'],
                'all_contacts': int(row['all_contacts']),
                'with_marks': int(row['with_marks']),
                'good_marks': int(row['good_marks']),
                'neutral_marks': int(row['neutral_marks']),
                'bad_marks': int(row['bad_marks']),
                'all_marks': int(row['good_marks']) + int(row['bad_marks'])
            }

            # Расчет индекса удовлетворенности клиентов
            item['customer_satisfaction_index'] = self._calculate_csi(
                int(row['good_marks']), int(row['bad_marks'])
            )

            # Расчет конверсии
            item['conversion'] = self._calculate_conversion(
                int(row['all_contacts']), int(row['good_marks']),
                int(row['bad_marks']), int(row['neutral_marks'])
            )

            item['project_name'] = self.project_names.get(self.project_name) if self.project_name else None
            data.append(item)

        return {
            'labels': [
                {'field': 'date', 'title': 'Дата'},
                {'field': 'all_contacts', 'title': 'all_contacts'},
                {'field': 'with_marks', 'title': 'with_marks'},
                {'field': 'good_marks', 'title': 'good_marks'},
                {'field': 'all_marks', 'title': 'all_marks'},
                {'field': 'neutral_marks', 'title': 'neutral_marks'},
                {'field': 'bad_marks', 'title': 'bad_marks'},
                {'field': 'customer_satisfaction_index', 'title': 'customer_satisfaction_index'}
            ],
            'items': data
        }

    def get_csi_by_staff_unit_id(self, staff_unit_id: int) -> Dict[str, Any]:
        """
        Получает CSI по ID штатной единицы.

        :param staff_unit_id: ID штатной единицы
        :return: Словарь с метками и данными
        """
        sql_path = self.base_sql_path / "stat" / "get_csi_by_staff_unit_id.sql"
        sql_template = sql_path.read_text(encoding="utf-8")

        selected_projects_filter = (
            "\n  AND project_id::text = ANY(%(selected_projects)s::text[])"
            if self.selected_projects else ""
        )

        excluded_dates_filter = (
            "\n  AND NOT (to_char(date_trunc('day', omni_inbound_subscribers_requests.request_ended_at), %(date_format)s) = ANY(%(excluded_dates)s::text[]))"
            if self.excluded_dates else ""
        )

        query = sql_template.format(
            selected_projects_filter=selected_projects_filter,
            excluded_dates_filter=excluded_dates_filter,
        )

        params = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "trunc": self.trunc,
            "date_format": self.date_format,
            "staff_unit_id": staff_unit_id,
        }

        if self.selected_projects:
            params["selected_projects"] = self.selected_projects
        if self.excluded_dates:
            params["excluded_dates"] = self.excluded_dates

        get_dagster_logger().info(f"Selected projects: {self.selected_projects}")
        get_dagster_logger().info(f"Excluded dates: {self.excluded_dates}")

        df = pd.read_sql_query(query, self.conn, params=params)

        # Обработка результатов
        data = []
        for _, row in df.iterrows():
            item = {
                'date': row['date'],
                'all_contacts': int(row['all_contacts']),
                'with_marks': int(row['with_marks']),
                'good_marks': int(row['good_marks']),
                'neutral_marks': int(row['neutral_marks']),
                'bad_marks': int(row['bad_marks']),
                'all_marks': int(row['good_marks']) + int(row['bad_marks'])
            }

            item['customer_satisfaction_index'] = self._calculate_csi(
                int(row['good_marks']), int(row['bad_marks'])
            )
            item['conversion'] = self._calculate_conversion(
                int(row['all_contacts']), int(row['good_marks']),
                int(row['bad_marks']), int(row['neutral_marks'])
            )

            data.append(item)

        return {
            'labels': [
                {'field': 'date', 'title': 'Дата'},
                {'field': 'all_contacts', 'title': 'all_contacts'},
                {'field': 'with_marks', 'title': 'with_marks'},
                {'field': 'good_marks', 'title': 'good_marks'},
                {'field': 'all_marks', 'title': 'all_marks'},
                {'field': 'neutral_marks', 'title': 'neutral_marks'},
                {'field': 'bad_marks', 'title': 'bad_marks'},
                {'field': 'customer_satisfaction_index', 'title': 'customer_satisfaction_index'}
            ],
            'items': data
        }

    def get_csi_by_staff_by_user_login(self, user_login: str) -> Dict[str, Any]:
        """
        Получает CSI по логину пользователя.

        :param user_login: Логин пользователя
        :return: Словарь с метками и данными
        """
        sql_path = self.base_sql_path / "stat" / "get_csi_by_user_login.sql"
        sql_template = sql_path.read_text(encoding="utf-8")

        query = sql_template  # Здесь нет условных фильтров

        params = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "trunc": self.trunc,
            "date_format": self.date_format,
            "user_login": user_login,
        }

        get_dagster_logger().info(f"User login: {user_login}")

        df = pd.read_sql_query(query, self.conn, params=params)

        # Обработка результатов
        data = []
        for _, row in df.iterrows():
            item = {
                'date': row['date'],
                'all_contacts': int(row['all_contacts']),
                'with_marks': int(row['with_marks']),
                'good_marks': int(row['good_marks']),
                'neutral_marks': int(row['neutral_marks']),
                'bad_marks': int(row['bad_marks']),
                'all_marks': int(row['good_marks']) + int(row['bad_marks'])
            }

            item['customer_satisfaction_index'] = self._calculate_csi(
                int(row['good_marks']), int(row['bad_marks'])
            )
            item['conversion'] = self._calculate_conversion(
                int(row['all_contacts']), int(row['good_marks']),
                int(row['bad_marks']), int(row['neutral_marks'])
            )

            data.append(item)

        return {
            'labels': [
                {'field': 'date', 'title': 'Дата'},
                {'field': 'all_contacts', 'title': 'all_contacts'},
                {'field': 'with_marks', 'title': 'with_marks'},
                {'field': 'good_marks', 'title': 'good_marks'},
                {'field': 'all_marks', 'title': 'all_marks'},
                {'field': 'neutral_marks', 'title': 'neutral_marks'},
                {'field': 'bad_marks', 'title': 'bad_marks'},
                {'field': 'customer_satisfaction_index', 'title': 'customer_satisfaction_index'}
            ],
            'items': data
        }

    def get_combined_rating_result(self, group: List[str], channel: List[str]) -> List[Dict[str, Any]]:
        """
        Получает объединенный результат рейтинга.

        :param group: Список групп
        :param channel: Список каналов
        :return: Список данных
        """
        sql_path = self.base_sql_path / "stat" / "get_combined_rating_result.sql"
        sql_template = sql_path.read_text(encoding="utf-8")

        date_field = self.date_field if self.date_field in ("hour", "date") else "date"

        query = sql_template.format(date_field=date_field)

        get_dagster_logger().info(f"start_date_in_method: {self.start_date}")
        get_dagster_logger().info(f"end_date_in_method: {self.end_date}")

        params = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "trunc": self.trunc,
            "date_format": self.date_format,
            "group": list(group),
            "channel": list(channel),
        }

        get_dagster_logger().info(f"Groups: {group}")
        get_dagster_logger().info(f"Channels: {channel}")

        df = pd.read_sql_query(query, self.conn, params=params)

        # Обработка результатов
        data = []
        for _, row in df.iterrows():
            item = {
                date_field: row[date_field],
                "all_contacts": int(row["all_contacts"]),
                "total_marks": int(row["total_marks"]),
                "good_marks": int(row["good_marks"]),
                "neutral_marks": int(row["neutral_marks"]),
                "bad_marks": int(row["bad_marks"]),
                "all_marks": int(row["good_marks"]) + int(row["bad_marks"]),
            }

            item["customer_satisfaction_index"] = self._calculate_csi(item["good_marks"], item["bad_marks"])
            item["conversion"] = self._calculate_conversion(
                item["all_contacts"], item["good_marks"], item["bad_marks"], item["neutral_marks"]
            )
            data.append(item)

        return data

    def get_combined_rating_result_with_group(self, group: List[str], channel: List[str]) -> List[Dict[str, Any]]:
        """
        Получает объединенный результат рейтинга с группировкой.

        :param group: Список групп
        :param channel: Список каналов
        :return: Список данных
        """
        sql_path = self.base_sql_path / "stat" / "get_combined_rating_result_with_group.sql"
        sql_template = sql_path.read_text(encoding="utf-8")

        query = sql_template.format(date_field = self.date_field)

        params = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "trunc": self.trunc,
            "date_format": self.date_format,
            "group": group,
            "channel": channel,
        }

        df = pd.read_sql_query(query, self.conn, params=params)

        data = []
        for _, row in df.iterrows():
            item = {
                self.date_field: row[self.date_field],
                'group_title': row['group_title'],
                'all_contacts': int(row['all_contacts']),
                'total_marks': int(row['total_marks']),
                'good_marks': int(row['good_marks']),
                'neutral_marks': int(row['neutral_marks']),
                'bad_marks': int(row['bad_marks']),
                'all_marks': int(row['good_marks']) + int(row['bad_marks'])
            }

            item['customer_satisfaction_index'] = self._calculate_csi(
                int(row['good_marks']), int(row['bad_marks'])
            )
            item['conversion'] = self._calculate_conversion(
                int(row['all_contacts']), int(row['good_marks']),
                int(row['bad_marks']), int(row['neutral_marks'])
            )

            data.append(item)

        return data

    def set_hours_truncated_data(self) -> None:
        """Функция для настройки агрегации по часам."""
        self.trunc = 'hour'
        self.date_format = 'HH24:MI:SS'

    def set_days_truncated_data(self) -> None:
        """Функция для настройки агрегации по дням."""
        self.trunc = 'day'
        self.date_format = 'YYYY-MM-DD HH24:MI:SS'

    def set_months_truncated_data(self) -> None:
        """Функция для настройки агрегации по месяцам."""
        self.trunc = 'month'
        self.date_format = 'YYYY-MM-DD'

    def set_exclude_dates(self, dates: List[str]) -> None:
        """
        Вставить даты, которые не нужно смотреть.

        :param dates: Список дат для исключения
        """
        self.excluded_dates = dates

    def _calculate_csi(self, good_marks: int, bad_marks: int) -> Optional[float]:
        """
        Расчет индекса удовлетворенности клиентов.

        :param good_marks: Количество хороших оценок
        :param bad_marks: Количество плохих оценок
        :return: Индекс удовлетворенности или None
        """
        if good_marks == 0 and bad_marks == 0:
            return None
        elif good_marks == 0:
            return 0.0
        elif bad_marks == 0:
            return 100.0
        else:
            return (good_marks / (good_marks + bad_marks)) * 100

    def _calculate_conversion(self, all_contacts: int, good_marks: int,
                              bad_marks: int, neutral_marks: int) -> Optional[float]:
        """
        Расчет конверсии.

        :param all_contacts: Общее количество контактов
        :param good_marks: Количество хороших оценок
        :param bad_marks: Количество плохих оценок
        :param neutral_marks: Количество нейтральных оценок
        :return: Конверсия или None
        """
        if all_contacts == 0:
            return None
        elif good_marks == 0 and bad_marks == 0 and neutral_marks == 0:
            return 0.0
        else:
            return ((good_marks + bad_marks + neutral_marks) / all_contacts) * 100

    def _format_date(self, date_str: str) -> str:
        """
        Форматирование даты.

        :param date_str: Строка с датой
        :return: Отформатированная дата
        """
        try:
            dt = datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError):
            return date_str

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
        self.date_format = "YYYY-MM-DD HH24:MI:SS"  # для SQL

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