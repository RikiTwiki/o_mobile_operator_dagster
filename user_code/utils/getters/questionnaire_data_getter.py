from collections import defaultdict
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Dict, Optional, List, Any
import pandas as pd
from dagster import get_dagster_logger
from utils.path import BASE_SQL_PATH
import psycopg2


class QuestionnaireDataGetter:
    def __init__(
        self,
        conn: psycopg2.extensions.connection,
        start_date: str,
        end_date: str,
        questionnaire_id: int,
        trunc: str = 'day',
        date_format: str = 'YYYY-MM-DD HH24:MI:SS'
        ):

        self.start_date = start_date
        self.end_date = end_date
        self.questionnaire_id = questionnaire_id
        self.conn = conn
        self.trunc = trunc
        self.base_sql_path = Path(BASE_SQL_PATH)

        if date_format is None:
            if self.trunc == 'hour':
                self.date_format = "YYYY-MM-DD HH24:MI:SS"
            elif self.trunc == 'day':
                self.date_format = "YYYY-MM-DD HH24:MI:SS"
            elif self.trunc == 'month':
                self.date_format = "YYYY-MM-DD"
        else:
            self.date_format = date_format

    def get_fullness_data(self) -> pd.DataFrame:
        """
        Получает данные о заполненности анкет с агрегацией по заданному периоду.

        Returns:
            pd.DataFrame: DataFrame с колонками date, filled, not_filled, total
        """
        sql_path = self.base_sql_path / "bpm" / "questionnaire_results.sql"
        sql_template = sql_path.read_text(encoding="utf-8")

        params = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "questionnaire_id": self.questionnaire_id,
            "trunc": self.trunc,
            "date_format": self.date_format,
        }

        get_dagster_logger().info(f"Questionnaire ID: {self.questionnaire_id}")
        get_dagster_logger().info(f"Date range: {self.start_date} - {self.end_date}")
        get_dagster_logger().info(f"Aggregation: {self.trunc}")

        df = pd.read_sql_query(sql_template, self.conn, params=params)
        return df


    def set_hours_trunked_data(self) -> None:
        """
        Настройка агрегации по часам.
        """
        self.trunc = 'hour'
        self.date_format = "YYYY-MM-DD HH24:MI:SS"


    def set_days_trunked_data(self) -> None:
        """
        Настройка агрегации по дням.
        """
        self.trunc = 'day'
        self.date_format = "YYYY-MM-DD HH24:MI:SS"


    def set_months_trunked_data(self) -> None:
        """
        Настройка агрегации по месяцам.
        """
        self.trunc = 'month'
        self.date_format = "YYYY-MM-DD"