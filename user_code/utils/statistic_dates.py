from __future__ import annotations
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from typing import List, Union


class StatisticDates:
    """
    Вычисляет все нужные даты и их форматированные представления
    на основе одной отчётной даты.
    """
    def __init__(
        self,
        start_date: Union[str, date],
        end_date: Union[str, date],
    ):
        # Парсим вход
        if isinstance(start_date, str):
            sd = datetime.fromisoformat(start_date).date()
        else:
            sd = start_date

        if isinstance(end_date, str):
            ed = datetime.fromisoformat(end_date).date()
        else:
            ed = end_date

        # Отчётная дата
        # self.report_date: date = rd
        # self.formatted_report_date: str = rd.strftime("%d.%m.%Y")
        # self.formatted_current_month: str = rd.strftime("%m.%Y")

        # Начало/конец месяца
        # self.month_start: date = rd.replace(day=1)
        # self.month_end: date = (self.month_start + relativedelta(months=1))

        # Периоды в днях
        self.start_date: date = sd
        self.end_date: date = ed
        # self.last_month_start: date = (self.month_start - relativedelta(months=1)).replace(day=1)
        # self.year_start: date = (rd - relativedelta(months=12)).replace(day=1)

        span = (ed - sd).days
        self.dates: List[str] = [
            (sd + timedelta(days=i)).isoformat() for i in range(span + 1)
        ]

        self.formatted_dates: List[str] = [
            (sd + timedelta(days=i)).strftime("%d.%m") for i in range(span + 1)
        ]

        # Диапазон для графика: от rd−7 до rd+1
        # self.dates: List[str] = [
        #     (rd + timedelta(days=offset)).strftime("%Y-%m-%d")
        #     for offset in range(-7, 2)
        # ]
        # self.formatted_dates: List[str] = [
        #     (rd + timedelta(days=offset)).strftime("%d.%m")
        #     for offset in range(-7, 2)
        # ]

    def get_addressees(self, additional: str | None = None) -> List[str]:
        # тут можно заменить на реальный SQLAlchemy/ORM‑запрос
        return ["arenadov@nurtelecom.kg"]

    def get_addressees_by_report_id(
        self, report_id: int, additional: str | None = None
    ) -> List[str]:
        return ["arenadov@nurtelecom.kg"]