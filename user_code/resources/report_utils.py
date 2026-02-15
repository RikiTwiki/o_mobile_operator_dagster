# resources/report_utils.py
from dagster import ConfigurableResource
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from utils.report_date_utils import ReportDateUtils

class ReportUtils(ConfigurableResource):
    tz: str = "Asia/Bishkek"
    report_date_str: str | None = None

    def get_utils(self) -> ReportDateUtils:
        if not self.report_date_str:
            today_local = datetime.now(ZoneInfo(self.tz)).date()
            report_day = today_local - timedelta(days=1)
            return ReportDateUtils(report_date_str=report_day.isoformat())
        return ReportDateUtils(report_date_str=self.report_date_str)