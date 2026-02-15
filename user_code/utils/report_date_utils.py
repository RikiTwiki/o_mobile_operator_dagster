#utils.report_date_utils
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

def format_date(date_obj: datetime, fmt: str) -> str:
    return date_obj.strftime(fmt)


def get_dates_from_range(start_date: datetime, end_date: datetime, step_days=1, fmt="%d.%m.%Y"):
    delta = timedelta(days=step_days)
    result = []
    current = start_date
    while current <= end_date:
        result.append({
            "date": current.strftime("%Y-%m-%d"),
            "formatted": current.strftime(fmt),
        })
        current += delta
    return result


def _fmt_from(dt: datetime, pattern: str) -> str:
    return dt.strftime(pattern)


def _get_dates_range(start, end, fmt="%d.%m.%Y"):
    dates = []
    current = start
    while current <= end:
        dates.append({
            "date": current.strftime("%Y-%m-%d"),
            "formatted": current.strftime(fmt)
        })
        current += timedelta(days=1)
    return dates


class ReportDateUtils:
    def __init__(self, report_date_str: str = None):
        if report_date_str is None:
            self.report_dt = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            self.report_dt = datetime.strptime(report_date_str, "%Y-%m-%d")

        self.ReportDate = self._fmt("%Y-%m-%d")
        self.FormattedReportDate = self._fmt("%d.%m.%Y")
        self.FormattedCurrentMonthDate = self._fmt("%m.%Y")

        self.MonthStart = _fmt_from(self.report_dt.replace(day=1), "%Y-%m-%d")
        self.MonthEnd = _fmt_from(self.report_dt.replace(day=1) + relativedelta(months=1), "%Y-%m-%d")

        self.StartDate = _fmt_from(self.report_dt - timedelta(days=31), "%Y-%m-%d")
        self.LastMonthStart = _fmt_from(self.report_dt.replace(day=1) - relativedelta(months=1), "%Y-%m-%d")
        self.EndDate = _fmt_from(self.report_dt + timedelta(days=1), "%Y-%m-%d")

        self.StartDateYear = _fmt_from(self.report_dt.replace(day=1) - relativedelta(months=12), "%Y-%m-%d")

        self.Dates = []
        self.FormattedDates = []

        start = (self.report_dt - timedelta(days=7)).date()
        end = self.report_dt.date()

        for d in _get_dates_range(start, end, fmt="%d.%m"):
            self.Dates.append(d["date"])
            self.FormattedDates.append(d["formatted"])

    def _fmt(self, pattern: str) -> str:
        return self.report_dt.strftime(pattern)
