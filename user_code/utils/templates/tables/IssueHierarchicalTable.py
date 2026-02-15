from typing import Any, Dict, List, Optional, Union
from datetime import datetime, timedelta
import html


class IssueHierarchicalTable:
    # ==== Публичные поля (как в PHP) ====
    Items: List[Dict[str, Any]]

    TableStyle: str = "border:1px solid #32383e; border-collapse:collapse; font-size:12px;"
    THStyle: str = "color:#fff; background-color:#43484c; border:1px solid #32383e; text-align:center; padding:4px;"
    TDStyle: str = "border:1px solid #858585; text-align:center; padding:4px;"
    TRStyle: str = ""
    TitleStyle: Optional[str] = None

    Top: int = 10
    ShowSummaryXAxis: bool = True

    def __init__(self) -> None:
        self.Items = []
        self.CurrentRank = 0
        self.TableBody = ""
        self.RowData = ""
        self.DatePeriod: List[Dict[str, str]] = []
        self.columnMinMax: Dict[int, Dict[str, int]] = {}
        self.raw: Optional[Dict[str, List[Dict[str, Any]]]] = None

    # ==== Публичные методы ====
    def setRawData(self, data: Dict[str, List[Dict[str, Any]]]) -> None:
        self.raw = data
        self.Items = []
        for title, records in (self.raw or {}).items():
            m: Dict[str, Dict[str, Any]] = {}
            for rec in records:
                r_type = rec.get("subtask_type") or "Нет подзадач"
                if r_type not in m:
                    m[r_type] = {
                        "subtask_title": r_type,
                        "date": [],
                        "total_issues": [],
                        "open": 0,
                        "closed": 0,
                        "parent_issue_count": 0,
                        "daughter_issue_count": 0,
                    }
                m[r_type]["date"].append(rec.get("date"))
                m[r_type]["total_issues"].append(rec.get("count"))
            self.Items.append({
                "title": title,
                "subtasks": list(m.values()),
            })

    def getTable(self) -> str:
        self.constructBody()
        return self.renderTable()

    # ==== Приватные методы ====
    def constructBody(self) -> None:
        if self.Top > 0 and len(self.Items) > self.Top:
            self.Items = self.Items[: self.Top]

        allDates: List[str] = []
        for task in self.Items:
            for sub in task.get("subtasks", []) or []:
                dates = sub.get("date")
                if isinstance(dates, list):
                    allDates.extend([d for d in dates if d])
        if not allDates:
            raise Exception("Нет ни одной даты в подзадачах.")

        minDate = min(allDates)
        maxDate = max(allDates)
        self.DatePeriod = self.getDaysTrunkedData(minDate, maxDate, "%d.%m")

        # Заголовки
        self.genCellTH("Rank")
        self.genCellTH("Основная заявка")
        self.genCellTH("Подзадачи")
        for d in self.DatePeriod:
            self.genCellTH(d["formatted"])
        self.genCellTH("Итого")
        self.genCellTH("Среднее")
        self.genCellTH("Мин.")
        self.genCellTH("Макс.")
        self.genRow()

        totalTasks = len(self.Items)
        rank = 1

        for task in self.Items:
            self.CurrentRank = rank
            title = task.get("title", "")
            subtasks = task.get("subtasks", []) or []

            flat: List[int] = []
            for s in subtasks:
                for v in (s.get("total_issues") or []):
                    if v not in ("", None):
                        try:
                            flat.append(int(v))
                        except Exception:
                            pass
            globalMin = min(flat) if flat else 0
            globalMax = max(flat) if flat else 0

            if not subtasks:
                subtasks = [{
                    "date": [],
                    "subtask_title": "Нет подзадач",
                    "total_issues": [],
                    "open": 0,
                    "closed": 0,
                    "parent_issue_count": 0,
                    "daughter_issue_count": 0,
                }]
            rowspanLen = max(1, len(subtasks))

            for idx, sub in enumerate(subtasks):
                if idx == 0:
                    self.genCellTD(
                        str(rank),
                        extraAttrs=f'rowspan="{rowspanLen}"',
                        extraStyle="font-weight:bold; vertical-align:middle;"
                    )
                    self.genCellTD(
                        html.escape(str(title)),
                        extraAttrs=f'rowspan="{rowspanLen}"',
                        extraStyle="font-weight:bold; vertical-align:middle;"
                    )

                self.genCellTD(html.escape(str(sub.get("subtask_title", ""))))

                valuesForSub: List[int] = []
                subDates = sub.get("date") or []
                subVals = sub.get("total_issues") or []

                for dateItem in self.DatePeriod:
                    cellValue = ""
                    try:
                        keyIdx = subDates.index(dateItem["date"])
                    except ValueError:
                        keyIdx = -1
                    if keyIdx != -1 and keyIdx < len(subVals):
                        cellValue = subVals[keyIdx]
                        try:
                            valuesForSub.append(int(cellValue))
                        except Exception:
                            pass

                    cellValue = ""
                    try:
                        keyIdx = subDates.index(dateItem["date"])
                    except ValueError:
                        keyIdx = -1
                    if keyIdx != -1 and keyIdx < len(subVals):
                        cellValue = subVals[keyIdx]
                        try:
                            valuesForSub.append(int(cellValue))
                        except Exception:
                            pass

                    # фон по глобальным min/max для этой задачи
                    extraStyle = ""
                    if cellValue not in ("", None):
                        try:
                            color = self.gradientColour(int(cellValue), globalMin, globalMax, "asc")
                            if color:
                                extraStyle = f"background-color:{color};"
                        except Exception:
                            pass

                    self.genCellTD(str(cellValue), extraAttrs=None, extraStyle=extraStyle)

                if valuesForSub:
                    s = sum(valuesForSub)
                    mn = min(valuesForSub)
                    mx = max(valuesForSub)
                    avg = round(s / len(valuesForSub), 1)
                else:
                    s = mn = mx = avg = ""
                self.genCellTD(str(s))
                self.genCellTD(str(avg))
                self.genCellTD(str(mn))
                self.genCellTD(str(mx))

                self.genRow()

            if rank < totalTasks:
                blanks = 3 + len(self.DatePeriod) + 4
                blankRow = ''.join('<td style="border: none; padding: 5px; height: 10px;"></td>'
                                   for _ in range(blanks))
                self.genRow(blankRow)

            rank += 1

    def renderTable(self) -> str:
        return f"<table style=\"{self.TableStyle}\">{self.TableBody}</table>"

    def genCellTD(self, cellData: Union[str, int, float],
                  extraAttrs: Optional[str] = None,
                  extraStyle: str = "") -> None:
        attrs = extraAttrs or ""
        style = self.TDStyle + extraStyle
        content = html.escape(str(cellData))
        self.RowData += f"<td {attrs} style=\"{style}\">{content}</td>"

    def genCellTH(self, cellData: str, extraAttrs: Optional[str] = None) -> None:
        attrs = extraAttrs or ""
        style = self.THStyle
        content = html.escape(cellData)
        self.RowData += f"<th style=\"{style}\" {attrs}>{content}</th>"

    def genRow(self, customRowData: Optional[str] = None) -> None:
        content = self.RowData if customRowData is None else customRowData
        if customRowData is None and (self.CurrentRank % 2 == 1):
            bg = 'background-color:#f0f0f0;'
        else:
            bg = ''
        self.TableBody += f"<tr style=\"{bg} {self.TRStyle}\">{content}</tr>"
        self.RowData = ""

    def gradientColour(self, current: int, min_v: int, max_v: int, type_: str = "asc") -> str:
        if max_v == min_v:
            return ""  # все значения одинаковые — фон не нужен
        pct = round(((current - min_v) / (max_v - min_v)) * 100.0, 0)
        if type_ == "desc":
            pct = 100 - pct
        steps = [0, 10, 35, 65, 95, 100]
        palette = {
            0: "#FFEBEE",
            10: "#FFD7DE",
            35: "#FFC3CE",
            65: "#FFAFBE",
            95: "#FF9CAE",
            100: "#FF4D6E",
        }
        key = max(s for s in steps if s <= pct)
        return palette.get(key, "")

    # ==== Датовые утилиты ====
    def getDaysTrunkedData(self, startDate: str, endDate: str,
                           fmt: Optional[str] = None) -> List[Dict[str, str]]:
        endObj = datetime.strptime(endDate, "%Y-%m-%d") + timedelta(days=1)
        return self.getDatesFromRange(startDate, endObj.strftime("%Y-%m-%d"), fmt=fmt)

    def getDatesFromRange(self, startDate: str, endDate: str,
                          fmt: Optional[str] = None) -> List[Dict[str, str]]:
        arr: List[Dict[str, str]] = []
        startObj = datetime.strptime(startDate, "%Y-%m-%d")
        endObj = datetime.strptime(endDate, "%Y-%m-%d")
        cur = startObj
        while cur < endObj:
            arr.append({
                'date': cur.strftime('%Y-%m-%d'),
                'formatted': cur.strftime(fmt) if fmt else cur.strftime('%Y-%m-%d'),
            })
            cur += timedelta(days=1)
        return arr


