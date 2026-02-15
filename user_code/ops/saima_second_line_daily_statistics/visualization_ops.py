# ops/saima_second_line/visualization_ops.py
from __future__ import annotations

import base64
import io
import math
from typing import Any, Dict, List, Tuple, Optional

from dagster import op, get_dagster_logger
from matplotlib import pyplot as plt

from ops.csf_daily_statistics.visualizations_ops import render_csf_basic_graphs
# Подстрой под ваши пути:
from utils.templates.tables.table_constructor import TableConstructor
from utils.templates.tables.pivot_table import PivotTable
from utils.templates.charts.multi_axis_chart import MultiAxisChart   # или ваш matplotlib MultiAxisChart
from utils.templates.charts.basic_line_graphics import BasicLineGraphics

from utils.transformers.mp_report import smart_number, _normalize, _parse_dt_any
from datetime import datetime, timedelta
from pathlib import Path

from dagster import op

from utils.mail.mail_report import MailReport, html_to_pdf_page, merge_pdfs

from decimal import Decimal, ROUND_HALF_UP

import matplotlib.dates as mdates
import matplotlib.transforms as mtransforms
from matplotlib.ticker import FuncFormatter

_STATUS_PRIORITY = {
    "Помогли": 1,
    "Не помогли": 2,
    "Другое": 3,
    "Причина на стороне абонента": 4,
    "Отмена заявки": 5,
    "Проблемы на линии/ с оборудованием": 6,
}

_SERIES_KEYS = [
    "resolved",
    "not_resolved",
    "infrastructure_hardware_issue",
    "ticket_cancel",
    "client_side_reason",
]

def _compute_max_y(items: List[Dict[str, Any]]) -> int:
    mx = 0
    for it in items or []:
        if "count" in it and isinstance(it.get("count"), (int, float)):
            total = float(it.get("count") or 0)
        else:
            total = 0.0
            for k in _SERIES_KEYS:
                v = it.get(k)
                try:
                    total += float(v or 0)
                except Exception:
                    pass
        if total > mx:
            mx = total
    # небольшой запас сверху, но не меньше 1
    return int(mx if mx > 0 else 1)

def _norm_day(s: str | None) -> str:
    return (str(s)[:10]) if s else ""  # 'YYYY-MM-DD'

def _build_group_ids(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    """Стабильные id для пар (status|title), начиная с 1."""
    group_ids: Dict[str, int] = {}
    next_id = 1
    for it in rows:
        key = f"{it.get('status','')}|{it.get('title','')}"
        if key not in group_ids:
            group_ids[key] = next_id
            next_id += 1
    return group_ids

def _group_by_status_with_ids(rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """Готовит {status: [ {id,status,title,date,count}, ... ]} с нормализованной датой."""
    norm_rows: List[Dict[str, Any]] = [
        {
            "status": it.get("status") or "",
            "title": it.get("title") or "",
            "date": _norm_day(it.get("date")),
            "count": int(it.get("count") or 0),
        }
        for it in rows
    ]
    group_ids = _build_group_ids(norm_rows)

    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for it in norm_rows:
        status, title = it["status"], it["title"]
        key = f"{status}|{title}"
        grouped.setdefault(status, []).append(
            {
                "id": group_ids[key],
                "status": status,
                "title": title,
                "date": it["date"],
                "count": it["count"],
            }
        )
    return grouped

def _sorted_statuses(grouped: Dict[str, List[Dict[str, Any]]]) -> List[str]:
    """Порядок статусов как в PHP-оригинале. Неизвестные — в конец."""
    return sorted(grouped.keys(), key=lambda s: _STATUS_PRIORITY.get(s, 999))

def _render_table_for_status(items: List[Dict[str, Any]]) -> str:
    """Аналог renderStatusesTable(...) из PHP. Возвращает только table.getTable()."""
    table = PivotTable()
    table.Items = items
    table.RoundValue = 1
    table.RowTitleStyle = "font-weight: bold;"
    table.PaintType = "asc"
    table.ShowId = False
    if hasattr(table, "HideRow"):
        table.HideRow = True
    if hasattr(table, "ShowRank"):
        table.ShowRank = True
    table.ShowSummaryXAxis = False
    table.OrderByTotal = "desc"

    table.Fields = [
        {"field": "date", "type": "period", "format": "%d.%m", "title": "Rank", "style": "width: 10px;"},
        {"field": "id",   "type": "row"},      # скрытая опорная строка
        {"field": "count","type": "value"},
    ]
    table.DescriptionColumns = [
        {"field": "status", "title": "Статус",  "style": "width: 100px;"},
        {"field": "title",  "title": "Причина", "style": "width: 250px;"},
    ]
    return table.getTable()

def color_weekend_xticklabels(ax):
    try:
        for lbl in ax.get_xticklabels():
            txt = lbl.get_text().strip()
            if not txt:
                continue
            dt = datetime.strptime(txt, "%d.%m")
            if dt.weekday() >= 5:  # 5=сб, 6=вс
                lbl.set_color("#757E85")
    except Exception:
        pass


def _get_max_open_close(items: List[Dict[str, Any]]) -> float:
    mx = 0.0
    for it in items or []:
        v = it.get("avg_open_close_min")
        try:
            v = float(v) if v is not None else None
        except Exception:
            v = None
        if v is not None and v > mx:
            mx = v
    return mx

def _norm_date(val: Any) -> Optional[str]:
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None

    # ISO: YYYY-MM-DD ...
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]

    # DD.MM.YYYY
    try:
        return datetime.strptime(s, "%d.%m.%Y").strftime("%Y-%m-%d")
    except Exception:
        return None


def _to_num(x: Any) -> float:
    if x is None:
        return 0.0
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip().replace("%", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0


def _fmt(x: Any, digits: int = 1) -> str:
    v = _to_num(x)
    if digits <= 0:
        return str(int(round(v, 0)))
    return str(round(v, digits))


def _get_data_in_dates(
    items: List[Dict[str, Any]],
    date_list: List[str],
    date_field: str,
    value_field: str,
    round_digits: int = 1,
) -> List[str]:
    acc: Dict[str, float] = {}
    for row in items or []:
        d = _norm_date(row.get(date_field))
        if not d:
            continue
        acc[d] = acc.get(d, 0.0) + _to_num(row.get(value_field))

    out: List[str] = []
    for d0 in date_list:
        d = _norm_date(d0) or str(d0)
        out.append(_fmt(acc.get(d, 0.0), round_digits))
    return out


def _sum_in_dates(
    items: List[Dict[str, Any]],
    date_list: List[str],
    date_field: str,
    value_field: str,
) -> float:
    wanted = {(_norm_date(d) or str(d)) for d in date_list}
    s = 0
    for row in items or []:
        d = _norm_date(row.get(date_field))
        if d and d in wanted:
            s += _to_num(row.get(value_field))
    return s


def _safe_pct(num: float, den: float, digits: int) -> float:
    if den <= 0:
        return 0.0
    return round((num / den) * 100.0, digits)


def _generate_block_new(
    table: TableConstructor,
    title: Optional[str],
    rows: List[Dict[str, Any]],
    date_list: List[str],
    date_field: str,
    overall_total: float,
    colspan: int,
):
    if title:
        table.genRowTitleAuto(title, colspan=str(colspan))

    for r in rows:
        label = r["label"]
        items = r["items"]
        field = r["field"]
        percent_digits = int(r.get("percent_digits", 0))
        round_digits = int(r.get("round_digits", 0))

        daily = _get_data_in_dates(items, date_list, date_field, field, round_digits)
        total = _sum_in_dates(items, date_list, date_field, field)
        avg = (total / len(date_list)) if date_list else 0.0
        pct = 100.0 if field == "total" else _safe_pct(total, overall_total, percent_digits)

        table.genCellTD(label, "font-weight: bold;")
        table.multiGenCellTD(daily)
        table.genCellTD(_fmt(total, round_digits), "font-weight: bold;")
        table.genCellTD(_fmt(avg, round_digits), "font-weight: bold;")
        table.genCellTD(f"{_fmt(pct, percent_digits)}%", "font-weight: bold;")
        table.genRow()

def color_weekend_xticklabels(ax):
    """Красим только подписи дат выходных на оси X."""
    from matplotlib import dates as _md

    ax.figure.canvas.draw()
    ticks = ax.get_xticks()
    labels = ax.get_xticklabels()

    for t, lab in zip(ticks, labels):
        try:
            dt = _md.num2date(float(t))
        except Exception:
            continue
        if dt.weekday() >= 5:
            lab.set_color("#6b7280")
            lab.set_fontweight("bold")


def _parse_item_dt(v) -> datetime:
    s = str(v).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    raise ValueError(f"Unknown date format: {s}")

def _normalize_to_ymd(s: str, date_formats=None) -> str:
    s = (s or "").strip()
    fmts = (date_formats or []) + [
        "%Y-%m-%d %H:%M:%S", "%Y-%m-%d",
        "%d.%m.%Y %H:%M:%S", "%d.%m.%Y",
        "%d-%m-%Y %H:%M:%S", "%d-%m-%Y",
        "%d/%m/%Y %H:%M:%S", "%d/%m/%Y",
    ]
    last_err = None
    for fmt in fmts:
        try:
            return datetime.strptime(s[:19], fmt).strftime("%Y-%m-%d")
        except Exception as e:
            last_err = e
    try:
        st = s.replace("T", " ").split(".")[0].rstrip("Z")
        return datetime.fromisoformat(st).strftime("%Y-%m-%d")
    except Exception:
        raise ValueError(f"Не удалось распарсить дату: {s!r}") from last_err


def _php_round(value: float, ndigits: int) -> float:
    q = Decimal(10) ** -ndigits
    return float(Decimal(value).quantize(q, rounding=ROUND_HALF_UP))

def get_data_in_dates(
    items,
    dates,
    date_key,
    data_key,
    ndigits=None,
    date_formats=None
) -> List[Any]:
    """
    Полный аналог PHP-версии + smart_number:
    - для каждой даты из `dates` ищем элемент в `items`, у которого date_key совпадает по 'Y-m-d'
    - если не найдено — добавляем ""
    - если найдено и значение data_key = null/None — добавляем ""
    - если найдено и указан ndigits — округляем по PHP (HALF_UP)
    - иначе добавляем значение как есть
    - перед возвратом каждое число приводим через smart_number
    """
    result: List[Any] = []

    for d in dates:
        ymd = _normalize_to_ymd(d, date_formats)
        found = False
        for it in items:
            it_date_raw = it.get(date_key)
            if it_date_raw is None:
                continue
            it_ymd = _normalize_to_ymd(str(it_date_raw), date_formats)
            if it_ymd == ymd:
                found = True
                val = it.get(data_key, None)
                if val is None:
                    result.append("")
                elif ndigits is not None:
                    try:
                        num = _php_round(float(val), ndigits)
                        result.append(smart_number(num))
                    except Exception:
                        result.append(val)
                else:
                    result.append(smart_number(val))
                break
        if not found:
            result.append("")
    return result

def _date10(x: Any) -> str:
    return str(x or "")[:10]


def _get_max_value(items: List[Dict[str, Any]], field: str) -> float:
    m = 0.0
    for it in items or []:
        try:
            v = float(it.get(field, 0) or 0)
        except Exception:
            v = 0.0
        m = max(m, v)
    # PHP-логика (упрощённо): округления вверх и *1.2
    if m >= 100:
        m = ((m + 99) // 100) * 100
    return round(m * 1.2, 2)

def _get_max_value_for_render_jira_graph_op(items, field: str) -> int:
    max_val = 0.0
    for it in items or []:
        v = it.get(field)
        try:
            v = float(v) if v is not None else 0.0
        except Exception:
            v = 0.0
        if v > max_val:
            max_val = v

    # если у тебя в оригинале getMaxValue() другая логика — поменяй здесь
    if max_val >= 100:
        max_val = round(max_val / 90) * 100
    max_val *= 2

    return int(max_val) if max_val else 0


def _build_indicators_table(formatted_dates: List[str], dates: List[str], indicators_rows: List[Dict[str, Any]]) -> str:
    """
    Таблица как getTaskTitle + genTaskRow (в конце "Среднее значение").
    Возвращаем HTML (renderTable()).
    """
    t = TableConstructor()
    t.genCellTH("Показатель", "word-wrap: break-word; width: 250px;")
    t.multiGenCellTH(formatted_dates)
    t.genCellTH("Среднее значение", "word-wrap: break-word; width: 160px;")
    t.genRow()

    for row in indicators_rows:
        label = row["label"]
        items = row["items"]
        field = row["field"]
        month_val = row["month_val"]
        period = row.get("period", "date")

        values = get_data_in_dates(items, dates, period, field, 1)

        # min/max для подсветки (как в PHP)
        try:
            _min = float(min(values)) if values else 0.0
            _max = float(max(values)) if values else 0.0
        except Exception:
            _min, _max = 0.0, 0.0

        t.genCellTD(label, "font-weight: bold;")
        t.multiGenCellTD(values, None, None, {"min": _min, "max": _max})
        t.genCellTD(month_val, "font-weight: bold;")
        t.genRow()

    return t.renderTable()


def _generate_block(task: TableConstructor, title: str | None, rows: List[Dict[str, Any]]) -> None:
    if title:
        task.genRowTitle(title)

    for r in rows:
        data = r["data"]
        try:
            _min = float(min(data)) if isinstance(data, list) and data else 0.0
            _max = float(max(data)) if isinstance(data, list) and data else 0.0
        except Exception:
            _min, _max = 0.0, 0.0

        task.genCellTD(r["label"], "font-weight: bold;")
        task.multiGenCellTD(data, None, None, {"min": _min, "max": _max})
        task.genCellTD(r["total"], "font-weight: bold;")
        task.genCellTD(r["avg"], "font-weight: bold;")
        task.genCellTD(f'{r["percent"]}%', "font-weight: bold;")
        task.genRow()


@op(required_resource_keys={"report_utils"})
def indicators_table_second_line_op(
    context,
    main_data: List[Dict[str, Any]],
    handling_daily: List[Dict[str, Any]],
    month_kpis: Dict[str, Any],
) -> str:
    dates = context.resources.report_utils.get_utils()
    formatted_dates = dates.FormattedDates
    date_list = dates.Dates

    indicators = [
        {"label": "Общее количество обработанных заявок", "items": main_data, "field": "total", "month_val": month_kpis["avg_issues"]},
        {"label": "%  решенных заявок на уровне 2 линии", "items": handling_daily, "field": "resolved_percent", "month_val": month_kpis["resolved_percent"]},
        {"label": "% нерешенных заявок на уровне 2 линии", "items": handling_daily, "field": "not_resolved_percent", "month_val": month_kpis["not_resolved_percent"]},
        {"label": "Среднее время закрытия заявки (с момента создания), минуты", "items": main_data, "field": "avg_open_close_min", "month_val": month_kpis["avg_open_close_min"]},
        {"label": "Среднее время обработки заявки (с момента взятия в работу), минуты", "items": main_data, "field": "avg_in_progress_min", "month_val": month_kpis["avg_in_progress_min"]},
        {"label": "AHT видеозвонка", "items": main_data, "field": "avg_handling_time_min", "month_val": month_kpis["avg_handling_time_min"]},
    ]

    return _build_indicators_table(formatted_dates, date_list, indicators)


@op(required_resource_keys={"report_utils"})
def requests_conversion_table_op(
    context,
    data: List[Dict[str, Any]],  # omni daily: total/closed_by_first_line/linked_jira_issue/...
    jira_handling_daily_data: List[Dict[str, Any]],  # jira daily: resolved/not_resolved/...
    days_count: int = 8,
) -> str:
    dates = context.resources.report_utils.get_utils()

    formatted_dates = (dates.FormattedDates or [])[:days_count]
    date_list = (dates.Dates or [])[:days_count]

    overall_total = _sum_in_dates(data or [], date_list, "date", "total")

    task = TableConstructor()

    # Шапка (как в PHP)
    task.genCellTH("Показатель", "word-wrap: break-word; width: 300px;")
    task.multiGenCellTH(formatted_dates)
    task.genCellTH("Итого", "word-wrap: break-word; width: 50px;")
    task.genCellTH("Сред.",  "word-wrap: break-word; width: 50px;")
    task.genCellTH("Доля",   "word-wrap: break-word; width: 50px;")
    task.genRow()

    colspan = 1 + len(formatted_dates) + 3  # показатель + дни + (итого/сред/доля)

    # Блок "Общее"
    _generate_block_new(
        table=task,
        title=None,
        rows=[
            {
                "label": "Общее количество обращений на оператора по интернету и ТВ",
                "items": data or [],
                "field": "total",
                "percent_digits": 0,
                "round_digits": 0,
            }
        ],
        date_list=date_list,
        date_field="date",
        overall_total=overall_total,
        colspan=colspan,
    )

    # Блок "1 линия"
    _generate_block_new(
        table=task,
        title="1 линия",
        rows=[
            {"label": "Обработано на уровне 1 линии", "items": data or [], "field": "closed_by_first_line", "percent_digits": 0},
            {"label": "Создана заявка другим отделам", "items": data or [], "field": "linked_jira_issue", "percent_digits": 0},
            {"label": "Повторное обращений по ранее созданной заявке", "items": data or [], "field": "created_other_jira_issue", "percent_digits": 0},
            {"label": "Создана заявка на 2 линию", "items": data or [], "field": "closed_by_second_line", "percent_digits": 0},
        ],
        date_list=date_list,
        date_field="date",
        overall_total=overall_total,
        colspan=colspan,
    )

    # Блок "2 линия" (проценты с 1 знаком)
    _generate_block_new(
        table=task,
        title="2 линия",
        rows=[
            {"label": "Помогли на уровне 2 линии", "items": jira_handling_daily_data or [], "field": "resolved", "percent_digits": 1},
            {"label": "Не помогли на уровне 2 линии", "items": jira_handling_daily_data or [], "field": "not_resolved", "percent_digits": 1},
            {"label": "Проблемы на линии/ с оборудованием", "items": jira_handling_daily_data or [], "field": "infrastructure_hardware_issue", "percent_digits": 1},
            {"label": "Отмена заявки", "items": jira_handling_daily_data or [], "field": "ticket_cancel", "percent_digits": 1},
            {"label": "Причина на стороне абонента", "items": jira_handling_daily_data or [], "field": "client_side_reason", "percent_digits": 1},
        ],
        date_list=date_list,
        date_field="date",
        overall_total=overall_total,
        colspan=colspan,
    )

    return task.renderTable()


@op
def jira_share_chart_op(jira_daily: Dict[str, Any]) -> str:
    """
    renderJiraGraph(): bars resolved/total + line resolved_percent
    Возвращаем base64 (как у вас принято в MailReport).
    """
    items = jira_daily["main"]
    max_value = _get_max_value(items, "total")

    chart = MultiAxisChart()
    chart.GraphTitle = ""
    chart.YAxesName = "Количество"
    chart.Items = items
    chart.YAxesMin = 0
    chart.YAxesMax = max_value
    chart.Position = "center"
    chart.Combine(3, max_value)
    chart.Fields = [
        {"field": "date", "title": "", "type": "label"},
        {"field": "resolved", "title": "Кол-во обработанных заявок", "type": "bar", "round": 0},
        {"field": "total", "title": "Кол-во заявок", "type": "bar", "round": 0},
        {"field": "resolved_percent", "title": "% обработанных заявок", "type": "line", "round": 0},
    ]
    return chart.get_graphics_in_base64()


@op
def jira_video_reasons_pivot_op(reasons_daily: List[Dict[str, Any]]) -> str:
    t = PivotTable()
    t.RoundValue = 1
    t.RowTitleStyle = "font-weight: bold;"
    t.PaintType = "asc"
    t.ShowId = False
    t.HideRow = True
    t.ShowRank = True
    t.ShowSummaryXAxis = False
    t.OrderByTotal = "desc"
    t.Items = reasons_daily
    t.Fields = [
        {"field": "date", "type": "period", "format": "d.m", "title": "Rank", "style": "width: 10px;"},
        {"field": "title", "type": "row"},
        {"field": "count", "type": "value"},
    ]
    t.DescriptionColumns = [{"field": "title", "title": "Причина", "style": "width: 250px;"}]
    return t.getTable()


@op
def jira_resolution_grouped_tables_op(resolution_daily: List[Dict[str, Any]]) -> List[Tuple[str, str]]:
    """
    Возвращаем список (status_title, html_table) — как PHP: foreach status -> renderStatusesTable
    """
    # назначаем group id по (status|title)
    group_ids: Dict[str, int] = {}
    next_id = 1
    for it in resolution_daily:
        key = f'{it.get("status","")}|{it.get("title","")}'
        if key not in group_ids:
            group_ids[key] = next_id
            next_id += 1

    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for it in resolution_daily:
        status = it.get("status", "")
        key = f'{status}|{it.get("title","")}'
        grouped.setdefault(status, []).append(
            {
                "id": group_ids[key],
                "status": status,
                "title": it.get("title"),
                "date": it.get("date"),
                "count": it.get("count"),
            }
        )

    status_priority = {
        "Помогли": 1,
        "Не помогли": 2,
        "Другое": 3,
        "Причина на стороне абонента": 4,
        "Отмена заявки": 5,
        "Проблемы на линии/ с оборудованием": 6,
    }

    out: List[Tuple[str, str]] = []
    for status in sorted(grouped.keys(), key=lambda s: status_priority.get(s, 999)):
        items = grouped[status]

        t = PivotTable()
        t.RoundValue = 1
        t.RowTitleStyle = "font-weight: bold;"
        t.PaintType = "asc"
        t.ShowId = False
        t.HideRow = True
        t.ShowRank = True
        t.ShowSummaryXAxis = False
        t.OrderByTotal = "desc"
        t.Items = items
        t.Fields = [
            {"field": "date", "type": "period", "format": "d.m", "title": "Rank", "style": "width: 10px;"},
            {"field": "id", "type": "row"},
            {"field": "count", "type": "value"},
        ]
        t.DescriptionColumns = [
            {"field": "status", "title": "Статус", "style": "width: 100px;"},
            {"field": "title", "title": "Причина", "style": "width: 250px;"},
        ]

        out.append((status, t.getTable()))

    return out


@op
def jira_help_graph_op(jira_daily: Dict[str, Any]) -> str:
    """
    renderJiraHelpGraph(): columns resolved + resolved_count + line resolved_percent
    """
    items = jira_daily["handling"]

    chart = MultiAxisChart()
    chart.GraphTitle = ""
    chart.YAxesName = "Количество"
    chart.Items = items
    chart.YAxesMin = 0
    chart.YAxesMax = 200
    chart.Pointer = 20
    chart.Combine(4, 100, 0)
    chart.Fields = [
        {"field": "date", "title": "", "type": "label"},
        {"field": "resolved", "title": "Кол-во оказанной помощи", "type": "column"},
        {"field": "resolved_count", "title": "Кол-во обработанных заявок", "type": "column"},
        {"field": "resolved_percent", "title": "% оказанной помощи", "type": "line", "round": 1},
    ]
    return chart.get_graphics_in_base64()


@op
def jira_help_bar_graph_op(jira_daily: Dict[str, Any]) -> str:
    """
    renderJiraHelpBarGraph(): stacked bars по итогам (Помогли/Не помогли/...)
    """
    items = jira_daily["handling"]
    max_value = _get_max_value(items, "count")

    chart = MultiAxisChart()
    chart.Position = "center"
    chart.GraphTitle = "Данный график показывает кол-во обработанных обращений."
    chart.YAxesName = "Кол-во"
    chart.YAxesMin = 0
    chart.Stack = True
    chart.ShowTooltips = False
    chart.setColourPack(6)
    chart.Pointer = 10
    chart.YAxesMax = max_value
    chart.Items = items
    chart.Fields = [
        {"field": "date", "title": "", "type": "label"},
        {"field": "resolved", "title": "Помогли", "type": "bar", "round": 0},
        {"field": "not_resolved", "title": "Не помогли", "type": "bar", "round": 0},
        {"field": "infrastructure_hardware_issue", "title": "Проблемы на линии/ с оборудованием", "type": "bar", "round": 0},
        {"field": "ticket_cancel", "title": "Отмена заявки", "type": "bar", "round": 0},
        {"field": "client_side_reason", "title": "Причина на стороне абонента", "type": "bar", "round": 0},
    ]
    return chart.get_graphics_in_base64()

@op
def render_jira_durations_graph_op(context, jira_daily: dict) -> str:
    """
    Аналог PHP renderJiraLineGraph(), но в твоём стиле:
    - BasicLineGraphics.make_preparation()
    - matplotlib render -> SVG -> base64
    """
    items = (jira_daily or {}).get("main", []) or []

    graphic = BasicLineGraphics(
        graph_title="Данный график показывает длительность обработки заявки",
        x_axes_name="Дни",
        y_axes_name="Минуты",
        stroke="smooth",
        show_tooltips=True,
        y_axes_min=0,
        items=items,
        fields=[
            {"field": "date", "title": "Дни", "type": "label"},
            {
                "field": "avg_in_progress_min",
                "title": "Среднее время обработки заявки (с момента взятия в работу), мин",
                "round": 0,
            },
            {
                "field": "avg_handling_time_min",
                "title": "AHT видеозвонка, мин",
                "round": 0,
            },
            {
                "field": "avg_open_close_min",
                "title": "Среднее время закрытия заявки (с момента создания), мин",
                "round": 0,
            },
        ],
        width=1920,
        height=400,
    )

    graphic.make_preparation()

    dpi = 100
    fig, ax = plt.subplots(figsize=(graphic.width / dpi, graphic.height / dpi), dpi=dpi)
    fig.patch.set_facecolor("#f8f9fa")
    ax.set_facecolor("#ffffff")

    # X — реальные даты
    dates = [_parse_item_dt(it["date"]) for it in graphic.items]

    # Рисуем серии
    # (да, тут цвета заданы специально — как у тебя в styled графиках)
    series_style = [
        {"line": "#111827", "point": "#2c5282"},
        {"line": "#374151", "point": "#805ad5"},
        {"line": "#6b7280", "point": "#d97706"},
    ]

    for idx, ds in enumerate(graphic.datasets):
        y = ds["data"]
        st = series_style[idx % len(series_style)]

        ax.plot(dates, y, color=st["line"], linewidth=2.2, label=ds["name"])

        # точки + подписи (на 8 днях ок, не будет каши)
        for d, val in zip(dates, y):
            if val is None:
                continue
            ax.plot(
                d, val,
                marker="o",
                markersize=5.5,
                markerfacecolor=st["point"],
                markeredgecolor="white",
                markeredgewidth=1,
                zorder=3,
            )
            ax.annotate(
                f"{val:.0f}",
                (d, val),
                textcoords="offset points",
                xytext=(0, 0),
                ha="center",
                fontsize=7,
                fontweight="bold",
                bbox=dict(boxstyle="round,pad=0.25", facecolor="black", edgecolor="none", alpha=0.75),
                color="white",
                zorder=4,
            )

    # X-limit без полей
    ax.set_xmargin(0)
    if dates:
        ax.set_xlim(min(dates), max(dates))

    # Динамические Y-границы (по всем сериям)
    all_values = [v for ds in graphic.datasets for v in ds["data"] if v is not None]
    if all_values:
        data_min = min(all_values)
        data_max = max(all_values)
        span = data_max - data_min
        if span <= 0:
            span = max(1.0, abs(data_max) * 0.1)

        desired_bands = 10
        raw_step = span / desired_bands
        exp = math.floor(math.log10(raw_step)) if raw_step > 0 else 0
        frac = raw_step / (10 ** exp) if raw_step > 0 else 1

        if frac <= 1:
            nice = 1
        elif frac <= 2:
            nice = 2
        elif frac <= 2.5:
            nice = 2.5
        elif frac <= 5:
            nice = 5
        else:
            nice = 10

        step = nice * (10 ** exp)
        padding = max(step, span * 0.15)

        y0 = math.floor((data_min - padding) / step) * step
        y1 = math.ceil((data_max + padding) / step) * step
        ax.set_ylim(y0, y1)

        # заливаем каждую вторую полосу
        i = 0
        yy = y0
        while yy < y1:
            if i % 2 == 1:
                ax.axhspan(yy, yy + step, alpha=0.05, color="gray", zorder=0)
            yy += step
            i += 1
    else:
        ax.set_ylim(0, 100)

    # Сетка и формат X
    ax.grid(True, linestyle="-", linewidth=0.5, color="#e2e8f0", alpha=0.7, axis="y")
    ax.set_axisbelow(True)

    ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    ax.xaxis.set_major_formatter(FuncFormatter(lambda x, pos: mdates.num2date(x).strftime("%d.%m")))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=0, fontsize=9)

    color_weekend_xticklabels(ax)

    # Оси/рамки
    ax.set_xlabel(graphic.x_axes_name, fontsize=10, color="#4a5568")
    ax.set_ylabel(graphic.y_axes_name, fontsize=10, color="#4a5568")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color("#e2e8f0")
    ax.spines["bottom"].set_color("#e2e8f0")
    ax.tick_params(axis="x", colors="#4a5568", labelsize=7)
    ax.tick_params(axis="y", colors="#4a5568", labelsize=10)

    # Заголовок
    ax.text(
        0.5, 1.08, graphic.graph_title,
        horizontalalignment="center",
        transform=ax.transAxes,
        fontsize=11,
        color="#2d3748",
        wrap=True,
    )

    ax.legend(loc="upper left")
    fig.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="svg", bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")

@op
def render_jira_graph_op(context, items):
    """
    Аналог PHP renderJiraGraph($items):
    строим MultiAxisChart и возвращаем SVG в base64 (как мы обычно делаем в Dagster).
    """
    items = items or []
    max_value = _get_max_value_for_render_jira_graph_op(items, "total")

    chart = MultiAxisChart(
        graph_title="",
        y_axes_name="Количество",
        y_axes_min=0,
        y_axes_max=max_value,
        items=items,
        fields=[
            {"field": "date", "title": "", "type": "label"},
            {
                "field": "resolved",
                "title": "Кол-во обработанных заявок",
                "chartType": "grouped_bars",
                "round": 0,
            },
            {
                "field": "total",
                "title": "Кол-во заявок",
                "chartType": "grouped_bars",
                "round": 0,
            },
            {
                "field": "resolved_percent",
                "title": "% обработанных заявок",
                "chartType": "line",
                "round": 0,
            },
        ],
    )

    # PHP: $table->Position = 'center';
    if hasattr(chart, "Position"):
        chart.Position = "center"
    elif hasattr(chart, "position"):
        chart.position = "center"

    # PHP: $table->Combine(3, $maxValue);
    try:
        chart.combine(3, max_percent=max_value)
    except TypeError:
        # на случай старой сигнатуры combine(min, max, min_percent)
        chart.combine(3, max_value)

    fig, ax = plt.subplots(
        figsize=(chart.width / chart.dpi, chart.height / chart.dpi),
        dpi=chart.dpi,
    )
    chart.render_graphics(ax)

    buf = io.BytesIO()
    fig.savefig(buf, format="svg", bbox_inches="tight")
    plt.close(fig)

    svg_64 = base64.b64encode(buf.getvalue()).decode("utf-8")
    get_dagster_logger().info(f"render_jira_graph_op: items={len(items)} y_max={max_value}")
    return svg_64

@op
def render_jira_line_graph_op(context, items: List[Dict[str, Any]]) -> str:
    """
    Рендерит линийный график по трем метрикам Jira (минуты), возвращает SVG base64.
    Основан на твоем render_csf_basic_graphs.
    """
    items = items or []

    fields = [
        ("avg_in_progress_min", "Среднее время обработки заявки(с момента взятия на работу), минуты"),
        ("avg_handling_time_min", "AHT, минуты"),
        ("avg_open_close_min", "Среднее время закрытия заявки (с момента cоздания), минуты"),
    ]

    # Можно оставить без color_map (возьмутся дефолтные цвета) — или задать явно:
    color_map = {
        "avg_in_progress_min": "#222d32",
        "avg_handling_time_min": "#e2007a",
        "avg_open_close_min": "#919191",
    }

    # Тот же пайплайн, что и в graphic_calls_dynamic_general:
    svg_b64 = render_csf_basic_graphs(
        items=items,
        fields=fields,
        is_target=False,                 # PHP-оригинал без плановой линии
        color_map=color_map,             # опционально, можно убрать
        ylabel="Минуты",
        graph_title="Данный график показывает длительность обработки заявки",
        label_interval=1,                # подпись каждой точки
    )
    return svg_b64

@op
def render_reasons_daily_pivot_op(context, reasons_daily_data):
    # нормализуем дату (отрезаем время)
    items = []
    for x in reasons_daily_data:
        y = dict(x)
        y["date"] = str(y.get("date",""))[:10]  # 'YYYY-MM-DD'
        items.append(y)

    table = PivotTable()
    table.Items = items
    table.RoundValue = 1
    table.RowTitleStyle = "font-weight: bold;"
    table.PaintType = "asc"
    table.ShowId = False
    table.HideRow = True            # прячем колонку row
    table.ShowRank = True
    table.ShowSummaryXAxis = False
    table.OrderByTotal = "desc"

    # важное: выводим текст причины отдельной описательной колонкой
    table.DescriptionColumns = [
        {"field": "title", "title": "Причина", "style": "width: 250px;"},
    ]

    table.Fields = [
        {"field": "date", "type": "period", "format": "%d.%m", "title": "Rank", "style": "width: 10px;"},
        {"field": "title", "type": "row"},      # используется для группировки, но визуально скрыт
        {"field": "count", "type": "value"},
    ]

    return table.getTable()

@op
def render_jira_resolution_status_tables_op(
    context,
    resolution_rows: List[Dict[str, Any]],
) -> Dict[str, str]:
    """
    Вход: строки {date, status, title, count} из jira_resolution_daily_op.
    Выход: {status: html_table}, без заголовков и объединений.
    """
    if not resolution_rows:
        return {}

    grouped = _group_by_status_with_ids(resolution_rows)
    tables_by_status: Dict[str, str] = {}
    for st in _sorted_statuses(grouped):
        tables_by_status[st] = _render_table_for_status(grouped[st])

    return tables_by_status

@op
def render_jira_help_graph_op(context, items: List[Dict[str, Any]]) -> str:
    """
    Аналог PHP renderJiraHelpGraph($items):
      • Y: 0..200, 'Количество'
      • Bars: resolved, resolved_count
      • Line (правый Y): resolved_percent (0..100, round=1)
      • Pointer = 20 (если свойство поддерживается)
    Возвращает SVG base64.
    """
    items = items or []

    chart = MultiAxisChart(
        graph_title="",
        y_axes_name="Количество",
        y_axes_min=0,
        y_axes_max=200,
        fields=[
            {"field": "date",             "title": "",                          "type": "label"},
            {"field": "resolved",         "title": "Кол-во оказанной помощи",   "chartType": "grouped_bars", "round": 0},
            {"field": "resolved_count",   "title": "Кол-во обработанных заявок","chartType": "grouped_bars", "round": 0},
            {"field": "resolved_percent", "title": "% оказанной помощи",        "chartType": "line",         "round": 1},
        ],
        items=items,
        dot=True,   # как в твоём примере с Jira-графиком
    )

    # Pointer=20 — если свойство есть
    if hasattr(chart, "pointer"):
        chart.pointer = 20

    # Правый Y для 4-й серии (resolved_percent): 0..100
    try:
        chart.combine(4, max_percent=100, min_percent=0)
    except TypeError:
        # на случай старой сигнатуры combine(index, max, min)
        chart.combine(4, 100, 0)

    # Рендер (в твоём привычном стиле)
    fig, ax = plt.subplots(
        figsize=(chart.width / chart.dpi, chart.height / chart.dpi),
        dpi=chart.dpi,
    )
    chart.render_graphics(ax)

    buf = io.BytesIO()
    fig.savefig(buf, format="svg", bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)

    svg_b64 = base64.b64encode(buf.read()).decode("utf-8")
    get_dagster_logger().info(f"render_jira_help_graph_op: items={len(items)}")
    return svg_b64

@op
def render_jira_help_bar_graph_op(context, items: List[Dict[str, Any]]) -> str:
    """
    Аналог PHP renderJiraHelpBarGraph($items):
      • Стековые бары: resolved, not_resolved, infrastructure_hardware_issue, ticket_cancel, client_side_reason
      • Палитра pack=6, Pointer=10, без тултипов
      • Y: 0..max(count) (или сумма серий по дате, если 'count' отсутствует)
    Возвращает SVG base64.
    """
    items = items or []
    y_max = _compute_max_y(items)

    chart = MultiAxisChart(
        graph_title="Данный график показывает кол-во обработанных обращений.",
        x_axes_name="Дата",
        y_axes_name="Кол-во",
        y_axes_min=0,
        y_axes_max=130,
        stack=True,
        show_tooltips=False,
        pointer=10,
        fields=[
            {"field": "date",  "title": "",                         "type": "label"},
            {"field": "resolved",       "title": "Помогли",                            "chartType": "bar", "round": 0},
            {"field": "not_resolved",   "title": "Не помогли",                         "chartType": "bar", "round": 0},
            {"field": "infrastructure_hardware_issue", "title": "Проблемы на линии/ с оборудованием", "chartType": "bar", "round": 0},
            {"field": "ticket_cancel",  "title": "Отмена заявки",                      "chartType": "bar", "round": 0},
            {"field": "client_side_reason", "title": "Причина на стороне абонента",    "chartType": "bar", "round": 0},
        ],
        items=items,
    )

    # Доп. настройки как в PHP
    if hasattr(chart, "position"):
        chart.position = "center"
    # палитра 6 как в PHP setColourPack(6)
    if hasattr(chart, "set_colour_pack"):
        chart.set_colour_pack(6)
    elif hasattr(chart, "setColourPack"):
        chart.setColourPack(6)

    # Рендер → SVG b64 (как в твоих примерах)
    fig, ax = plt.subplots(
        figsize=(chart.width / chart.dpi, chart.height / chart.dpi),
        dpi=chart.dpi,
    )
    chart.render_graphics(ax)

    buf = io.BytesIO()
    fig.savefig(buf, format="svg", bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)

    b64 = base64.b64encode(buf.read()).decode("utf-8")
    get_dagster_logger().info(f"render_jira_help_bar_graph_op: items={len(items)} y_max={y_max}")
    return b64

@op(required_resource_keys={"report_utils"})
def report_pdf_saima_second_line_op(
    context,
    kpi_table,
    conversion_table,
    render_jira_graph,
    render_jira_line_graph,
    render_reasons_daily_pivot,
    render_jira_resolution_status_tables,
    render_jira_help_graph,
    render_jira_help_bar_graph,
) -> str:
    report = MailReport()
    report.image_mode = "file"

    APP_ROOT = Path(__file__).resolve().parents[2]
    LOGO_PATH = APP_ROOT / "utils" / "logo_png" / "logo.png"
    report.logo_file_override = str(LOGO_PATH)

    dates = context.resources.report_utils.get_utils()
    report_date = datetime.strptime(dates.ReportDate, "%Y-%m-%d")

    # ---------------- PAGE 1 ----------------
    p1 = ""
    p1 += report.set_basic_header(
        f"2 ЛИНИЯ ПОДДЕРЖКИ SAIMA | ЕЖЕДНЕВНАЯ СТАТИСТИКА ИСС НА {report_date.strftime('%d.%m.%Y')}"
    )

    p1 += report.add_block_header("Срез по основным показателям")
    p1 += report.add_content_to_statistic(kpi_table)

    p1 += report.add_block_header("Конверсия обращений по всем линиям поддержки Saima")
    p1 += report.add_content_to_statistic(conversion_table)

    # ---------------- PAGE 2 ----------------
    p2 = ""
    p2 += report.add_block_header("Статистика по основным показателям")
    p2 += report.add_module_header("Доля поступивших и обработанных заявок 2 линией")
    p2 += report.add_figure(render_jira_graph)

    p2 += report.add_module_header("Время обработки заявки")
    p2 += report.add_figure(render_jira_line_graph)

    # # ---------------- PAGE 3 ----------------
    p3 = ""
    p3 += report.add_block_header("Причины создания заявки для видеозвонка")
    p3 += report.add_content_to_statistic(render_reasons_daily_pivot)

    p3 += report.add_block_header("Результат рассмотрения заявки")
    p3 += report.add_module_header("Разбивка заявок по результатам рассмотрения")

    status_order = [
        "Помогли",
        "Не помогли",
        "Другое",
        "Причина на стороне абонента",
        "Отмена заявки",
        "Проблемы на линии/ с оборудованием",
    ]

    seen = set()
    for st in status_order:
        tbl = render_jira_resolution_status_tables.get(st)
        if not tbl:
            continue
        p3 += report.add_module_child_header(st)
        p3 += report.add_content_to_statistic(tbl)
        seen.add(st)

    for st, tbl in render_jira_resolution_status_tables.items():
        if st in seen:
            continue
        p3 += report.add_module_child_header(st)
        p3 += report.add_content_to_statistic(tbl)

    # ---------------- PAGE 4 ----------------
    p4 = ""
    p4 += report.add_module_header(
        'Доля оказанной помощи (без создания дальнейшей заявки) от всех "Помогли" + "Не помогли"'
    )
    p4 += report.add_figure(render_jira_help_graph)

    p4 += report.add_module_header("Разбивка обработанных заявок по общему результату рассмотрения")
    p4 += report.add_figure(render_jira_help_bar_graph)

    # ---------------- RENDER PDF ----------------
    tmp1 = "saima_second_line_page1.pdf"
    tmp2 = "saima_second_line_page2.pdf"
    tmp3 = "saima_second_line_page3.pdf"
    tmp4 = "saima_second_line_page4.pdf"

    html_to_pdf_page(html=p1, output_path=tmp1, height=700)
    html_to_pdf_page(html=p2, output_path=tmp2, height=700)
    html_to_pdf_page(html=p3, output_path=tmp3)
    html_to_pdf_page(html=p4, output_path=tmp4)

    final_pdf = "saima_second_line_daily_statistics.pdf"
    merge_pdfs([tmp1, tmp2, tmp3, tmp4], final_pdf)

    final_path = str(Path(final_pdf).resolve())
    context.log.info(f"final_pdf_path: {final_path}")
    return final_path