import base64
import io
import math
import re
from collections import defaultdict
from typing import Any, Dict, List, Optional, Sequence, Iterable, Tuple, DefaultDict
from dagster import op, Out
from matplotlib import pyplot as plt

from utils.mail.mail_report import MailReport, html_to_pdf_page, merge_pdfs
from utils.templates.charts.basic_line_graphics import BasicLineGraphics
from utils.templates.charts.gtm_multi_axis_chart import GTMMultiAxisChart
from utils.templates.charts.multi_axis_chart import MultiAxisChart
from utils.templates.tables.basic_table import BasicTable
from utils.templates.tables.pivot_table import PivotTable
# поправьте импорт под ваш проект
from utils.templates.tables.table_constructor import TableConstructor  # genCellTH, multiGenCellTH, genRow, genRowTitle, genCellTD, multiGenCellTD, renderTable

from pathlib import Path

from datetime import datetime, timedelta

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.transforms as mtransforms
from matplotlib.ticker import FuncFormatter

from typing import List

from itertools import chain

_SERIES_KEYS: List[str] = [
    "shareOfConnectionAll", "ahtAll", "artAll", "utilization", "occupancy",
    "shareOfConnection", "connections", "conversion", "aht", "art",
]

def _fmt_num(v, decimals: int | None = None, na: str = "") -> str:
    """
    decimals=None -> авто: целые без дроби, иначе 1 знак (как старая версия 1).
    decimals задан -> фиксированная точность с обрезкой нулей (как старая версия 2).
    na -> что выводить для None/NaN/inf (по умолчанию пусто).
    """
    if v is None:
        return na
    try:
        f = float(v)
    except (TypeError, ValueError):
        return str(v)

    if math.isnan(f) or math.isinf(f):
        return na

    if decimals is None:
        # авто-режим
        if abs(f - round(f)) < 1e-9:
            return f"{int(round(f))}"
        return f"{f:.1f}".rstrip("0").rstrip(".")
    else:
        s = f"{f:.{decimals}f}"
        return s.rstrip("0").rstrip(".") if decimals > 0 else s


def _values_by_dates(
    series: Optional[Sequence[Dict[str, Any]]],
    dates: Sequence[str],
    date_key: str = "date",
    value_key: str = "value",
    decimals: int = 1,
) -> List[str]:
    """Выравнивает ряд по списку дат и форматирует значения; пусто если нет значения."""
    if not series:
        return ["" for _ in dates]
    lookup = {str(r.get(date_key)): r.get(value_key) for r in series}
    out: List[str] = []
    for d in dates:
        v = lookup.get(str(d))
        out.append(_fmt_num(v, decimals) if v is not None else "")
    return out

def _extract_iso_dates(gpd: Dict[str, Any]) -> List[str]:
    """Собирает уникальные даты YYYY-MM-DD из всех доступных серий general_performance_data."""
    def _dates_from_series(series: Iterable[Dict[str, Any]]) -> Iterable[str]:
        for r in (series or []):
            ds = r.get("date")
            if not ds:
                continue
            # поддерживаем 'YYYY-MM-DD' и 'YYYY-MM-DD HH:MM:SS'
            yield ds.split(" ")[0]

    all_dates = set()
    for k in _SERIES_KEYS:
        all_dates.update(_dates_from_series(gpd.get(k)))
    return sorted(all_dates)  # по возрастанию

def _format_dates(dates: List[str], fmt: str = "%d.%m.%Y") -> List[str]:
    return [datetime.strptime(d, "%Y-%m-%d").strftime(fmt) for d in dates]

def _read(rec: Any, key: str, default=None):
    """Аккуратно читаем поле из dict/объекта/NamedTuple."""
    if isinstance(rec, dict):
        return rec.get(key, default)
    if hasattr(rec, key):
        return getattr(rec, key, default)
    try:
        return dict(rec).get(key, default)
    except Exception:
        return default

def _to_date_label(v) -> Tuple[str, float]:
    s = "" if v is None else str(v)
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.strftime("%d.%m"), dt.timestamp()
        except Exception:
            pass
    # как fallback: оставляем как есть
    return s, 0.0

def _to_date_label_for_spent_time(s: Any) -> Tuple[str, float]:
    """Превращает дату в подпись 'd.m' + ts для сортировки колонок."""
    ts = _to_ts(s)
    if ts == 0.0 and s:
        # оставим как есть, но ts=0; колонки без валидной даты уйдут влево
        return str(s), 0.0
    dt = datetime.fromtimestamp(ts)
    return dt.strftime("%d.%m"), ts

def _gradient_colour(current: float, min_val: float, max_val: float, paint_type: str = "asc") -> str:
    """
    Тот же градиент, что и в BasicTable: шаги по 10%.
    paint_type='asc' — «инверсия» (меньше → насыщеннее), как просили.
    """
    palette = {
        0: "#FFFFFF",
        10: "#FFEBEE",
        20: "#FFD7DE",
        30: "#FFC3CE",
        40: "#FFAFBE",
        50: "#FF9CAE",
        60: "#FF889E",
        70: "#FF748E",
        80: "#FF607E",
        90: "#FF4D6E",
        100: "#FF4D6E",
    }
    if max_val is None or min_val is None or max_val == min_val:
        return "#FFFFFF"

    pct = 0.0 if max_val == min_val else (current - min_val) / (max_val - min_val)
    pct = max(0.0, min(1.0, pct))
    # asc → инверсия
    if paint_type == "asc":
        pct = 1.0 - pct
    step = int(round(pct * 10) * 10)
    return palette.get(step, "#FFFFFF")

def _parse_dt(s: Any) -> datetime | None:
    if isinstance(s, datetime):
        return s
    if s is None:
        return None
    s = str(s)
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    return None


def _date_label(dt: datetime | None, fmt_out: str = "%d.%m") -> str:
    if dt is None:
        return ""
    return dt.strftime(fmt_out)


def _to_float(x):
    try:
        f = float(x)
        if math.isnan(f):
            return None
        return f
    except (TypeError, ValueError):
        return None


def _round1(v: float) -> str:
    # как RoundValue=1: «целые» печатаем без .0, иначе 1 знак
    if abs(v - int(v)) < 1e-9:
        return f"{int(v)}"
    return f"{v:.1f}"


def _gradient_desc(current: float, col_min: float, col_max: float) -> str:
    """
    Градиент как в ваших таблицах с шагом 10%.
    PaintType='desc' — чем больше значение, тем насыщеннее.
    """
    palette = {
        0: "#FFFFFF",
        10: "#FFEBEE",
        20: "#FFD7DE",
        30: "#FFC3CE",
        40: "#FFAFBE",
        50: "#FF9CAE",
        60: "#FF889E",
        70: "#FF748E",
        80: "#FF607E",
        90: "#FF4D6E",
        100: "#FF4D6E",
    }
    if col_max is None or col_min is None or col_max == col_min:
        return "#FFFFFF"
    pct = (current - col_min) / (col_max - col_min)
    pct = max(0.0, min(1.0, pct))
    step = int(round(pct * 10) * 10)
    return palette.get(step, "#FFFFFF")

def _to_ts(s: Any) -> float:
    """Временная метка для сортировки дат (fallback=0)."""
    if s is None:
        return 0.0
    ss = str(s)
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(ss, fmt).timestamp()
        except Exception:
            pass
    try:
        # вдруг уже d.m
        return datetime.strptime(ss, "%d.%m").replace(year=datetime.now().year).timestamp()
    except Exception:
        return 0.0

def _fmt_num_dec(v: Any, decimals: int = 1) -> str:
    """Формат с отсечением хвостовых нулей; None -> ''."""
    if v is None:
        return ""
    try:
        f = float(v)
    except (TypeError, ValueError):
        return str(v)
    s = f"{f:.{decimals}f}"
    return s.rstrip("0").rstrip(".")

def _to_int(v) -> int:
    try:
        return int(float(v))
    except Exception:
        return 0


@op(
    out=Out(str, description="HTML-таблица сводных метрик (Все проекты / Продажи)."),
    description="Рендерит HTML таблицу как в исходном PHP: показатели по датам + средние значения.",
)
def render_general_performance_table_op(
    context,
    general_performance_data: Dict[str, Any],
    dates: Optional[List[str]] = None,
    formatted_dates: Optional[List[str]] = None,
) -> str:
    # 0) Конфиг с дефолтами
    days_count = 8
    header_fmt = "%d.%m"

    # 1) Если даты не переданы — посчитаем из general_performance_data
    if not dates or not formatted_dates:
        all_dates = _extract_iso_dates(general_performance_data)
        if not all_dates:
            context.log.warning("Не нашли дат в general_performance_data — таблица будет с пустой шкалой дат.")
            dates = []
            formatted_dates = []
        else:
            # Берём последние N дат (по возрастанию, как в ваших примерах)
            dates = all_dates[-days_count:]
            formatted_dates = _format_dates(dates, header_fmt)

    # 2) Дальше — ваш текущий шаблон рендера
    t = TableConstructor()

    # ── Шапка ────────────────────────────────────────────────────────
    t.genCellTH("Показатель", style="word-wrap: break-word; width: 250px;")
    t.genCellTH("Ед. измерения")
    t.multiGenCellTH(list(formatted_dates))
    t.genCellTH("Сред.", style="word-wrap: break-word; width: 160px;")
    t.genRow()

    # ===== Все проекты =====
    t.genRowTitle("Все проекты")

    # Доля дозвона от всех попыток соединения (%)
    t.genCellTD("Доля дозвона от всех попыток соединения", "font-weight: bold;")
    t.genCellTD("%")
    t.multiGenCellTD(_values_by_dates(general_performance_data.get("shareOfConnectionAll"), dates, "date", "value", 1))
    t.genCellTD(_fmt_num(general_performance_data.get("averageShareOfConnectionAll"), 1), "font-weight: bold;")
    t.genRow()

    # Average Handling Time (сек.)
    t.genCellTD("Average Handling Time", "font-weight: bold;")
    t.genCellTD("сек.")
    t.multiGenCellTD(_values_by_dates(general_performance_data.get("ahtAll"), dates, "date", "value", 1))
    t.genCellTD(_fmt_num(general_performance_data.get("averageAHTAll"), 1), "font-weight: bold;")
    t.genRow()

    # Average Ring Time (сек.)
    t.genCellTD("Average Ring Time", "font-weight: bold;")
    t.genCellTD("сек.")
    t.multiGenCellTD(_values_by_dates(general_performance_data.get("artAll"), dates, "date", "value", 1))
    t.genCellTD(_fmt_num(general_performance_data.get("averageARTAll"), 1), "font-weight: bold;")
    t.genRow()

    # Utilization Rate (%)
    t.genCellTD("Utilization Rate", "font-weight: bold;")
    t.genCellTD("%")
    t.multiGenCellTD(_values_by_dates(general_performance_data.get("utilization"), dates, "date", "value", 1))
    t.genCellTD(_fmt_num(general_performance_data.get("averageUtilization"), 1), "font-weight: bold;")
    t.genRow()

    # Occupancy (%)
    t.genCellTD("Occupancy", "font-weight: bold;")
    t.genCellTD("%")
    t.multiGenCellTD(_values_by_dates(general_performance_data.get("occupancy"), dates, "date", "value", 1))
    t.genCellTD(_fmt_num(general_performance_data.get("averageOccupancy"), 1), "font-weight: bold;")
    t.genRow()

    return t.renderTable()


@op(
    out=Out(str, description="HTML pivot по исходящим вызовам"),
    description="Рендер PivotTable из списка записей {date, row, value}.",
)
def render_calls_table_op(context, items: List[Dict[str, Any]]) -> str:
    table = PivotTable()
    table.PaintType = "asc"
    table.OrderByTotal = "desc"      # если свойство поддерживается вашим PivotTable
    table.ShowTotalPercentage = True
    table.ShowSummaryXAxis = True
    table.ShowSummaryXAxisNew = False

    table.Items = items or []
    table.Fields = [
        {"field": "date", "type": "period", "format": "%d.%m"},
        {"field": "row",  "type": "row"},
        {"field": "value","type": "value"},
    ]

    return table.getTable()

@op(
    out=Out(str, description="SVG Base64 график: Конверсия подключений (кол-во/соединено + %)"),
    description=(
        "Рендер смешанного графика по general_connections_ratio: "
        "бары (grouped) — 'кол-во подключений' и 'соединено', "
        "линия — 'соединено / кол-во подключений' (%). "
        "Левая ось: количества, правая ось скрыта."
    ),
)
def render_general_connections_ratio_chart_op(
    context,
    general_connections_ratio: List[Dict[str, Any]],
) -> str:
    data = general_connections_ratio or []

    # 1) max по «соединено» → Y max
    max_connected = 0
    for it in data:
        v = int(it.get("соединено", it.get("Соединено", 0)) or 0)
        if v > max_connected:
            max_connected = v
    y_max = int(math.ceil((max_connected * 1.5) / 1000.0) * 1000) if max_connected > 0 else 1000

    # 2) Нормализация + сортировка по дате
    norm: List[Dict[str, Any]] = []
    for it in data:
        norm.append({
            "date": it.get("date"),
            "соединено": int(it.get("соединено", it.get("Соединено", 0)) or 0),
            "кол-во подключений": int(it.get("кол-во подключений", 0) or 0),
            "соединено / кол-во подключений": float(it.get("соединено / кол-во подключений", 0.0) or 0.0),
        })
    try:
        norm.sort(key=lambda x: datetime.strptime(str(x["date"]), "%Y-%m-%d"))
    except Exception:
        pass

    # 3) Конфигурация графика
    chart = GTMMultiAxisChart(dot=True, is_marker=False)
    chart.GraphTitle = ""
    chart.YAxesName = "Количество"
    chart.XAxesName = "Дата"
    chart.Items = norm

    chart.YAxesMin = 0
    chart.YAxesMax = y_max
    chart.Pointer = 500
    chart.Height = 450

    chart.HideRightAxis = True
    chart.Combine(4, 20, -25)

    chart.Fields = [
        {"field": "date", "title": "", "type": "label"},
        {"field": "кол-во подключений", "title": "Количество подключений", "chartType": "grouped_bars", "round": 0},
        {"field": "соединено", "title": "Количество дозвонов", "chartType": "grouped_bars", "round": 0},
        {"field": "соединено / кол-во подключений", "title": "Конверсия", "chartType": "line", "postfix": "%"},
    ]
    try:
        chart.setColourPack(6)
    except Exception:
        pass

    # 4) Рендер в SVG Base64 (как вы обычно делаете)
    fig, ax = plt.subplots(
        figsize=(
            getattr(chart, "width", 800) / getattr(chart, "dpi", 100),
            getattr(chart, "height", 450) / getattr(chart, "dpi", 100),
        ),
        dpi=getattr(chart, "dpi", 100),
    )
    chart.render_graphics(ax)

    buf = io.BytesIO()
    fig.savefig(buf, format="svg", bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    b64 = base64.b64encode(buf.read()).decode("utf-8")

    context.log.info(
        "render_general_connections_ratio_chart_op: rows=%d, y_max=%d (max_connected=%d)",
        len(norm), y_max, max_connected
    )
    return b64

@op(
    out=Out(str, description="SVG Base64 график: статусы подключений (active/suspended/total)"),
    description=(
        "Рендер смешанного графика по salesStatus: "
        "бары (stacked) — active/suspended, линия — total (active+suspended)."
    ),
)
def render_sales_status_chart_op(context, sales_status: List[Dict[str, Any]]) -> str:
    items = sales_status or []

    # 1) нормализация и total
    norm: List[Dict[str, Any]] = []
    max_total = 0
    for r in items:
        active = int(r.get("active", 0) or 0)
        suspended = int(r.get("suspended", 0) or 0)
        total = active + suspended
        d = r.get("date")
        norm.append({"date": d, "active": active, "suspended": suspended, "total": total})
        if total > max_total:
            max_total = total

    # 2) верхняя граница оси Y (как было)
    y_max = int(max_total + 100)
    y_max = max(0, y_max - 57)

    # 3) сортировка по дате
    def _ts(row: Dict[str, Any]) -> float:
        s = row.get("date")
        if not isinstance(s, str):
            return 0.0
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(s, fmt).timestamp()
            except Exception:
                pass
        return 0.0

    norm.sort(key=_ts)

    # 4) график (без устаревших аргументов)
    chart = MultiAxisChart(
        graph_title="",
        x_axes_name="Дата",
        y_axes_name="",
        stack=True,           # стекируем бары
        show_tooltips=False,
        height=400,
        pointer=20,
        y_axes_min=0,
        y_axes_max=y_max,
        fields=[
            {"field": "date", "title": "", "type": "label", "stacked": True},
            {"field": "active", "title": "Подключено снятием АП", "chartType": "bar", "round": 0, "stacked": True},
            {"field": "suspended", "title": "Подключено в Suspended", "chartType": "bar", "round": 0, "stacked": True},
            {"field": "total", "title": "Общее количество подключений", "chartType": "line", "round": 0, "points_only": True},
        ],
        items=norm,
        dot=True,
        base=50,

    )

    # Рендер → SVG Base64
    fig, ax = plt.subplots(
        figsize=(getattr(chart, "width", 800) / getattr(chart, "dpi", 100),
                 getattr(chart, "height", 400) / getattr(chart, "dpi", 100)),
        dpi=getattr(chart, "dpi", 100),
    )
    chart.render_graphics(ax)

    buf = io.BytesIO()
    fig.savefig(buf, format="svg", bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    b64 = base64.b64encode(buf.read()).decode("utf-8")

    context.log.info(
        "render_sales_status_chart_op: rows=%d, y_max=%d (max_total=%d)",
        len(norm), y_max, max_total
    )
    return b64

@op(
    out=Out(str, description="HTML-профиль дня: old_name → new_name (колонки в порядке «крупных» значений)."),
    description=(
        "Повторяет PHP: сортируем detailed_data по value desc, "
        "колонки берём в порядке первого появления, строки сортируем по 'всего' desc."
    ),
)
def render_day_profile_op(context, detailed_data: List[Dict[str, Any]]) -> str:
    data = detailed_data or []

    # 1) сортировка по value desc
    data_sorted = sorted(data, key=lambda r: _to_int(_read(r, "value", 0)), reverse=True)

    # 2) список колонок (new_name) в порядке появления ПОСЛЕ сортировки
    columns: List[str] = []
    seen = set()
    for rec in data_sorted:
        nn = _read(rec, "new_name")
        if nn is None:
            continue
        s = str(nn)
        if s not in seen:
            seen.add(s)
            columns.append(s)

    # 3) пивот: строки по old_name, столбцы по new_name; считаем суммы
    rows: Dict[str, Dict[str, Any]] = {}
    for rec in data_sorted:
        old_name = _read(rec, "old_name")
        new_name = _read(rec, "new_name")
        val = _to_int(_read(rec, "value", 0))
        if old_name is None or new_name is None:
            continue

        o = str(old_name)
        n = str(new_name)

        if o not in rows:
            rows[o] = {"old_name": o, "всего": 0}
            for c in columns:
                rows[o][c] = 0

        if n not in rows[o]:
            rows[o][n] = 0
            if n not in columns:
                columns.append(n)

        rows[o][n] += val
        rows[o]["всего"] += val

    pivot_data = list(rows.values())

    # 4) строки по 'всего' убыв.
    pivot_data.sort(key=lambda r: r.get("всего", 0), reverse=True)

    # === NEW: глобальные min/max по всем числовым ячейкам (включая "всего") ===
    all_vals: List[int] = []
    paint_cols = columns + ["всего"]
    for r in pivot_data:
        for c in paint_cols:
            v = r.get(c, 0)
            if v is None:
                continue
            try:
                all_vals.append(int(v))
            except Exception:
                pass

    if all_vals:
        global_min = min(all_vals)
        global_max = max(all_vals)
    else:
        global_min = 0
        global_max = 0

    # 5) поля для BasicTable (вшиваем paint_min/paint_max для глобальной шкалы)
    fields: List[Dict[str, Any]] = [
        {"field": "old_name", "title": "Старый ТП"},
    ]
    for col in columns:
        fields.append({
            "field": col,
            "title": col,
            "paint": True,
            "summary": "sum",
            # глобальные пределы окраски:
            "paint_min": global_min,
            "paint_max": global_max,
        })
    fields.append({
        "field": "всего",
        "title": "Grand Total",
        "paint": True,
        "summary": "sum",
        "paint_min": global_min,
        "paint_max": global_max,
    })

    # 6) рендер
    t = BasicTable()
    t.Fields = fields
    t.Items = pivot_data

    # (опционально) передаём глобальные пределы как свойства таблицы —
    # если ваш BasicTable умеет их читать на уровне виджета
    setattr(t, "GlobalPaintMin", global_min)
    setattr(t, "GlobalPaintMax", global_max)

    return t.getTable()

@op(
    out=Out(str, description="HTML-таблица: попытки дозвона / дозвоны > 5 сек. / подключения по старым и новым ТП"),
    description=(
        "Строит таблицу со столбцами: "
        "Старый ТП, Минимальный ТП на продажу, Подключенный ТП, "
        "Количество попыток дозвона, Дозвонов > 5 сек., Всего подключений, Конверсия. "
        "Группировка по (old_name, qwes_recomendet), сортировка групп по attempt_number по убыванию."
    ),
)
def render_attempts_services_connections_table_op(
    context,
    all_data: List[Dict[str, Any]],
) -> str:
    """
    Эквивалент PHP-кода:
    - нормализует входные записи
    - сортирует по connections (desc)
    - агрегирует по (old_name, qwes_recomendet)
    - считает конверсию conn/service * 100 в группе
    - сортирует группы по attempt_number (desc)
    - рендерит таблицу через TableConstructor.genDataRow(...)
    """
    rows = all_data or []

    # 1) Подготовка item'ов
    items: List[Dict[str, Any]] = []
    for data in rows:
        attempt_number = int(data.get("total") or data.get("TOTAL") or 0)
        service = int(data.get("more5_sec") or data.get("MORE5_SEC") or 0)
        connections = int(data.get("VALUE") or data.get("value") or 0)

        qwes_recomendet = data.get("qwes_recomendet")

        old_name = str(data.get("OLD_NAME") or data.get("old_name") or "")
        new_name = str(data.get("NEW_NAME") or data.get("new_name") or "")

        # как в PHP — на уровне item это поле не используется дальше,
        # но оставим вычисление для идентичности логики
        if attempt_number > 0 and service > 0:
            item_conversion = f"{round((float(connections) / service) * 100, 2)}%"
        else:
            item_conversion = "0%"

        items.append(
            {
                "old_name": old_name,
                "qwes_recomendet": qwes_recomendet,
                "new_name": new_name,
                "attempt_number": attempt_number,
                "service": service,
                "connections": connections,
                "technical_work_count": item_conversion,
            }
        )

    # 2) Сортировка items по убыванию connections
    items.sort(key=lambda r: int(r.get("connections") or 0), reverse=True)

    # 3) Агрегация по (old_name, qwes_recomendet)
    grouped: Dict[str, Dict[str, Any]] = {}
    for it in items:
        key = f"{it['old_name']}-{it.get('qwes_recomendet')}"
        if key not in grouped:
            grouped[key] = {
                "old_name": it["old_name"],
                "qwes_recomendet": it.get("qwes_recomendet"),
                "attempt_number": 0,
                "service": 0,
                "new_items": [],
                "total_connections": 0,
            }

        grouped[key]["new_items"].append(
            {
                "new-name": it["new_name"],
                "connections": int(it["connections"]),
            }
        )
        grouped[key]["attempt_number"] += int(it["attempt_number"])
        grouped[key]["service"] += int(it["service"])
        grouped[key]["total_connections"] += int(it["connections"])

    # 4) Конверсия по группе и финальная чистка
    for g in grouped.values():
        svc = int(g["service"])
        conn = int(g["total_connections"])
        g["conversion"] = f"{round(conn / svc * 100)}%" if svc > 0 else "–"
        g.pop("total_connections", None)

    # 5) Сортировка групп по attempt_number (desc)
    grouped_sorted = sorted(
        grouped.values(),
        key=lambda r: int(r.get("attempt_number") or 0),
        reverse=True,
    )

    # 6) Рендер таблицы
    table = TableConstructor()
    table.genCellTH("Старый ТП")
    table.genCellTH("Минимальный ТП на продажу")
    table.genCellTH("Подключенный ТП")
    table.genCellTH("Количество попыток дозвона")
    table.genCellTH("Дозвонов > 5 сек.")
    table.genCellTH("Всего подключений")
    table.genCellTH("Конверсия")
    table.genRow()

    for row in grouped_sorted:
        # genDataRow ожидает ключи:
        # old_name, qwes_recomendet, attempt_number, service, new_items, conversion
        table.genDataRow(row)

    return table.renderTable()

@op(
    out=Out(str, description="HTML-пивот: date(d.m) → колонки, name → строки, сумма value; итоги и доля %."),
    description=(
        "Аналог PHP renderSalesTable: PaintType='asc', OrderByTotal='desc', "
        "ShowSummaryXAxis=True, ShowSummaryYAxis=True, ShowTotalPercentage=True."
    ),
)
def render_sales_table_op(context, item: List[Dict[str, Any]]) -> str:
    pt = PivotTable()

    # поведение как в PHP
    pt.PaintType = 'asc'
    pt.OrderByTotal = 'desc'
    pt.ShowTotalPercentage = True

    pt.ShowSummaryXAxis = True
    pt.ShowSummaryYAxis = True
    pt.ShowAverageYAxis = False   # как в PHP не требовалось
    pt.ShowMinYAxis = False
    pt.ShowMaxYAxis = False
    pt.RoundValue = None          # чтобы целые печатались без .0 (см. реализацию PivotTable)

    pt.Items = item or []
    pt.Fields = [
        {'field': 'date', 'type': 'period', 'format': 'd.m'},
        {'field': 'name', 'type': 'row'},
        {'field': 'value', 'type': 'value'},  # агрегируется суммой
    ]

    return pt.getTable()

@op(
    out=Out(str, description="HTML-таблица: среднее затраченное время по логинам и датам (pivot avg) с нижней строкой 'Среднее'."),
    description=(
        "Сводная таблица (Pivot): колонки — даты (d.m), строки — login, ячейки — avg(value). "
        "Сортировка строк по среднему (desc). Раскраска по столбцам (desc). "
        "Нижняя строка — 'Среднее' по каждой дате и общий средний."
    ),
)
def project_spent_time_op(context, items: List[Dict[str, Any]]) -> str:
    rows = list(items or [])

    # 1) сортировка по дате (как в PHP)
    rows.sort(key=lambda r: (_parse_dt(r.get("date")) or datetime.max))

    # 2) соберём все даты и отсортируем
    dates_dt: List[datetime] = []
    for r in rows:
        dt = _parse_dt(r.get("date"))
        if dt:
            dates_dt.append(dt)
    # уникальные даты по дню
    seen_days = set()
    ordered_dt: List[datetime] = []
    for dt in sorted(dates_dt):
        key = dt.date()
        if key not in seen_days:
            seen_days.add(key)
            ordered_dt.append(dt)

    # подписи колонок в формате d.m (как ['15.08', ...])
    cols = [_date_label(dt) for dt in ordered_dt]

    # 3) агрегация для avg: накопим сумму и количество для (login, col)
    agg_sum: DefaultDict[str, DefaultDict[str, float]] = defaultdict(lambda: defaultdict(float))
    agg_cnt: DefaultDict[str, DefaultDict[str, int]] = defaultdict(lambda: defaultdict(int))

    for r in rows:
        login = str(r.get("login", "") or "")
        dt = _parse_dt(r.get("date"))
        if not dt:
            continue
        col = _date_label(dt)
        val = _to_float(r.get("value"))
        agg_sum[login][col] += val
        agg_cnt[login][col] += 1

    # 4) считаем средние по ячейкам и среднее по строке (для сортировки)
    row_avgs: Dict[str, float] = {}
    pivot_avg: Dict[str, Dict[str, float]] = {}
    for login, colmap in agg_sum.items():
        row_vals: List[float] = []
        avg_row_cells: Dict[str, float] = {}
        for col in cols:
            s = colmap.get(col, 0.0)
            c = agg_cnt[login].get(col, 0)
            cell = (s / c) if c > 0 else 0.0
            avg_row_cells[col] = cell
            if c > 0:
                row_vals.append(cell)
        # среднее по строке: среднее по присутствующим дням
        row_avg = (sum(row_vals) / len(row_vals)) if row_vals else 0.0
        row_avgs[login] = row_avg
        pivot_avg[login] = avg_row_cells

    # 5) порядок строк: по убыванию среднего (OrderByTotal='desc')
    row_order = sorted(pivot_avg.keys(), key=lambda k: row_avgs.get(k, 0.0), reverse=True)

    # 6) min/max по колонкам (для раскраски); игнорируем нули, чтобы пустоты не делали колонку «бледной»
    col_minmax: Dict[str, Tuple[float, float]] = {}
    for col in cols:
        vals = [pivot_avg[l].get(col, 0.0) for l in row_order]
        present = [v for v in vals if v > 0]
        if present:
            col_minmax[col] = (min(present), max(present))
        else:
            col_minmax[col] = (0.0, 0.0)

    # 7) нижняя строка «Среднее» по каждой дате и общий средний
    col_avg: Dict[str, float] = {}
    all_vals: List[float] = []
    for col in cols:
        s, c = 0.0, 0
        for l in row_order:
            v = pivot_avg[l].get(col, 0.0)
            # учитываем только реальные ячейки (как для avg)
            if agg_cnt[l].get(col, 0) > 0:
                s += v
                c += 1
                all_vals.append(v)
        col_avg[col] = (s / c) if c > 0 else 0.0
    grand_avg = (sum(all_vals) / len(all_vals)) if all_vals else 0.0

    # 8) рендер через TableConstructor
    t = TableConstructor()
    t.EscapeHTMLDefault = True  # безопаснее экранировать по умолчанию

    # Заголовок
    t.genCellTH("Логин")
    for col in cols:
        t.genCellTH(col)
    t.genCellTH("Среднее")  # total_text
    t.genRow()

    # Строки (логины)
    row_title_style = "font-weight: bold; text-align: left"
    for login in row_order:
        t.genCellTD(login, style=row_title_style)

        for col in cols:
            v = pivot_avg[login].get(col, 0.0)
            cmin, cmax = col_minmax.get(col, (0.0, 0.0))
            bg = _gradient_desc(v, cmin, cmax)
            t.genCellTD(_round1(v), style=f"background: {bg};")

        t.genCellTD(_round1(row_avgs.get(login, 0.0)))
        t.genRow()

    # Нижняя строка «Среднее» (ShowSummaryXAxisNew = true)
    t.genCellTH("Среднее")
    for col in cols:
        t.genCellTH(_round1(col_avg[col]))
    t.genCellTH(_round1(grand_avg))
    t.genRow()

    return t.renderTable()

@op(
    out=Out(str, description="SVG Base64 график: динамика среднего времени диалога (AHT), сек."),
    description=(
        "Линейный график за последние дни: среднее время диалога (более 10 сек). "
        "Y фиксирован: 0..170 сек."
    ),
)
def render_call_aht_graphic_op(context, general_performance_data: Dict[str, Any]) -> str:
    """
    Аналог PHP:
      - GraphTitle: 'Данный график показывает динамику ...'
      - YAxesMin=0, YAxesMax=170
      - YAxesName='Секунды'
      - setColourPack(2)
      - Fields: date (label), value ('Звонки')
    """

    # --- Собираем граф через ваш BasicLineGraphics ---
    graphic = BasicLineGraphics(
        graph_title="Данный график показывает динамику среднего времени диалога за последние 30 дней (данные по диалогам более 10 секунд)",
        x_axes_name="Дни",
        y_axes_name="Секунды",
        target=None,               # без плана
        stroke='smooth',
        show_tooltips=True,
        y_axes_min=0,
        y_axes_max=170,
        items=general_performance_data.get("ahtAllMonth") or [],
        fields=[
            {"field": "date", "title": "Дни", "type": "label"},
            {"field": "value", "title": "Звонки"},
        ],
        width=1920,
        height=500,
    )

    # Цветовая палитра, как в PHP setColourPack(2)
    if hasattr(graphic, "set_colour_pack"):
        try:
            graphic.set_colour_pack(2)
        except Exception:
            pass

    # Подготовка внутренних структур (labels, datasets)
    graphic.make_preparation()

    # --- Парсим даты из graphic.labels ---
    parsed_dates = []
    for lbl in graphic.labels:
        s = (lbl or "").strip().split()[0]
        dt = None
        for fmt in ("%d.%m.%Y", "%d.%m", "%d.%m.%y", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(s, fmt)
                if fmt == "%d.%m":
                    # если год не дан — возьмем текущий
                    dt = dt.replace(year=datetime.now().year)
                break
            except Exception:
                continue
        if dt is None:
            # последний шанс — ISO
            try:
                dt = datetime.fromisoformat(s)
            except Exception:
                pass
        if dt is None:
            context.log.warning(f"Не удалось распарсить дату '{lbl}', пропускаю точку")
            continue
        parsed_dates.append(dt)

    # --- Данные единственной серии ('Звонки') ---
    series_vals = []
    series_title = "Звонки"
    for ds in graphic.datasets:
        if ds.get("name") == series_title:
            series_vals = ds.get("data", [])
            break

    # Отсечем значения под те же индексы, что и даты
    n = min(len(parsed_dates), len(series_vals))
    parsed_dates = parsed_dates[:n]
    y = [float(v) if v is not None else None for v in series_vals[:n]]

    # --- Фигура и оси ---
    dpi = 100
    fig, ax = plt.subplots(figsize=(graphic.width / dpi, graphic.height / dpi), dpi=dpi)
    fig.subplots_adjust(top=0.85)
    fig.patch.set_facecolor('#f8f9fa')
    ax.set_facecolor('#ffffff')

    # Цвет линии: если у BasicLineGraphics есть палитра — возьмём первый цвет из неё;
    # иначе используем тёмно-серый из пакета (setColourPack(2) → '#222d32').
    line_color = '#222d32'
    if hasattr(graphic, "colors"):
        try:
            if isinstance(graphic.colors, (list, tuple)) and graphic.colors:
                line_color = graphic.colors[0]
        except Exception:
            pass

    # --- Линия и маркеры ---
    if parsed_dates and y:
        ax.plot(parsed_dates, y, linewidth=2.5, label=series_title, color=line_color)

        # Маркеры + подписи в «капсуле»
        for dt_point, val in zip(parsed_dates, y):
            if val is None:
                continue
            ax.plot(
                dt_point, val,
                marker='o', markersize=6,
                markerfacecolor=line_color, markeredgecolor='white', markeredgewidth=1,
                zorder=4
            )
            label_text = f"{val:.0f}" if float(val).is_integer() else f"{val:.1f}"
            ax.annotate(
                label_text, (dt_point, val),
                textcoords="offset points", xytext=(0, 0), ha='center', fontsize=8, fontweight='bold',
                bbox=dict(boxstyle="round,pad=0.3", facecolor=line_color, edgecolor='none', alpha=0.9),
                color='white', zorder=5
            )

    # --- Ось X: красивые границы и формат дат ---
    if parsed_dates:
        ax.set_xticks(parsed_dates)
        x_labels = [dt.strftime('%d.%m') for dt in parsed_dates]
        if len(x_labels) >= 2:
            x_labels[0] = ''
            x_labels[-1] = ''
        ax.set_xticklabels(x_labels, rotation=0, fontsize=10)

        ax.xaxis.set_major_formatter(mdates.DateFormatter('%d.%m'))
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))

        # небольшие «поля» по краям оси X
        if len(parsed_dates) >= 2:
            step = parsed_dates[1] - parsed_dates[0]
            ax.set_xlim(parsed_dates[0] - step * 1.7, parsed_dates[-1] + step * 1.7)
        else:
            ax.set_xlim(parsed_dates[0] - timedelta(hours=12), parsed_dates[0] + timedelta(hours=12))

    # --- Ось Y: ЖЁСТКО 0..170, как в PHP ---
    ax.set_ylim(0, 170)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, pos: f"{int(x)}"))

    # Сетка и стили
    ax.grid(True, linestyle='-', linewidth=0.5, color='#e2e8f0', alpha=0.7, axis='y')
    ax.set_axisbelow(True)

    ax.set_xlabel(graphic.x_axes_name, fontsize=10, color='#4a5568')
    ax.set_ylabel(graphic.y_axes_name, fontsize=10, color='#4a5568')

    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_color('#e2e8f0')
    ax.spines['bottom'].set_color('#e2e8f0')
    ax.tick_params(axis='x', colors='#4a5568', labelsize=7)
    ax.tick_params(axis='y', colors='#4a5568', labelsize=10)

    # Заголовок сверху (как у вас в BasicLineGraphics)
    ax.text(
        0.0, 1.08, graphic.graph_title if hasattr(graphic, "graph_title") else "",
        horizontalalignment='left',
        transform=ax.transAxes,
        fontsize=11,
        color='#2d3748',
        fontweight="bold",
        wrap=True
    )

    # Легенда — у нас одна серия, можно не показывать; оставлю на случай расширения
    ax.legend(loc='upper right', fontsize=8, frameon=False, ncol=1)

    # --- Экспорт в SVG Base64 ---
    buf = io.BytesIO()
    fig.savefig(buf, format="svg", bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    b64 = base64.b64encode(buf.read()).decode("utf-8")

    context.log.info("render_call_aht_graphic_op: points=%d, y in [0,170]", len(parsed_dates))
    return b64

@op(
    out=Out(str, description="SVG Base64 график: динамика загруженности специалистов, %"),
    description=(
        "Линейный график загруженности специалистов за последние дни. "
        "Есть плановая линия (70%). Ось Y: 0..100."
    ),
)
def render_occupancy_graphic_op(context, items: List[Dict[str, Any]]) -> str:
    # Сборка объекта как в PHP
    graphic = BasicLineGraphics(
        graph_title="Данный график показывает динамику загруженнности специалистов за последние 30 дней в %",
        x_axes_name="Дни",
        y_axes_name="Загруженность",
        target=70,               # План
        stroke='smooth',
        show_tooltips=True,
        y_axes_min=0,
        y_axes_max=100,
        items=items or [],
        fields=[
            {"field": "date", "title": "Дни", "type": "label"},
            {"field": "value", "title": "Звонки"},
        ],
        width=1920,
        height=500,
    )

    if hasattr(graphic, "set_colour_pack"):
        try:
            graphic.set_colour_pack(2)
        except Exception:
            pass

    graphic.make_preparation()

    # Парсим даты из graphic.labels
    parsed_dates: List[datetime] = []
    for lbl in getattr(graphic, "labels", []):
        s = (lbl or "").strip().split()[0]
        dt = None
        for fmt in ("%d.%m.%Y", "%d.%m", "%d.%m.%y", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(s, fmt)
                if fmt == "%d.%m":
                    dt = dt.replace(year=datetime.now().year)
                break
            except Exception:
                continue
        if dt is None:
            try:
                dt = datetime.fromisoformat(s)
            except Exception:
                pass
        if dt is None:
            context.log.warning(f"render_occupancy_graphic_op: не распарсил дату '{lbl}', пропускаю")
            continue
        parsed_dates.append(dt)

    # Берём единственную серию «Звонки»
    y_vals = []
    for ds in getattr(graphic, "datasets", []):
        if ds.get("name") == "Звонки":
            y_vals = ds.get("data", [])
            break

    n = min(len(parsed_dates), len(y_vals))
    parsed_dates = parsed_dates[:n]
    y = [None if v is None else float(v) for v in y_vals[:n]]

    # Фигура/оси
    dpi = 100
    fig, ax = plt.subplots(figsize=(graphic.width / dpi, graphic.height / dpi), dpi=dpi)
    fig.subplots_adjust(top=0.85)
    fig.patch.set_facecolor('#f8f9fa')
    ax.set_facecolor('#ffffff')

    # Цвет линии (если у BasicLineGraphics есть палитра — берём первый, иначе дефолт)
    line_color = '#e2007a'
    if hasattr(graphic, "colors"):
        try:
            if isinstance(graphic.colors, (list, tuple)) and graphic.colors:
                line_color = graphic.colors[0]
        except Exception:
            pass

    # Линия + маркеры + подписи
    if parsed_dates and y:
        ax.plot(parsed_dates, y, linewidth=2.5, label="Звонки", color=line_color)
        for dtp, val in zip(parsed_dates, y):
            if val is None:
                continue
            ax.plot(
                dtp, val,
                marker='o', markersize=6,
                markerfacecolor=line_color, markeredgecolor='white', markeredgewidth=1,
                zorder=4
            )
            label_text = f"{val:.0f}" if float(val).is_integer() else f"{val:.1f}"
            ax.annotate(
                label_text, (dtp, val),
                textcoords="offset points", xytext=(0, 0),
                ha='center', fontsize=8, fontweight='bold',
                bbox=dict(boxstyle="round,pad=0.3", facecolor=line_color, edgecolor='none', alpha=0.9),
                color='white', zorder=5
            )

    # Ось X
    if parsed_dates:
        ax.set_xticks(parsed_dates)
        labels = [dt.strftime('%d.%m') for dt in parsed_dates]
        if len(labels) >= 2:
            labels[0] = ''
            labels[-1] = ''
        ax.set_xticklabels(labels, rotation=0, fontsize=10)

        ax.xaxis.set_major_formatter(mdates.DateFormatter('%d.%m'))
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))

        if len(parsed_dates) >= 2:
            step = parsed_dates[1] - parsed_dates[0]
            ax.set_xlim(parsed_dates[0] - step * 1.7, parsed_dates[-1] + step * 1.7)
        else:
            ax.set_xlim(parsed_dates[0] - timedelta(hours=12), parsed_dates[0] + timedelta(hours=12))

    # Ось Y: жёстко 0..100
    ax.set_ylim(0, 100)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, pos: f"{int(x)}"))

    # Плановая линия 70%
    target = 70
    ax.axhline(y=target, color='#38a169', linestyle='-', linewidth=2, alpha=0.8, zorder=1)
    trans = mtransforms.blended_transform_factory(ax.transAxes, ax.transData)
    ax.text(0, target, 'План',
            transform=trans, ha='left', va='center',
            bbox=dict(boxstyle="round,pad=0.3", facecolor='#38a169', edgecolor='none'),
            color='white', fontweight='bold', fontsize=8,
            clip_on=False, zorder=3)

    # Сетка/стили
    ax.grid(True, linestyle='-', linewidth=0.5, color='#e2e8f0', alpha=0.7, axis='y')
    ax.set_axisbelow(True)

    ax.set_xlabel(graphic.x_axes_name, fontsize=10, color='#4a5568')
    ax.set_ylabel(graphic.y_axes_name, fontsize=10, color='#4a5568')

    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_color('#e2e8f0')
    ax.spines['bottom'].set_color('#e2e8f0')
    ax.tick_params(axis='x', colors='#4a5568', labelsize=7)
    ax.tick_params(axis='y', colors='#4a5568', labelsize=10)

    # Заголовок
    ax.text(
        0.0, 1.08, getattr(graphic, "graph_title", ""),
        horizontalalignment='left',
        transform=ax.transAxes,
        fontsize=11,
        color='#2d3748',
        fontweight="bold",
        wrap=True
    )

    # Легенда (одна серия — опционально)
    ax.legend(loc='upper right', fontsize=8, frameon=False, ncol=1)

    # Экспорт в SVG Base64
    buf = io.BytesIO()
    fig.savefig(buf, format="svg", bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    b64 = base64.b64encode(buf.read()).decode("utf-8")

    context.log.info("render_occupancy_graphic_op: points=%d (Y: 0..100, plan=70%%)", len(parsed_dates))
    return b64

@op(
    out=Out(str, description="HTML-пивот: колонки — даты (d.m), строки — login, значения — AVG(value). "
                             "Снизу строка со средними по колонкам. Раскраска по столбцам (desc)."),
    description="Аналог PHP projectSpentTime: агрегирование средних значений по логинам и датам."
)
def render_project_spent_time_op(context, items: List[Dict[str, Any]]) -> str:
    rows = items or []

    # 1) Сортировка исходных записей по дате (как в PHP)
    rows_sorted = sorted(rows, key=lambda r: _to_ts(r.get("date")))

    # 2) Колонки: список уникальных дат-лейблов d.m, по возрастанию
    seen = set()
    cols_sorted: List[str] = []
    for r in rows_sorted:
        lbl, ts = _to_date_label(r.get("date"))
        # вставляем в порядке ts (rows уже отсортированы), но без дублей
        if lbl not in seen:
            seen.add(lbl)
            cols_sorted.append(lbl)

    # 3) Пивот: login -> {date_label: [values...]}  (для AVG)
    buckets: Dict[str, Dict[str, List[float]]] = {}
    for r in rows_sorted:
        login = str(r.get("login", ""))
        lbl, _ = _to_date_label(r.get("date"))
        val = _to_float(r.get("value"))
        if val is None:
            continue
        buckets.setdefault(login, {}).setdefault(lbl, []).append(val)

    # 4) Усреднение по ячейкам; расчет row_avg (для сортировки desc)
    pivot_avg: Dict[str, Dict[str, float]] = {}
    row_avg_value: Dict[str, float] = {}
    for login, by_date in buckets.items():
        out_row: Dict[str, float] = {}
        vals_for_row_avg: List[float] = []
        for c in cols_sorted:
            arr = by_date.get(c)
            if arr:
                v = sum(arr) / len(arr)
                out_row[c] = v
                vals_for_row_avg.append(v)
        pivot_avg[login] = out_row
        row_avg_value[login] = (sum(vals_for_row_avg) / len(vals_for_row_avg)) if vals_for_row_avg else 0.0

    # 5) Порядок строк: по среднему по строке (desc)
    row_order = sorted(pivot_avg.keys(), key=lambda lg: row_avg_value.get(lg, 0.0), reverse=True)

    # 6) Мин/Макс по каждому столбцу (для раскраски)
    col_minmax: Dict[str, Tuple[float | None, float | None]] = {}
    for c in cols_sorted:
        vals = [pivot_avg.get(lg, {}).get(c) for lg in row_order]
        present = [v for v in vals if v is not None]
        if present:
            col_minmax[c] = (min(present), max(present))
        else:
            col_minmax[c] = (None, None)

    # 7) Итоговая строка: среднее по колонке (среди имеющихся значений)
    col_means: Dict[str, float | None] = {}
    for c in cols_sorted:
        vals = [pivot_avg.get(lg, {}).get(c) for lg in row_order]
        present = [v for v in vals if v is not None]
        col_means[c] = (sum(present) / len(present)) if present else None
    # общий "Среднее" по всей таблице — среднее из всех присутствующих ячеек
    all_present = [v for lg in row_order for v in pivot_avg.get(lg, {}).values() if v is not None]
    grand_mean = (sum(all_present) / len(all_present)) if all_present else None

    # 8) Рендер через TableConstructor
    t = TableConstructor()

    # Заголовок
    t.genCellTH("Логин")
    for c in cols_sorted:
        t.genCellTH(c)
    t.genCellTH("Среднее")
    t.genRow()

    # Строки данных
    for lg in row_order:
        # стиль заголовка строки как в PHP: "font-weight: bold; text-align: left"
        t.genCellTD(lg, style="font-weight: bold; text-align: left;")

        row_map = pivot_avg.get(lg, {})
        vals_for_avg: List[float] = []

        for c in cols_sorted:
            v = row_map.get(c)
            if v is None:
                t.genCellTD("")  # пусто, если нет данных
                continue
            vals_for_avg.append(v)
            cmin, cmax = col_minmax.get(c, (None, None))
            bg = _gradient_colour(v, cmin, cmax, paint_type="desc")
            t.genCellTD(_fmt_num_dec(v, 1), style=f"background: {bg};")

        row_mean = (sum(vals_for_avg) / len(vals_for_avg)) if vals_for_avg else None
        t.genCellTD(_fmt_num_dec(row_mean, 1))

        t.genRow()

    # Строка «Среднее» по колонкам внизу (ShowSummaryXAxisNew = true)
    t.genCellTH("Среднее")
    for c in cols_sorted:
        t.genCellTH(_fmt_num_dec(col_means.get(c), 1))
    t.genCellTH(_fmt_num_dec(grand_mean, 1))
    t.genRow()

    return t.renderTable()

@op(required_resource_keys={"report_utils"})
def report_pdf_gtm(context, gtm_fig1, gtm_fig10, gtm_fig11, gtm_fig12, gtm_fig13, gtm_fig14):
    report = MailReport()
    report.image_mode = "file"

    APP_ROOT = Path(__file__).resolve().parents[2]

    LOGO_PATH = APP_ROOT / "utils" / "logo_png" / "logo.png"

    report.logo_file_override = str(LOGO_PATH)
    dates = context.resources.report_utils.get_utils()
    report_date = datetime.strptime(dates.ReportDate, "%Y-%m-%d")
    gtm_p1 = ""
    gtm_p1 += report.set_basic_header(f"ОБЩАЯ ЕЖЕДНЕВНАЯ СТАТИСТИКА ГТМ НА {report_date.strftime('%d.%m.%Y')}")
    gtm_p1 += report.add_block_header("Срез по основным показателям")
    gtm_p1 += report.add_content_to_statistic(gtm_fig1)

    gtm_p1 += report.add_block_header("Статистика по затраченным ресурсам ГТМ")

    gtm_p1 += report.add_module_header("Затраченное время на проекты за последние 30 дней (минут)")
    gtm_p1 += report.add_content_to_statistic(gtm_fig10)

    gtm_p1 += report.add_module_header("Процент времени операторов на звонках по всем проектам к оплачиваемому времени")
    gtm_p1 += report.add_content_to_statistic(gtm_fig11)

    gtm_p1 += report.add_module_header("Динамика AHT")
    gtm_p1 += report.add_figure(gtm_fig12)

    gtm_p1 += report.add_module_header("Динамика Occupancy. Цель: > 70%")
    gtm_p1 += report.add_figure(gtm_fig13)

    gtm_p1 += report.add_module_header("Динамика Utilization Rate. Цель ≥ 86%.")
    gtm_p1 += report.add_figure(gtm_fig14)

    gtm_tmp1 = "gtm_page1.pdf"

    html_to_pdf_page(html = gtm_p1, output_path=gtm_tmp1)

    gtm_final_pdf = "gtm_report.pdf"
    merge_pdfs([gtm_tmp1], gtm_final_pdf)

    gtm_final_path = str(Path(gtm_final_pdf).resolve())
    context.log.info(f"gtm_final_pdf_path: {gtm_final_path}")
    return gtm_final_path