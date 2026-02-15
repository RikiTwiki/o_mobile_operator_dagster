# user_code/ops/personal_telemarketing_daily_statistics/visualization_ops.py
from __future__ import annotations

import base64
import io
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import matplotlib.dates as mdates
from matplotlib import pyplot as plt
from matplotlib.ticker import FuncFormatter

from dagster import op, Out, get_dagster_logger

# Таблицы/графики из твоих шаблонов
from utils.templates.tables.pivot_table import PivotTable
from utils.templates.tables.table_constructor import TableConstructor
from utils.templates.charts.basic_line_graphics import BasicLineGraphics

# Почтовый билдер / PDF
from utils.mail.mail_report import MailReport, html_to_pdf_page, merge_pdfs
from pathlib import Path

def _empty_svg_b64(width: int = 1600, height: int = 420, text: str = "Нет данных") -> str:
    svg = f"""<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">
  <rect width="100%" height="100%" fill="white"/>
  <text x="50%" y="50%" dominant-baseline="middle" text-anchor="middle"
        font-family="sans-serif" font-size="18" fill="#777">{text}</text>
</svg>"""
    return base64.b64encode(svg.encode("utf-8")).decode("ascii")

def _empty_table_html(message: str = "Нет данных") -> str:
    return f"""
<table style="width:100%;border-collapse:collapse;border:1px solid #e5e7eb;font-family:sans-serif">
  <thead>
    <tr><th style="padding:8px;border:1px solid #e5e7eb;text-align:left">{html.escape(message)}</th></tr>
  </thead>
  <tbody>
    <tr><td style="padding:12px;border:1px solid #e5e7eb;color:#6b7280">—</td></tr>
  </tbody>
</table>""".strip()

# ────────────────────────── helpers ──────────────────────────
def _parse_dt_any(s: Any) -> Optional[datetime]:
    if isinstance(s, datetime):
        return s
    if s is None:
        return None
    ss = str(s).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d.%m.%Y", "%d.%m"):
        try:
            dt = datetime.strptime(ss, fmt)
            if fmt == "%d.%m":
                dt = dt.replace(year=datetime.now().year)
            return dt
        except Exception:
            continue
    return None


def _line_svg_from_basic(graphic: BasicLineGraphics, series_name: str = "Значение") -> str:
    """
    Рендерит SVG Base64 по данным из BasicLineGraphics (labels/datasets).
    """
    # Готовим внутренние структуры
    graphic.make_preparation()

    # 1) Даты (по labels)
    xs: List[datetime] = []
    for lbl in getattr(graphic, "labels", []):
        # label может быть 'dd.mm' / 'dd.mm.yyyy' / ISO; берём первый токен
        token = (lbl or "").strip().split()[0]
        dt = _parse_dt_any(token)
        if dt:
            xs.append(dt)

    # 2) Находим серию
    ys: List[float] = []
    for ds in getattr(graphic, "datasets", []):
        if ds.get("name") == series_name:
            ys = [None if v is None else float(v) for v in ds.get("data", [])]
            break
    n = min(len(xs), len(ys))
    xs, ys = xs[:n], ys[:n]

    # 3) Фигура
    width = getattr(graphic, "width", 1920)
    height = getattr(graphic, "height", 500)
    dpi = 100
    fig, ax = plt.subplots(figsize=(width / dpi, height / dpi), dpi=dpi)
    fig.subplots_adjust(top=0.85)
    fig.patch.set_facecolor("#f8f9fa")
    ax.set_facecolor("#ffffff")

    # Линия
    line_color = "#222d32"
    if hasattr(graphic, "colors"):
        try:
            if isinstance(graphic.colors, (list, tuple)) and graphic.colors:
                line_color = graphic.colors[0]
        except Exception:
            pass

    if xs and ys:
        ax.plot(xs, ys, linewidth=2.5, color=line_color, label=series_name)
        for x, y in zip(xs, ys):
            if y is None:
                continue
            ax.plot(
                x, y,
                marker="o", markersize=6,
                markerfacecolor=line_color, markeredgecolor="white", markeredgewidth=1,
                zorder=4,
            )
            label_text = f"{y:.0f}" if float(y).is_integer() else f"{y:.1f}"
            ax.annotate(
                label_text, (x, y),
                textcoords="offset points", xytext=(0, 0), ha="center", fontsize=8, fontweight="bold",
                bbox=dict(boxstyle="round,pad=0.3", facecolor=line_color, edgecolor="none", alpha=0.9),
                color="white", zorder=5,
            )

    # Оси
    if xs:
        ax.set_xticks(xs)
        labels = [dt.strftime("%d.%m") for dt in xs]
        if len(labels) >= 2:
            labels[0] = ""
            labels[-1] = ""
        ax.set_xticklabels(labels, rotation=0, fontsize=10)

        ax.xaxis.set_major_formatter(mdates.DateFormatter("%d.%m"))
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))

        if len(xs) >= 2:
            step = xs[1] - xs[0]
            ax.set_xlim(xs[0] - step * 1.7, xs[-1] + step * 1.7)
        else:
            ax.set_xlim(xs[0] - timedelta(hours=12), xs[0] + timedelta(hours=12))

    # Диапазоны Y (если заданы в графике)
    ymin = getattr(graphic, "y_axes_min", None)
    ymax = getattr(graphic, "y_axes_max", None)
    if ymin is not None and ymax is not None:
        ax.set_ylim(float(ymin), float(ymax))
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, pos: f"{int(v)}"))

    # Сетка/стили
    ax.grid(True, linestyle="-", linewidth=0.5, color="#e2e8f0", alpha=0.7, axis="y")
    ax.set_axisbelow(True)

    ax.set_xlabel(getattr(graphic, "x_axes_name", "Дни"), fontsize=10, color="#4a5568")
    ax.set_ylabel(getattr(graphic, "y_axes_name", ""), fontsize=10, color="#4a5568")

    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color("#e2e8f0")
    ax.spines["bottom"].set_color("#e2e8f0")
    ax.tick_params(axis="x", colors="#4a5568", labelsize=7)
    ax.tick_params(axis="y", colors="#4a5568", labelsize=10)

    # Заголовок
    title = getattr(graphic, "graph_title", "") or ""
    if title:
        ax.text(
            0.0, 1.08, title,
            horizontalalignment="left", transform=ax.transAxes,
            fontsize=11, color="#2d3748", fontweight="bold", wrap=True,
        )

    # Экспорт
    buf = io.BytesIO()
    fig.savefig(buf, format="svg", bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")


# ────────────────────────── SALES ──────────────────────────
@op(
    out=Out(str, description="HTML: Персональная статистика продаж (pivot: date d.m → колонки, name → строки, сумма value)"),
)
def render_personal_sales_table_op(context, sales_personal_gtm: List[Dict[str, Any]]) -> str:
    """
    Ожидает список dict с ключами: date(или day/month/Дата), name, value.
    При ошибке/пустых данных возвращает пустую таблицу.
    """
    try:
        # 1) нормализация и отсев битых строк
        items: List[Dict[str, Any]] = []
        skipped = 0
        for r in (sales_personal_gtm or []):
            d = r.get("date") or r.get("day") or r.get("month") or r.get("Дата")
            name = r.get("name") or r.get("employee") or r.get("user") or r.get("operator")
            v = r.get("value")
            if not d or name is None or v is None:
                skipped += 1
                continue
            items.append({"date": str(d), "name": str(name), "value": v})
        if skipped:
            context.log.warning(f"[personal_sales_table] skipped rows without date/name/value: {skipped}")

        if not items:
            context.log.warning("[personal_sales_table] no usable data → empty table")
            return _empty_table_html("Нет данных")

        # 2) построение сводной таблицы
        pt = PivotTable()
        pt.PaintType = "asc"
        pt.OrderByTotal = "desc"
        pt.ShowSummaryXAxis = True
        pt.ShowSummaryYAxis = True
        pt.Items = items
        pt.Fields = [
            {"field": "date", "type": "period", "format": "d.m"},
            {"field": "name", "type": "row"},
            {"field": "value", "type": "value"},
        ]
        return pt.getTable()

    except Exception as e:
        context.log.error(f"[personal_sales_table] render failed: {e}", exc_info=True)
        return _empty_table_html("Ошибка построения таблицы")


@op(
    out=Out(str, description="SVG Base64: Динамика продаж за 30 дней (линия)"),
)
def render_personal_sales_dynamic_graph_op(context, sales_a: List[Dict[str, Any]]) -> str:
    """
    Ожидает список dict с ключами 'date' (YYYY-MM-DD или d.m.YYYY) и 'value'.
    При любой ошибке возвращает пустой график.
    """
    try:
        # 1) Нормализация входа и отсев битых строк
        items: List[Dict[str, Any]] = []
        skipped = 0
        for r in (sales_a or []):
            d = r.get("date") or r.get("day") or r.get("month") or r.get("Дата")
            v = r.get("value")
            if not d or v is None:
                skipped += 1
                continue
            items.append({"date": str(d), "value": v})

        if skipped:
            context.log.warning(f"[personal_sales_dynamic] skipped rows without date/value: {skipped}")

        if not items:
            context.log.warning("[personal_sales_dynamic] no usable data → empty chart")
            return _empty_svg_b64(text="Нет данных")

        # 2) Построение графика
        g = BasicLineGraphics(
            graph_title="Динамика продаж за последние 30 дней",
            x_axes_name="Дни",
            y_axes_name="Количество",
            y_axes_min=0,
            y_axes_max=None,
            items=items,
            fields=[
                {"field": "date", "title": "Дни", "type": "label"},
                {"field": "value", "title": "Значение"},
            ],
            width=1600,
            height=420,
            show_tooltips=True,
            stroke="smooth",
        )
        try:
            g.set_colour_pack(2)
        except Exception:
            # косметика не критична
            pass

        return _line_svg_from_basic(g, series_name="Значение")

    except Exception as e:
        context.log.error(f"[personal_sales_dynamic] render failed: {e}", exc_info=True)
        return _empty_svg_b64(text="Ошибка отрисовки")


@op(
    out=Out(str, description="HTML: Динамика активаций в % (pivot)"),
)
def render_personal_activation_table_op(context, personal_activation_data: List[Dict[str, Any]]) -> str:
    """
    Ожидает список dict с ключами: date(или day/month/Дата), name, value.
    При ошибке/пустых данных возвращает пустую таблицу.
    """
    try:
        # 1) нормализация входа
        items: List[Dict[str, Any]] = []
        skipped = 0
        for r in (personal_activation_data or []):
            d = r.get("date") or r.get("day") or r.get("month") or r.get("Дата")
            name = r.get("name") or r.get("employee") or r.get("user") or r.get("operator")
            v = r.get("value")
            if not d or name is None or v is None:
                skipped += 1
                continue
            items.append({"date": str(d), "name": str(name), "value": v})
        if skipped:
            context.log.warning(f"[personal_activation_table] skipped rows without date/name/value: {skipped}")

        if not items:
            context.log.warning("[personal_activation_table] no usable data → empty table")
            return _empty_table_html("Нет данных")

        # 2) pivot-таблица
        pt = PivotTable()
        pt.PaintType = "desc"
        pt.OrderByTotal = "desc"
        pt.ShowSummaryXAxis = False
        pt.ShowSummaryYAxis = False
        pt.Items = items
        pt.Fields = [
            {"field": "date", "type": "period", "format": "d.m"},
            {"field": "name", "type": "row"},
            {"field": "value", "type": "value"},
        ]
        return pt.getTable()

    except Exception as e:
        context.log.error(f"[personal_activation_table] render failed: {e}", exc_info=True)
        return _empty_table_html("Ошибка построения таблицы")


# ────────────────────────── STATUSES ──────────────────────────
def _pivot_row_value(items: List[Dict[str, Any]], paint: str = "asc") -> str:
    # 0) нормализуем вход и фильтруем записи без даты
    raw = items or []
    data = [r for r in raw if isinstance(r, dict) and r.get("date")]

    # 1) если данных нет — аккуратно вернём пустую табличку, чтобы PivotTable не падал
    if not data:
        t = TableConstructor()
        t.genCellTH("Нет данных за период")
        t.genRow()
        return t.renderTable()

    # 2) авто-детект формата даты
    iso_like = any(isinstance(r.get("date"), str) and "-" in r["date"] for r in data)
    date_fmt = "%Y-%m-%d" if iso_like else "d.m"

    # 3) обычный PivotTable
    pt = PivotTable()
    pt.PaintType = paint
    pt.OrderByTotal = "desc"
    pt.ShowSummaryXAxis = False
    pt.ShowSummaryYAxis = False
    pt.Items = data
    pt.Fields = [
        {"field": "date", "type": "period", "format": date_fmt},
        {"field": "row",  "type": "row"},
        {"field": "value","type": "value"},
    ]
    return pt.getTable()


@op(out=Out(str, description="HTML: Кофе (pivot)"))
def render_coffee_table_op(context, coffee: List[Dict[str, Any]]) -> str:
    return _pivot_row_value(coffee, paint="asc")


@op(out=Out(str, description="HTML: Обед (pivot)"))
def render_lunch_table_op(context, dinner: List[Dict[str, Any]]) -> str:
    return _pivot_row_value(dinner, paint="asc")


@op(out=Out(str, description="HTML: Не беспокоить (pivot)"))
def render_dnd_table_op(context, dnd: List[Dict[str, Any]]) -> str:
    return _pivot_row_value(dnd, paint="asc")


@op(out=Out(str, description="HTML: Общее время в системе (pivot, desc)"))
def render_work_time_table_op(context, work_time: List[Dict[str, Any]]) -> str:
    return _pivot_row_value(work_time, paint="desc")


@op(out=Out(str, description="HTML: Общее время на заявки (pivot)"))
def render_request_time_table_op(context, request: List[Dict[str, Any]]) -> str:
    return _pivot_row_value(request, paint="asc")


@op(out=Out(str, description="HTML: Общее время по всем статусам (pivot)"))
def render_summary_request_table_op(context, summary_request: List[Dict[str, Any]]) -> str:
    return _pivot_row_value(summary_request, paint="asc")


# ────────────────────────── EFFECTIVENESS ──────────────────────────
@op(out=Out(str, description="HTML: Загруженность (pivot)"))
def render_occupancy_table_op(context, occupancy: List[Dict[str, Any]]) -> str:
    return _pivot_row_value(occupancy, paint="asc")


@op(out=Out(str, description="HTML: Разбивка звонков по длительности (pivot)"))
def render_calls_duration_table_op(context, calls_duration: List[Dict[str, Any]]) -> str:
    return _pivot_row_value(calls_duration, paint="asc")


@op(out=Out(str, description="HTML: Персональный AHT (pivot)"))
def render_personal_aht_table_op(context, personal_aht: List[Dict[str, Any]]) -> str:
    return _pivot_row_value(personal_aht, paint="asc")


# ────────────────────────── PDF assembler ──────────────────────────
@op(required_resource_keys={"report_utils"})
def report_pdf_personal_gtm(
    context,
    # SALES
    tbl_sales_personal: str,
    img_sales_dynamic_svg_b64: str,
    tbl_personal_activation: str,
    # STATUSES
    tbl_coffee: str,
    tbl_lunch: str,
    tbl_dnd: str,
    tbl_work_time: str,
    tbl_request_time: str,
    tbl_summary_request: str,
    # EFFECTIVENESS
    tbl_occupancy: str,
    tbl_calls_duration: str,
    tbl_personal_aht: str,
) -> str:
    report = MailReport()
    report.image_mode = "file"

    # Логотип как в твоём примере
    APP_ROOT = Path(__file__).resolve().parents[2]
    report.logo_file_override = str(APP_ROOT / "utils" / "logo_png" / "logo.png")

    # Дата в заголовке — отчётный день из ресурсов
    dates = context.resources.report_utils.get_utils()
    try:
        report_day = datetime.strptime(dates.ReportDate, "%Y-%m-%d")
    except Exception:
        report_day = datetime.now()

    # Страница 1
    personal_gtm_p = ""
    personal_gtm_p += report.set_basic_header(f"ЕЖЕДНЕВНАЯ ПЕРСОНАЛЬНАЯ СТАТИСТИКА ГТМ ЗА {report_day.strftime('%d.%m.%Y')}")

    # ── Блок «Статистика продаж»
    personal_gtm_p += report.add_block_header("Статистика продаж")
    personal_gtm_p += report.add_module_header("Персональная статистика продаж")
    personal_gtm_p += report.add_content_to_statistic(tbl_sales_personal)

    personal_gtm_p += report.add_module_header("Динамика продаж за последние 30 дней")
    personal_gtm_p += report.add_figure(img_sales_dynamic_svg_b64)

    personal_gtm_p += report.add_module_header("Динамика активаций в %")
    personal_gtm_p += report.add_content_to_statistic(tbl_personal_activation)

    # ── Блок «Статусы»
    personal_gtm_p += report.add_block_header("Статусы")
    personal_gtm_p += report.add_module_header("Кофе")
    personal_gtm_p += report.add_content_to_statistic(tbl_coffee)

    personal_gtm_p += report.add_module_header("Обед")
    personal_gtm_p += report.add_content_to_statistic(tbl_lunch)

    personal_gtm_p += report.add_module_header("Не беспокоить")
    personal_gtm_p += report.add_content_to_statistic(tbl_dnd)

    personal_gtm_p += report.add_module_header("Общее время в системе")
    personal_gtm_p += report.add_content_to_statistic(tbl_work_time)

    personal_gtm_p += report.add_module_header("Общее время, затраченное на заявки")
    personal_gtm_p += report.add_content_to_statistic(tbl_request_time)

    personal_gtm_p += report.add_module_header("Общее время по всем статусам")
    personal_gtm_p += report.add_content_to_statistic(tbl_summary_request)

    # ── Блок «Показатели эффективности»
    personal_gtm_p += report.add_block_header("Показатели эффективности")
    personal_gtm_p += report.add_module_header("Загруженность")
    personal_gtm_p += report.add_content_to_statistic(tbl_occupancy)

    personal_gtm_p += report.add_module_header("Разбивка звонков по длительности")
    personal_gtm_p += report.add_content_to_statistic(tbl_calls_duration)

    personal_gtm_p += report.add_module_header("Среднее время разговора — AHT")
    personal_gtm_p += report.add_content_to_statistic(tbl_personal_aht)

    personal_gtm_tmp_pdf = "personal_gtm_page1.pdf"
    html_to_pdf_page(html=personal_gtm_p, output_path=personal_gtm_tmp_pdf)

    personal_gtm_final_pdf = "personal_gtm_report.pdf"
    merge_pdfs([personal_gtm_tmp_pdf], personal_gtm_final_pdf)
    personal_gtm_final_path = str(Path(personal_gtm_final_pdf).resolve())
    context.log.info(f"[personal_gtm] PDF: {personal_gtm_final_path}")
    return personal_gtm_final_path