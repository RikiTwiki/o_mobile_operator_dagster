import base64
import io
import math
import os
from collections import defaultdict
from datetime import datetime, date, timedelta, time
from io import BytesIO
from typing import Any, Dict, List, Optional, Generator

from dagster import op, In, Out, AssetMaterialization, AssetKey, Output, get_dagster_logger, MetadataValue
from matplotlib import pyplot as plt
import matplotlib.transforms as mtransforms
from matplotlib.ticker import FuncFormatter
from utils.templates.tables.table_constructor import TableConstructor
from utils.mail.mail_report import MailReport, html_to_pdf_page, merge_pdfs

from utils.templates.charts.basic_line_graphics import BasicLineGraphics
from utils.templates.charts.multi_axis_chart import MultiAxisChart
from utils.templates.tables.basic_table import BasicTable
from utils.templates.tables.pivot_table import PivotTable
from utils.transformers.mp_report import group_by_shifts, _norm_date, _fmt_num
import matplotlib.dates as mdates

from pathlib import Path

from utils.templates.charts.basic_line_graphics import color_weekend_xticklabels
from utils.transformers.mp_report import strip_seconds_from_shifts


def extract_metric_series(
    day_data: List[Dict[str, Any]],
    dates: List[str],
    date_key: str,
    metric_key: str,
) -> List[Any]:
    """
    Простейший аналог get_data_in_dates: для каждого даты из dates
    берёт соответствующее значение metric_key из day_data по совпадению даты (по дате, без времени).
    """
    mapping = {}
    for entry in day_data:
        raw_date = entry.get(date_key)
        if not raw_date:
            continue
        key = raw_date.split("T")[0]
        mapping[key] = entry.get(metric_key)

    result = []
    for dt in dates:
        key = dt.split("T")[0]
        result.append(mapping.get(key, ""))
    return result

@op
def render_aggregation_project_data_op(context,
project_data: Dict[str, Any],
self_service_rate: Optional[List[Dict[str, Any]]] = None,
month_self_service_rate: Optional[Any] = None,
days_count: int = 8,
target = 60,
bot_self_service_rate = 70
):
    """
     Текстовые: 1.1, 1.2, 1.3, 1.4
     """

    # 1) Подготовка списка записей по дням
    day_data = project_data.get("dayData", [])
    _ssr_input_is_none = (self_service_rate is None and month_self_service_rate is None)
    self_service_rate = self_service_rate or []
    month_self_service_rate = month_self_service_rate or []
    last_days = sorted(day_data, key=lambda x: x["date"])[-days_count:]
    self_service_rate = sorted(self_service_rate, key=lambda x: x["date"])[-days_count:]
    dates_dt = [datetime.strptime(rec["date"].split(" ")[0], "%Y-%m-%d") for rec in last_days]
    weekend_mask = [dt.weekday() >= 5 for dt in dates_dt]

    # 2) Форматированные даты для заголовков
    formatted_dates = [
        datetime.strptime(rec["date"].split(" ")[0], "%Y-%m-%d").strftime("%d.%m")
        for rec in last_days
    ]

    # 3) Средние за месяц
    month_rt = project_data.get("monthAverageReactionTime", 0)
    month_sta = project_data.get("monthAverageSpeedToAnswer", 0)

    # 4) Готовим табличный конструктор
    table = TableConstructor()

    # ── 1-я строка шапки ─────────────────────────────────────────────
    table.genCellTH("Показатель", style="vertical-align: middle;", props={"rowspan": "2"})
    table.genCellTH("План", style="vertical-align: middle;", props={"rowspan": "2"})

    # ── 1-я строка шапки: дни недели под каждой датой ───────────────
    wd = ['пн', 'вт', 'ср', 'чт', 'пт', 'сб', 'вс']
    for i, dt in enumerate(dates_dt):
        is_weekend = weekend_mask[i]
        th_style = "background-color:#757E85; color:white;" if is_weekend else None
        table.genCellTH(wd[dt.weekday()], style=th_style)

    table.genCellTH(
        "Среднее значение за текущий месяц",
        style="word-wrap: break-word; width: 160px; vertical-align: middle;",
        props={"rowspan": "2"}
    )
    table.genRow()

    # 2-я строка шапки
    table.multiGenCellTH(formatted_dates)
    table.genRow()

    # — Average Reaction Time —
    rt_vals = [_fmt_num(rec.get("average_reaction_time", 0)) for rec in last_days]
    table.genCellTD("Average Reaction Time", "font-weight: bold;")
    table.genCellTD(f"≤ {target} сек.")
    table.multiGenCellTD(
        rt_vals,
        None,
        {"plan": target, "operator": ">", "style": "background: #FF4D6E;"}
    )
    table.genCellTD(_fmt_num(month_rt), "font-weight: bold;")
    table.genRow()

    # — Average Speed To Answer —
    sta_vals = [_fmt_num(rec.get("average_speed_to_answer", 0)) for rec in last_days]
    table.genCellTD("Average Speed To Answer", "font-weight: bold;")
    table.genCellTD(f"≤ {target} сек.")
    table.multiGenCellTD(
        sta_vals,
        None,
        {"plan": target, "operator": ">", "style": "background: #FF4D6E;"}
    )
    table.genCellTD(_fmt_num(month_sta), "font-weight: bold;")
    table.genRow()

    # — Bot Self Service Rate —
    if not _ssr_input_is_none:
        ssr_daily = [_fmt_num(rec.get("self_service_rate", 0)) for rec in self_service_rate]
        table.genCellTD("Bot Self Service Rate (чат-бот)", "font-weight: bold;")
        table.genCellTD(f"≥ {bot_self_service_rate}%")
        table.multiGenCellTD(
            ssr_daily,
            None,
            {"plan": bot_self_service_rate, "operator": "<", "style": "background: #FF4D6E;"}
        )

        if isinstance(month_self_service_rate, list):
            if month_self_service_rate:
                m = month_self_service_rate[0].get("self_service_rate", 0)
            else:
                m = 0
        else:
            m = month_self_service_rate or 0

        table.genCellTD(_fmt_num(m), "font-weight: bold;")
        table.genRow()
    return table.renderTable()

@op
def render_Hour_Jivo_Data_Aggregated(hour_MPData_Messengers_Detailed):
    items = sorted(hour_MPData_Messengers_Detailed, key=lambda x: datetime.strptime(x['date'], '%H:%M:%S'))

    table = BasicTable()
    table.Items = items
    table.Fields = [
      {'field': 'date', 'title': 'Часы'},
      {'field': 'total_chats', 'title': 'Поступило \n обращений', 'summary': 'sum', 'paint': False},
      {'field': 'total_handled_chats', 'title': 'Обработано \n обращений', 'summary': 'sum', 'paint': False},
      {'field': 'total_by_operator', 'title': 'Закрыто без \n задержки \n (операторами)', 'summary': 'sum', 'paint': False},
      {'field': 'bot_handled_chats', 'title': 'Закрыто \n с задержкой \n 12 часов (чат-ботом)', 'summary': 'sum', 'paint': False},
      {'field': 'average_reaction_time', 'title': 'Average \n Reaction Time', 'paint': True, 'summary': 'sum', 'round': 1, 'paint_type': 'desc'},
      {'field': 'average_speed_to_answer', 'title': 'Average \n Speed To Answer', 'paint': True, 'summary': 'sum', 'round': 1, 'paint_type': 'desc'},
      {'field': 'user_total_reaction_time', 'title': 'Chat Total \n Reaction Time', 'paint': True, 'summary': 'sum', 'round': 1, 'paint_type': 'desc', 'int_if_whole': True},
      {'field': 'user_count_replies', 'title': 'Chat Count \n Replies', 'paint': True, 'summary': 'sum', 'round': 1, 'paint_type': 'asc', 'int_if_whole': True},
     ]


    table.Items = hour_MPData_Messengers_Detailed
    return table.getTable()

@op
def render_Mixed_Graph(items: Dict[str, Any]) -> str:
    """
    2.1.1  2.1.2  2.1.3  2.1.4
    """
    pointer = 1
    day_data_items = items.get('dayData', [])

    for item in day_data_items:
        item['date_dt'] = datetime.strptime(item['date'], '%Y-%m-%d %H:%M:%S')
    day_data_items.sort(key=lambda x: x['date_dt'])
    for item in day_data_items:
        del item['date_dt']

    # Вычисляем максимальное значение
    max_val = 100
    for item in day_data_items:
        item['bot_handled_chats'] = item.get('bot_handled_chats', 0)
        if item.get('total_chats', 0) > max_val:
            max_val = item['total_chats']

    max_val += 3000

    # Параметры можно менять здесь
    chart = MultiAxisChart(
        graph_title="Данный график показывает кол-во обработанных обращений.",
        x_axes_name="Дата",
        y_axes_name="Кол-во",
        stack=True,
        show_tooltips=False,
        height=400,
        pointer=pointer,
        y_axes_min=0,
        y_axes_max=max_val,
        fields=[
            {"field": "date", "title": "", "type": "label"},
            {"field": "total", "title": "Кол-во чатов обработанных специалистами", "chartType": "bar",
             "round": 1},
            {"field": "bot_handled_chats", "title": "Кол-во чатов обработанных чат-ботом", "chartType": "bar",
             "round": 1},
            {"field": "total_chats", "title": "Кол-во поступивших чатов", "chartType": "line", "round": 1},
        ],
        items=day_data_items  # Pass the corrected list of items
    )
    fig, ax = plt.subplots(
        figsize=(chart.width / chart.dpi, chart.height / chart.dpi),
        dpi=chart.dpi
    )
    chart.render_graphics(ax)

    buf = io.BytesIO()
    fig.savefig(buf, format="svg", bbox_inches="tight")
    buf.seek(0)
    b64 = base64.b64encode(buf.read()).decode("utf-8")
    return b64

@op
def render_jivo_total_31_days_detailed(items: List[Dict[str, Any]]) -> str:
    """
    Сводка по проектам 2.1.5
    """
    items = _norm_date(items)

    get_dagster_logger().info(f"items {items}")
    pivot = PivotTable()
    pivot.RoundValue = 1
    pivot.RowTitleStyle = "font-weight: bold; text-align: left"
    pivot.PaintType = 'asc'
    pivot.OrderByTotal = 'desc'
    pivot.Items = items
    pivot.TitleStyle = (pivot.TitleStyle or "") + " width: 150px; min-width: 150px;"
    pivot.RowTitleStyle = (
        "font-weight: bold; text-align: left; "
        "width: 150px; min-width: 150px; "
        "white-space: normal; "  # разрешаем перенос строк
        "word-break: break-word; "  # переносим длинные слова
        "overflow-wrap: anywhere;"  # если нет пробелов — всё равно переносим
    )
    pivot.Fields = [
        {'field': 'date', 'type': 'period', 'format': '%d.%m'},
        {'field': 'project', 'type': 'row'},
        {'field': 'total', 'type': 'value'}
    ]
    return pivot.getTable()


@op
def render_jivo_total_31_days_users_detailed(
    items: List[Dict[str, Any]]
):
    """
    Сводка по специалистам 2.1.6
    """
    items = _norm_date(items)

    get_dagster_logger().info(f"items {items}")
    pivot = PivotTable()
    pivot.RoundValue         = 1
    pivot.RowTitleStyle      = "font-weight: bold; text-align: left"
    pivot.PaintType          = "desc"
    pivot.OrderByTotal       = "desc"
    pivot.Items              = items
    pivot.TitleStyle = (pivot.TitleStyle or "") + " width: 150px; min-width: 150px;"
    pivot.RowTitleStyle = (
        "font-weight: bold; text-align: left; "
        "width: 150px; min-width: 150px; "
        "white-space: normal; "  # разрешаем перенос строк
        "word-break: break-word; "  # переносим длинные слова
        "overflow-wrap: anywhere;"  # если нет пробелов — всё равно переносим
    )
    pivot.Fields = [
        {"field": "date",  "type": "period", "format": "%d.%m"},
        {"field": "title", "type": "row"},
        {"field": "total", "type": "value"},
    ]
    pivot.DescriptionColumns = [
        {
            "field": "abbreviation",
            "title": "Отдел",
            "style": "width: 100px; word-wrap: break-all;",
        },
    ]
    return pivot.getTable()

@op(required_resource_keys={"report_utils"},)
def render_total_31_days_users_detailed_with_shifts(context,
    items: List[Dict[str, Any]]
) -> str:
    """
    Сводка по специалистам 2.1.7
    """
    def to_datetime(v):
        if isinstance(v, datetime):
            return v
        if isinstance(v, date):
            return datetime.combine(v, time.min)
        if isinstance(v, str):
            try:
                return datetime.fromisoformat(v)
            except ValueError:
                for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%d.%m.%Y", "%d.%m.%Y %H:%M:%S"):
                    try:
                        return datetime.strptime(v, fmt)
                    except ValueError:
                        pass
                raise ValueError(f"Unsupported EndDate format: {v!r}")
        raise TypeError(f"Unsupported EndDate type: {type(v)}")

    dates = context.resources.report_utils.get_utils()
    context.log.info(f"EndDate: {dates.EndDate} (type={type(dates.EndDate)})")

    end_dt = to_datetime(dates.EndDate)
    max_date = (end_dt - timedelta(days=2)).date().isoformat()
    context.log.info(f"max_date: {max_date}")

    items = _norm_date(items)

    get_dagster_logger().info(f"items {items}")
    grouped_items = group_by_shifts(items)
    grouped_items = strip_seconds_from_shifts(grouped_items)  # по умолчанию чистит любые секунды

    pivot = PivotTable()
    pivot.RoundValue         = 0
    pivot.RowTitleStyle      = "font-weight: bold; text-align: left"
    pivot.PaintType          = "desc"
    pivot.OrderByTotal       = "desc"
    pivot.Items              = grouped_items
    pivot.maxDate = max_date
    pivot.TitleStyle = (pivot.TitleStyle or "") + " width: 150px; min-width: 150px;"
    pivot.RowTitleStyle = (
        "font-weight: bold; text-align: left; "
        "width: 150px; min-width: 150px; "
        "white-space: normal; "  # разрешаем перенос строк
        "word-break: break-word; "  # переносим длинные слова
        "overflow-wrap: anywhere;"  # если нет пробелов — всё равно переносим
    )
    pivot.Fields = [
        {"field": "date",  "type": "period", "format": "%d.%m"},
        {"field": "title", "type": "row"},
        {"field": "total", "type": "value"},
    ]
    pivot.DescriptionColumns = [{
        "field": "shifts",
        "title": "Смена",
        "style": (
            "max-width: 220px; "
            "white-space: nowrap; "  # всё в одну строку
            "overflow: hidden; "  # если не помещается — обрезаем
            "text-overflow: ellipsis;"  # и показываем «…»
        ),
    }]

    return pivot.getTable()

@op
def render_styled_reaction_time_graph(context, items:  Dict[str, Any], target = 60):
    """
    Показатель Average Reaction Time: 2.2.1, 2.2.2, 2.2.3, 2.2.4
    """
    items = items.get('dayData', [])

    graphic = BasicLineGraphics(
        graph_title="Данный график показывает среднее время ответа на первое сообщение.",
        x_axes_name="Дни",
        y_axes_name="Секунды",
        target=target ,
        stroke='smooth',
        show_tooltips=True,
        y_axes_min=0,
        items=items,
        fields=[
            {'field': 'date', 'title': 'Дни', 'type': 'label'},
            {
                'field': 'average_speed_to_answer',
                'title': 'Среднее время ответа реакции на первое сообщение',
                'round': 1,
            },
        ],
        width=1920,
        height=400,
    )

    graphic.make_preparation()

    dpi = 100
    fig, ax = plt.subplots(figsize=(graphic.width / dpi, graphic.height / dpi), dpi=dpi)
    fig.patch.set_facecolor('#f8f9fa')   # Цвет фона всей фигуры
    ax.set_facecolor('#ffffff')          # Цвет фона графика

    # Шаг 4: Построение линии графика
    def _parse_item_dt(v):
        s = str(v).strip()
        try:
            return datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            return datetime.strptime(s, '%Y-%m-%d')

    dates = [_parse_item_dt(it['date']) for it in graphic.items]
    for ds in graphic.datasets:
        y = ds['data']
        ax.plot(dates, y, color='black', linewidth=2.5, label=ds['name'])  # Основная линия

        # Добавление точек и аннотаций на линии
        for date, value in zip(dates, y):
            if value is not None:
                ax.plot(date, value, marker='o', markersize=6, markerfacecolor='#2c5282',
                        markeredgecolor='white', markeredgewidth=1)
                ax.annotate(f'{value:.1f}', (date, value),
                            textcoords="offset points", xytext=(0, 0),
                            ha='center', fontsize=8, fontweight='bold',
                            bbox=dict(boxstyle="round,pad=0.3", facecolor='black',
                                      edgecolor='none', alpha=0.8),
                            color='white')

    # Шаг 5: Добавление зелёной горизонтальной линии — "цели"
    ax.set_xmargin(0)
    ax.set_xlim(min(dates), max(dates))

    # линия цели по всей ширине
    ax.axhline(y=graphic.target, color='#38a169', linestyle='-', linewidth=2, alpha=0.8, zorder=2)

    # подпись «План» у самого левого края оси X, Y — в данных
    trans = mtransforms.blended_transform_factory(ax.transAxes, ax.transData)
    ax.text(0, graphic.target, 'План',  # 0.01 — чуть правее края; можно 0.005
            transform=trans, ha='left', va='center',
            bbox=dict(boxstyle="round,pad=0.3", facecolor='#38a169', edgecolor='none'),
            color='white', fontweight='bold', fontsize=8,
            clip_on=False, zorder=3)

    # Шаг 6: Горизонтальные полосы + динамические границы оси Y (без нуля)
    all_values = [val for ds in graphic.datasets for val in ds['data'] if val is not None]
    if all_values:
        data_min = min(all_values)
        data_max = max(all_values)

        # включим цель, чтобы точно была в пределах
        if graphic.target is not None:
            data_min = min(data_min, graphic.target)
            data_max = max(data_max, graphic.target)

        span = data_max - data_min
        if span <= 0:
            span = max(1.0, abs(data_max) * 0.1)

        # хотим ~8–12 полос → возьмём "красивый" шаг
        desired_bands = 10
        raw_step = span / desired_bands
        exp = math.floor(math.log10(raw_step)) if raw_step > 0 else 0
        frac = raw_step / (10 ** exp)
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

        # небольшой запас сверху/снизу (не меньше шага)
        padding = max(step, span * 0.15)

        # округляем границы к кратным step
        y0 = math.floor((data_min - padding) / step) * step
        y1 = math.ceil((data_max + padding) / step) * step
        if graphic.target is not None and y1 <= graphic.target:
            y1 = (math.floor(graphic.target / step) + 1) * step
        ax.set_ylim(y0, y1)

        # заливаем каждую вторую полосу
        i = 0
        y = y0
        while y < y1:
            if i % 2 == 1:
                ax.axhspan(y, y + step, alpha=0.05, color='gray', zorder=0)
            y += step
            i += 1


    # Шаг 7: Настройка осей и сетки
    ax.grid(True, linestyle='-', linewidth=0.5, color='#e2e8f0', alpha=0.7, axis='y')
    ax.set_axisbelow(True)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%d.%m'))  # Формат дат на оси X
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    ax.xaxis.set_major_formatter(
        FuncFormatter(lambda x, pos: mdates.num2date(x).strftime('%d.%m'))
    )
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=0, fontsize=9)

    # >>> ПОДСВЕТКА ТОЛЬКО МЕТОК ВЫХОДНЫХ <<<
    color_weekend_xticklabels(ax)

    # Шаг 8: Подписи осей и стилизация рамок графика
    ax.set_xlabel(graphic.x_axes_name, fontsize=10, color='#4a5568')
    ax.set_ylabel(graphic.y_axes_name, fontsize=10, color='#4a5568')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_color('#e2e8f0')
    ax.spines['bottom'].set_color('#e2e8f0')
    ax.tick_params(axis='x', colors='#4a5568', labelsize=7)
    ax.tick_params(axis='y', colors='#4a5568', labelsize=10)

    # Шаг 9: Добавление заголовка графика
    ax.text(0.5, 1.08, graphic.graph_title,
            horizontalalignment='center',
            transform=ax.transAxes,
            fontsize=11,
            color='#2d3748',
            wrap=True)
    buf = io.BytesIO()
    fig.savefig(buf, format="svg", bbox_inches="tight")
    buf.seek(0)
    b64 = base64.b64encode(buf.read()).decode("utf-8")
    return b64

@op
def render_jivo_avg_reaction_time_31_days_detailed(
    items: List[Dict[str, Any]]
):
    """
    Сводка по проектам 2.2.5
    """
    items = _norm_date(items)
    pivot = PivotTable()
    pivot.RoundValue = 1
    pivot.RowTitleStyle = "font-weight: bold; text-align: left"
    pivot.PaintType = "asc"
    pivot.ShowSummaryXAxis           = False
    pivot.ShowSummaryYAxis           = False
    pivot.AdvancedAverageMultiplication = 1
    pivot.Items                      = items
    pivot.TitleStyle = (pivot.TitleStyle or "") + " width: 150px; min-width: 150px;"
    pivot.RowTitleStyle = (
        "font-weight: bold; text-align: left; "
        "width: 150px; min-width: 150px; "
        "white-space: normal; "  # разрешаем перенос строк
        "word-break: break-word; "  # переносим длинные слова
        "overflow-wrap: anywhere;"  # если нет пробелов — всё равно переносим
    )
    pivot.Fields = [
        {"field": "date",                   "type": "period", "format": "%d.%m"},
        {"field": "project",                "type": "row"},
        {
            "field": "average_reaction_time",
            "type": "value",
            "advancedAverage": {
                "numerator": "user_reaction_time",
                "denominator": "total",
                "order": "desc",
            },
        },
    ]
    return pivot.getTable()

@op
def render_jivo_avg_reaction_time_31_days_users_detailed(
    items: List[Dict[str, Any]]
):
    """
    Сводка по специалистам 2.2.6
    """
    items = _norm_date(items)
    pivot = PivotTable()
    pivot.RoundValue                   = 1
    pivot.RowTitleStyle                = "font-weight: bold; text-align: left"
    pivot.PaintType                    = "asc"
    pivot.ShowSummaryXAxis             = False
    pivot.ShowSummaryYAxis             = False
    pivot.AdvancedAverageMultiplication = 1
    pivot.Items                        = items
    pivot.TitleStyle = (pivot.TitleStyle or "") + " width: 150px; min-width: 150px;"
    pivot.RowTitleStyle = (
        "font-weight: bold; text-align: left; "
        "width: 150px; min-width: 150px; "
        "white-space: normal; "  # разрешаем перенос строк
        "word-break: break-word; "  # переносим длинные слова
        "overflow-wrap: anywhere;"  # если нет пробелов — всё равно переносим
    )
    pivot.Fields = [
        {"field": "date",                   "type": "period", "format": "%d.%m"},
        {"field": "title",                  "type": "row"},
        {
            "field": "average_reaction_time",
            "type": "value",
            "advancedAverage": {
                "numerator": "user_reaction_time",
                "denominator": "total",
                "order": "desc",
            },
        },
    ]
    pivot.DescriptionColumns = [
        {
            "field": "abbreviation",
            "title": "Отдел",
            "style": "width: 100px; word-wrap: break-all;",
        },
    ]
    return pivot.getTable()

@op
def render_styled_speed_answer_time_graph(context, items:  Dict[str, Any], target=60):
    """
    Показатель Average Speed To answer: 2.3.1, 2.3.2, 2.3.3, 2.3.4
    """
    items = items.get("dayData", [])
    graphic = BasicLineGraphics(
        graph_title="Данный график показывает среднее время ответа на первое сообщение.",
        x_axes_name="Дни",
        y_axes_name="Секунды",
        target=target,
        stroke='smooth',
        show_tooltips=True,
        y_axes_min=0,
        items=items,
        fields=[
            {'field': 'date', 'title': 'Дни', 'type': 'label'},
            {
                'field': 'average_speed_to_answer',
                'title': 'Среднее время ответа реакции на первое сообщение',
                'round': 1,
            },
        ],
        width=1920,
        height=400,
    )

    graphic.make_preparation()

    dpi = 100
    fig, ax = plt.subplots(figsize=(graphic.width / dpi, graphic.height / dpi), dpi=dpi)
    fig.patch.set_facecolor('#f8f9fa')  # Цвет фона всей фигуры
    ax.set_facecolor('#ffffff')  # Цвет фона графика

    # Шаг 4: Построение линии графика
    def _parse_item_dt(v):
        s = str(v).strip()
        try:
            return datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            return datetime.strptime(s, '%Y-%m-%d')

    dates = [_parse_item_dt(it['date']) for it in graphic.items]
    for ds in graphic.datasets:
        y = ds['data']
        ax.plot(dates, y, color='#e52990', linewidth=2.5, label=ds['name'])  # Основная линия

        # Добавление точек и аннотаций на линии
        for date, value in zip(dates, y):
            if value is not None:
                ax.plot(date, value, marker='o', markersize=6, markerfacecolor='#2c5282',
                        markeredgecolor='white', markeredgewidth=1)
                ax.annotate(f'{value:.1f}', (date, value),
                            textcoords="offset points", xytext=(0, 0),
                            ha='center', fontsize=8, fontweight='bold',
                            bbox=dict(boxstyle="round,pad=0.3", facecolor='#e52990',
                                      edgecolor='none', alpha=0.8),
                            color='white')

    # Добавление зелёной горизонтальной линии — "цели"
    ax.set_xmargin(0)
    ax.set_xlim(min(dates), max(dates))

    # линия цели по всей ширине
    ax.axhline(y=graphic.target, color='#38a169', linestyle='-', linewidth=2, alpha=0.8, zorder=2)

    # подпись «План» у самого левого края оси X, Y — в данных
    trans = mtransforms.blended_transform_factory(ax.transAxes, ax.transData)
    ax.text(0, graphic.target, 'План',  # 0.01 — чуть правее края; можно 0.005
            transform=trans, ha='left', va='center',
            bbox=dict(boxstyle="round,pad=0.3", facecolor='#38a169', edgecolor='none'),
            color='white', fontweight='bold', fontsize=8,
            clip_on=False, zorder=3)

    # Добавление горизонтальных цветных полос (для визуального разделения уровней)
    all_values = [val for ds in graphic.datasets for val in ds['data'] if val is not None]
    if all_values:
        min_val = 0
        max_val = max(all_values)
        if graphic.target is not None:
            max_val = max(max_val, graphic.target)

        padding = (max_val - min_val) * 0.15
        padding = max(padding, 6)

        y_top = max_val + padding

        # NEW: если даже с padding верх не выше плана — поднимем ещё
        if graphic.target is not None and y_top <= graphic.target:
            y_top = graphic.target + padding

        ax.set_ylim(min_val, y_top)

        range_val = y_top - min_val
        band_height = max(6, range_val * 0.1)
        band_height = int(band_height)

        start_band = int(min_val // band_height) * band_height
        end_band = int(y_top // band_height + 1) * band_height

        for i in range(start_band, end_band, band_height):
            if (i // band_height) % 2 == 1:
                ax.axhspan(i, i + band_height, alpha=0.05, color='gray', zorder=0)

    # Настройка осей и сетки
    ax.grid(True, linestyle='-', linewidth=0.5, color='#e2e8f0', alpha=0.7, axis='y')
    ax.set_axisbelow(True)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%d.%m'))  # Формат дат на оси X
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    ax.xaxis.set_major_formatter(
        FuncFormatter(lambda x, pos: mdates.num2date(x).strftime('%d.%m'))
    )
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=0, fontsize=9)

    # >>> ПОДСВЕТКА ТОЛЬКО МЕТОК ВЫХОДНЫХ <<<
    color_weekend_xticklabels(ax)

    # Подписи осей и стилизация рамок графика
    ax.set_xlabel(graphic.x_axes_name, fontsize=10, color='#4a5568')
    ax.set_ylabel(graphic.y_axes_name, fontsize=10, color='#4a5568')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_color('#e2e8f0')
    ax.spines['bottom'].set_color('#e2e8f0')
    ax.tick_params(axis='x', labelsize=7, colors='#4a5568')
    ax.tick_params(axis='y', labelsize=10, colors='#4a5568')


    # Добавление заголовка графика
    ax.text(0.5, 1.08, graphic.graph_title,
            horizontalalignment='center',
            transform=ax.transAxes,
            fontsize=11,
            color='#2d3748',
            wrap=True)
    buf = io.BytesIO()
    fig.savefig(buf, format="svg", bbox_inches="tight")
    buf.seek(0)
    b64 = base64.b64encode(buf.read()).decode("utf-8")
    return b64


@op
def render_jivo_avg_speed_to_answer_31_days_detailed(
    items: List[Dict[str, Any]]
):
    """
    Сводка по специалистам 2.3.5
    """
    items = _norm_date(items)
    pivot = PivotTable()
    pivot.RoundValue                   = 1
    pivot.RowTitleStyle                = "font-weight: bold; text-align: left"
    pivot.PaintType                    = "asc"
    pivot.ShowSummaryXAxis             = False
    pivot.ShowSummaryYAxis             = False
    pivot.AdvancedAverageMultiplication = 1
    pivot.Items                        = items
    pivot.TitleStyle = (pivot.TitleStyle or "") + " width: 150px; min-width: 150px;"
    pivot.RowTitleStyle = (
        "font-weight: bold; text-align: left; "
        "width: 150px; min-width: 150px; "
        "white-space: normal; "  # разрешаем перенос строк
        "word-break: break-word; "  # переносим длинные слова
        "overflow-wrap: anywhere;"  # если нет пробелов — всё равно переносим
    )
    pivot.Fields = [
        {"field": "date",                     "type": "period", "format": "%d.%m"},
        {"field": "project",                  "type": "row"},
        {
            "field": "average_speed_to_answer",
            "type": "value",
            "advancedAverage": {
                "numerator": "user_total_reaction_time",
                "denominator": "user_count_replies",
                "order": "desc",
            },
        },
    ]
    return pivot.getTable()

@op
def render_jivo_avg_speed_answer_time_31_days_users_detailed(
    items: List[Dict[str, Any]]
) -> str:
    """
    Сводка по специалистам 2.3.6
    """
    items = _norm_date(items)
    pivot = PivotTable()
    pivot.RoundValue                   = 1
    pivot.RowTitleStyle                = "font-weight: bold; text-align: left"
    pivot.PaintType                    = "asc"
    pivot.ShowSummaryXAxis             = False
    pivot.ShowSummaryYAxis             = False
    pivot.AdvancedAverageMultiplication = 1
    pivot.Items                        = items
    pivot.TitleStyle = (pivot.TitleStyle or "") + " width: 150px; min-width: 150px;"
    pivot.RowTitleStyle = (
        "font-weight: bold; text-align: left; "
        "width: 150px; min-width: 150px; "
        "white-space: normal; "  # разрешаем перенос строк
        "word-break: break-word; "  # переносим длинные слова
        "overflow-wrap: anywhere;"  # если нет пробелов — всё равно переносим
    )
    pivot.Fields = [
        {"field": "date",                     "type": "period", "format": "%d.%m"},
        {"field": "title",                    "type": "row"},
        {
            "field": "average_speed_to_answer",
            "type": "value",
            "advancedAverage": {
                "numerator": "user_total_reaction_time",
                "denominator": "user_count_replies",
                "order": "desc",
            },
        },
    ]
    pivot.DescriptionColumns = [
        {
            "field": "abbreviation",
            "title": "Отдел",
            "style": "width: 100px; word-wrap: break-all;",
        }
    ]
    return pivot.getTable()


@op(required_resource_keys={"source_dwh", "report_utils", "mp_daily_config"},)
def statuses_op(context, data):
    dates = context.resources.report_utils.get_utils()

    def _to_ymd(x):
        if isinstance(x, (date, datetime)):
            return x.strftime("%Y-%m-%d")
        return str(x)[:10] if x is not None else None

    # В начале statuses_op, до цикла генерации таблиц:
    for code, rows in data.items():
        for r in rows:
            r["date"] = _to_ymd(r.get("date"))

    min_date = _to_ymd(dates.StartDate)
    max_date = _to_ymd(dates.ReportDate)
    STATUSES = {
        'absent': 'Отсутствует',
        'lunch': 'Обед',
        'dnd': 'Блокировка по инициативе супервайзера',
        'normal': 'Normal',
        'social_media': 'Social-Media',
        'eshop': 'eshop',
        'coffee': 'Кофе',
        'pre_coffee': 'Pre-Coffee',
    }

    tables = {}
    for status, status_label in STATUSES.items():
        table = PivotTable()
        table.RoundValue = 1
        if status == "coffee" or status == "pre_coffee":
            table.coffee = True
        table.Items = data.get(status, [])
        table.minDate = min_date
        table.maxDate = max_date
        table.ShowSummaryYAxis = True
        table.PaintType = 'asc'
        table.OrderByTotal = 'desc'
        table.TitleStyle = 'width: 250px;'
        table.Fields = [
            {
                'field': 'date',
                'type': 'period',
                'format': '%d.%m',
                'title': 'Логин',
            },
            {'field': 'login',   'type': 'row'},
            {'field': 'minutes', 'type': 'value'},
        ]
        table.DescriptionColumns = [
            {
                'field': 'abbreviation',
                'title': 'Отдел',
                'style': 'width: 100px; word-wrap: break-all;',
            },
        ]
        to_table = table.getTable()
        tables[status_label] = to_table
        context.log.info(f"{status_label} - {data.get(status, [])}")

    return tables


@op(required_resource_keys={"source_dwh", "report_utils", "mp_daily_config"},
    )
def create_html(context, text_fig1, text_fig2, text_fig3, text_fig4, text_fig5, text_fig6, text_fig7, text_fig8, text_fig9, text_fig10, text_fig11, text_fig12, text_fig13, text_fig14, text_fig15, text_fig16, text_fig17, text_fig18, text_fig19, text_fig20, text_fig21, text_fig22, text_fig23, text_fig24, tables,
                month_average_reaction_time, month_average_speed):
    report = MailReport()
    report.image_mode = "file"

    APP_ROOT = Path(__file__).resolve().parents[2]

    LOGO_PATH = APP_ROOT / "utils" / "logo_png" / "logo.png"

    report.logo_file_override = str(LOGO_PATH)
    dates = context.resources.report_utils.get_utils()
    report_date = datetime.strptime(dates.ReportDate, "%Y-%m-%d")
    text_p1 = ""
    text_p1 += report.set_basic_header(f"ЕЖЕДНЕВНАЯ СТАТИСТИКА ПО ТЕКСТОВЫМ КАНАЛАМ НА {report_date.strftime('%d.%m.%Y')}")
    text_p1 += report.add_block_header("Сводка по основным показателям")
    text_p1 += report.add_module_header("Динамика за последние 7 дней и предварительные данные за месяц по всем проектам.")
    text_p1 += report.add_content_to_statistic(text_fig1)
    text_p1 += report.add_module_header("Динамика за последние 7 дней и предварительные данные за месяц Saima.")
    text_p1 += report.add_content_to_statistic(text_fig2)
    text_p1 += report.add_module_header("Динамика за последние 7 дней и предварительные данные за месяц (Основная линия, Интернет магазин)")
    text_p1 += report.add_content_to_statistic(text_fig3)
    text_p1 += report.add_module_header("Динамика за последние 7 дней и предварительные данные за месяц (Акча Булак, О!Банк, О!Агент, O!Терминалы, O!Bank)")
    text_p1 += report.add_content_to_statistic(text_fig4)

    text_p2 = ""
    text_p2 += report.add_module_header("Динамика по часам за отчетный день")
    text_p2 += report.add_content_to_statistic(text_fig5)

    text_p3 = ""
    text_p3 += report.add_block_header("Статистика по мессенджерам")
    text_p3 += report.add_module_header("Количество обращений")
    text_p3 += report.add_module_child_header("Все проекты")
    text_p3 += report.add_figure(text_fig6)
    text_p3 += report.add_module_child_header("Saima")
    text_p3 += report.add_figure(text_fig7)
    text_p3 += report.add_module_child_header("Основная линия, Интернет магазин")
    text_p3 += report.add_figure(text_fig8)
    text_p3 += report.add_module_child_header("Акча Булак, О!Банк, О!Агент, O!Терминалы, O!Bank")
    text_p3 += report.add_figure(text_fig9)

    text_p4 = ""
    text_p4 += report.add_module_child_header("Сводка по проектам")
    text_p4 += report.add_content_to_statistic(text_fig10)
    text_p4 += report.add_module_child_header("Сводка по специалистам")
    text_p4 += report.add_content_to_statistic(text_fig11)

    text_p5 = ""
    text_p5 += report.add_module_child_header("Сводка по специалистам по сменам")
    text_p5 += report.add_content_to_statistic(text_fig12)

    text_p6 = ""
    text_p6 += report.add_module_header(f"Показатель Average Reaction Time. План ≤ 60 сек. Значение за текущий месяц {month_average_reaction_time} сек.")
    text_p6 += report.add_module_child_header("Среднее по всем проектам")
    text_p6 += report.add_figure(text_fig13)
    text_p6 += report.add_module_child_header("Среднее по Saima")
    text_p6 += report.add_figure(text_fig14)
    text_p6 += report.add_module_child_header("Среднее по др. проектам (Основная линия, Интернет магазин)")
    text_p6 += report.add_figure(text_fig15)
    text_p6 += report.add_module_child_header("Среднее по др. проектам (Акча Булак, Банк, О!Агент, O!Терминалы, O!Bank)")
    text_p6 += report.add_figure(text_fig16)

    text_p7 = ""
    text_p7 += report.add_module_child_header("Сводка по проектам")
    text_p7 += report.add_content_to_statistic(text_fig17)
    text_p7 += report.add_module_child_header("Сводка по специалистам")
    text_p7 += report.add_content_to_statistic(text_fig18)

    text_p8 = ""
    text_p8 += report.add_module_header(f"Показатель Average Speed To Answer. План ≤ 60 сек. Значение за текущий месяц {month_average_speed} сек.")
    text_p8 += report.add_module_child_header("Среднее по всем проектам")
    text_p8 += report.add_figure(text_fig19)
    text_p8 += report.add_module_child_header("Среднее по Saima")
    text_p8 += report.add_figure(text_fig20)
    text_p8 += report.add_module_child_header("Среднее по др. проектам (Основная линия, Интернет магазин)")
    text_p8 += report.add_figure(text_fig21)
    text_p8 += report.add_module_child_header("Среднее по др. проектам (Акча Булак, О!Банк, О!Агент, O!Терминалы, O!Bank)")
    text_p8 += report.add_figure(text_fig22)

    text_p9 = ""
    text_p9 += report.add_module_child_header("Сводка по проектам")
    text_p9 += report.add_content_to_statistic(text_fig23)
    text_p9 += report.add_module_child_header("Сводка по специалистам")
    text_p9 += report.add_content_to_statistic(text_fig24)

    text_p10 = ""
    text_p10 += report.add_block_header("Динамика по кол-ву времени в статусах (минуты)")
    text_p10 += report.add_module_header("Отсутствует")
    text_p10 += report.add_content_to_statistic(tables['Отсутствует'])

    text_p11 = ""
    text_p11 += report.add_module_header("Обед")
    text_p11 += report.add_content_to_statistic(tables['Обед'])
    text_p11 += report.add_module_header("Кофе")
    text_p11 += report.add_content_to_statistic(tables['Кофе'])

    text_p12 = ""
    text_p12 += report.add_module_header("Normal")
    text_p12 += report.add_content_to_statistic(tables['Normal'])

    text_p13 = ""
    text_p13 += report.add_module_header("Pre-Coffee")
    text_p13 += report.add_content_to_statistic(tables['Pre-Coffee'])
    text_p13 += report.add_module_header("Блокировка по инициативе супервайзера")
    text_p13 += report.add_content_to_statistic(tables['Блокировка по инициативе супервайзера'])

    text_p14 = ""
    text_p14 += report.add_module_header("Social-Media")
    text_p14 += report.add_content_to_statistic(tables['Social-Media'])

    text_p15 = ""
    text_p15 += report.add_module_header("eshop")
    text_p15 += report.add_content_to_statistic(tables['eshop'])

    text_tmp1 = "text_page1.pdf"
    text_tmp2 = "text_page2.pdf"
    text_tmp3 = "text_page3.pdf"
    text_tmp4 = "text_page4.pdf"
    text_tmp5 = "text_page5.pdf"
    text_tmp6 = "text_page6.pdf"
    text_tmp7 = "text_page7.pdf"
    text_tmp8 = "text_page8.pdf"
    text_tmp9 = "text_page9.pdf"
    text_tmp10 = "text_page10.pdf"
    text_tmp11 = "text_page11.pdf"
    text_tmp12 = "text_page12.pdf"
    text_tmp13 = "text_page_13.pdf"
    text_tmp14 = "text_page14.pdf"
    text_tmp15 = "text_page15.pdf"

    # --------------------- Генерация PDF ---------------------
    html_to_pdf_page(html = text_p1, output_path=text_tmp1, height=600)
    html_to_pdf_page(html = text_p2, output_path = text_tmp2, height=670)
    html_to_pdf_page(text_p3, text_tmp3)
    html_to_pdf_page(text_p4, text_tmp4)
    html_to_pdf_page(text_p5, text_tmp5)
    html_to_pdf_page(text_p6, text_tmp6)
    html_to_pdf_page(text_p7, text_tmp7)
    html_to_pdf_page(text_p8, text_tmp8)
    html_to_pdf_page(text_p9, text_tmp9)
    html_to_pdf_page(text_p10, text_tmp10)
    html_to_pdf_page(text_p11, text_tmp11)
    html_to_pdf_page(text_p12, text_tmp12)
    html_to_pdf_page(text_p13, text_tmp13)
    html_to_pdf_page(text_p14, text_tmp14)
    html_to_pdf_page(text_p15, text_tmp15)

    final_pdf = "text_daily_statistics.pdf"

    merge_pdfs([text_tmp1, text_tmp2, text_tmp3, text_tmp4, text_tmp5, text_tmp6, text_tmp7, text_tmp8, text_tmp9, text_tmp10, text_tmp11, text_tmp12, text_tmp13, text_tmp14, text_tmp15], final_pdf)

    final_path = str(Path(final_pdf).resolve())
    context.log.info(f"final_pdf_path: {final_path}")
    return final_path