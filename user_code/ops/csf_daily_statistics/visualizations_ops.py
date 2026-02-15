import base64
import io
import math
import os
import re
from datetime import datetime, timedelta, date
from typing import Any

from typing import Optional, Dict, List
import matplotlib.transforms as mtransforms
from dagster import op, Out, Output, get_dagster_logger

from matplotlib import pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import FuncFormatter

from ops.csf_daily_statistics.process_ops import potential_fraud_statuses
from utils.array_operations import left_join_array
from utils.getters.naumen_service_level_data_getter import NaumenServiceLevelDataGetter
from utils.mail.mail_report import MailReport, html_to_pdf_page, merge_pdfs
from utils.templates.charts.basic_line_graphics import BasicLineGraphics, color_weekend_xticklabels
from utils.templates.tables.pivot_table import PivotTable
from utils.transformers.mp_report import _norm_date

projects = [
    {
        "array": ["General", "TechSup"],
        "title": "ОСНОВНАЯ ЛИНИЯ - 707"
    },
    {
        "array": ["Saima"],
        "title": "САЙМА - 0706 909 000"
    },
    {
        "array": ["Money", "O!Bank"],
        "title": "О! ДЕНЬГИ + O! BANK - 999, 9999, 8008, 0700000999"
    },
    {
        "array": ["Terminals"],
        "title": "О! ТЕРМИНАЛЫ - 799"
    },
    {
        "array": ["Agent"],
        "title": "О! АГЕНТ - 5858"
    },
    {
        "array": ["Telesales"],
        "title": "TELESALES - Номер ГТМ"
    },
    {
        "array": ["Entrepreneurs"],
        "title": "ОФД - 7878"
    },
    {
        "array": ["AkchaBulak"],
        "title": "АКЧАБУЛАК"
    }
]

from utils.templates.tables.basic_table import BasicTable

from pathlib import Path

@op
def graphic_sl_dynamic_general(context, data):
    fields = {
        ("SL", "Service level")
    }
    result = render_csf_basic_graphs(data,
                                     fields,
                                     target=80,
                                     is_target=True,
                                     ylabel="Процент",
                                     graph_title="Данный график показывает процент вызовов, которые были приняты операторами в рамках установленной цели"
                                     )
    return result

@op
def graphic_calls_dynamic_general(context, data):
    fields = [
        ("ivr_total", "Поступило на IVR"),
        ("ivr_ivr", "Завершены в IVR"),
        ("total_to_operators", "Распределено на операторов"),
        ("answered", "Отвечено операторами"),
        ("lost", "Потерянные")
    ]

    color_map = {
        "ivr_total": "#e2007a",
        "ivr_ivr": "#222d32",
        "total_to_operators": "#49b6d6",
        "answered": "#32a932",
        "lost": "#f59c1a"
    }

    result = render_csf_basic_graphs(data,
                                     fields,
                                     color_map=color_map,
                                     is_target=False,
                                     ylabel="Звонки",
                                     graph_title="Данный график показывает конверсию по звонкам"
                                     )
    return result

@op
def graphic_occupancy_dynamic_general(context, data):
    """
    Occupancy - По времени
    """

    data = data['occupancy']
    fields = {
        ("day", "день"),
        ("evening", "вечер"),
        ("night", "ночь")
    }

    color_map = {
        "day": "#F0047F",
        "evening": "#000000",
        "night": "#90A4AE"
    }

    result = render_csf_basic_graphs(data,
                                     fields,
                                     color_map=color_map,
                                     target=70,
                                     is_target=True,
                                     ylabel="Процент",
                                     graph_title="Данный график показывает процент занятости операторов"
                                     )

    return result


@op
def graphic_productive_time_dynamic(data):
    """
    Продуктивное время и время для обработки вызова (переводим минуты в часы)
    Ожидается, что вход: data = {'detailed': [ { 'productive_time': ..., 'handling_time': ..., 'difference': ..., ... }, ... ] }
    """
    raw = data.get('detailed') if isinstance(data, dict) else data
    rows = raw or []

    # безопасная конверсия в float
    def to_float_safe(x):
        try:
            if x is None:
                return 0.0
            return float(x)
        except Exception:
            # если приходят строки с запятой — заменим на точку и попробуем снова
            try:
                return float(str(x).replace(',', '.'))
            except Exception:
                return 0.0

    # переводим минуты -> часы (делим на 60) в копии данных,
    # чтобы не мутировать исходный список (на случай повторного использования)
    converted_rows = []
    for r in rows:
        rr = dict(r)  # shallow copy
        # поля, которые нужно разделить на 60
        for fld in ("productive_time", "handling_time", "difference"):
            if fld in rr:
                val = to_float_safe(rr.get(fld))
                rr[fld] = round(val / 60.0, 2)   # округляем до 2 знаков (по желанию)
        converted_rows.append(rr)

    fields = {
        ("productive_time", "Продуктивное время"),
        ("handling_time", "Время для обработки вызова"),
        ("difference", "Свободное время")
    }

    color_map = {
        "productive_time": "#000000",
        "handling_time": "#F0047F",
        "difference": "#90A4AE"
    }

    result = render_csf_basic_graphs(
        converted_rows,
        fields,
        color_map=color_map,
        is_target=False,
        label_interval=2,   # Здесь можно изменить интервал, сейчас данные выходят как через день, можно тут задать
        ylabel="Минуты",
        graph_title="Данный график показывает продуктивное время и время обработки вызова"
    )

    return result


@op
def month_statuses_main(month_statuses_data, start_date, report_date):
    items = month_statuses_data.get("main", [])

    def to_ymd(x):
        if hasattr(x, "strftime"):
            return x.strftime("%Y-%m-%d")
        s = str(x)
        return s[:10]

    min_date = to_ymd(start_date)
    max_date = to_ymd(report_date)

    # Строим таблицу
    table = PivotTable()
    table.Items = items
    table.minDate = min_date
    table.maxDate = max_date
    table.ShowSummaryYAxis = True
    table.ShowSummaryXAxis = True

    table.HideRow = False
    table.ShowId = False

    table.ShowProjectTitle = False

    table.PaintType = "asc"
    table.OrderByTotal = "desc"
    table.TitleStyle = "width: 250px;"
    table.RoundValue = 0

    table.Fields = [
        {"field": "date",     "type": "period", "format": "%d.%m"},
        {"field": "status",   "type": "row", "title": "Время"},
        {"field": "duration", "type": "value"},
    ]

    return table.getTable()

@op
def month_statuses_secondary(month_statuses_data, start_date, report_date):
    items = month_statuses_data.get("secondary", [])

    def to_ymd(x):
        if hasattr(x, "strftime"):
            return x.strftime("%Y-%m-%d")
        s = str(x)
        return s[:10]

    min_date = to_ymd(start_date)
    max_date = to_ymd(report_date)

    table = PivotTable()
    table.Items = items
    table.minDate = min_date
    table.maxDate = max_date
    table.ShowSummaryYAxis = True
    table.ShowSummaryXAxis = True

    table.HideRow = False
    table.ShowId = False

    table.ShowProjectTitle = False

    table.PaintType = "asc"
    table.OrderByTotal = "desc"
    table.TitleStyle = "width: 250px;"
    table.RoundValue = 0

    table.Fields = [
        {"field": "date",     "type": "period", "format": "%d.%m"},
        {"field": "status",   "type": "row", "title": "Время"},
        {"field": "duration", "type": "value"},
    ]

    return table.getTable()

@op
def count_statuses_main(count_statuses_data, start_date, report_date):
    items = count_statuses_data.get("main", [])

    def to_ymd(x):
        if hasattr(x, "strftime"):
            return x.strftime("%Y-%m-%d")
        s = str(x)
        return s[:10]

    min_date = to_ymd(start_date)
    max_date = to_ymd(report_date)

    # Строим таблицу
    table = PivotTable()
    table.Items = items
    table.minDate = min_date
    table.maxDate = max_date
    table.ShowSummaryYAxis = True
    table.ShowSummaryXAxis = True

    table.HideRow = False
    table.ShowId = False

    table.ShowProjectTitle = False

    table.PaintType = "asc"
    table.OrderByTotal = "desc"
    table.TitleStyle = "width: 250px;"
    table.RoundValue = 0

    table.Fields = [
        {"field": "date",     "type": "period", "format": "%d.%m"},
        {"field": "status",   "type": "row", "title": "Кол-во"},
        {"field": "count", "type": "value"},
    ]

    return table.getTable()


@op
def count_statuses_secondary(count_statuses_data, start_date, report_date):
    items = count_statuses_data.get("secondary", [])

    def to_ymd(x):
        if hasattr(x, "strftime"):
            return x.strftime("%Y-%m-%d")
        s = str(x)
        return s[:10]

    min_date = to_ymd(start_date)
    max_date = to_ymd(report_date)

    # Строим таблицу
    table = PivotTable()
    table.Items = items
    table.minDate = min_date
    table.maxDate = max_date
    table.ShowSummaryYAxis = True
    table.ShowSummaryXAxis = True

    table.HideRow = False
    table.ShowId = False

    table.ShowProjectTitle = False

    table.PaintType = "asc"
    table.OrderByTotal = "desc"
    table.TitleStyle = "width: 250px;"
    table.RoundValue = 0

    table.Fields = [
        {"field": "date",     "type": "period", "format": "%d.%m"},
        {"field": "status",   "type": "row", "title": "Время"},
        {"field": "count", "type": "value"},
    ]

    return table.getTable()


@op(required_resource_keys={'source_dwh', 'report_utils', 'source_mp'})
def sl_data_joined_op(
        context,
        start_date,
        end_date,
):
    for project in projects:
        get_sl_data = NaumenServiceLevelDataGetter(
            start_date=start_date,
            end_date=end_date,
            conn=context.resources.source_dwh,
            trunc='day',
        )

        get_sl_data.filter_projects_by_group_array(project["array"])
        sl_data = get_sl_data.get_aggregated_data()

        get_sl_data.reload_projects()
        get_sl_data.filter_projects_by_group_array(get_sl_data.project_array, type_='IVR')
        sl_data_ivr = get_sl_data.get_aggregated_data_ivr()

        sl_data_joined = left_join_array(sl_data['items'], sl_data_ivr['items'], left_join_on='date')

        context.log.info(f"sl_data_joined = {sl_data_joined}")

@op
def daily_operators(data):
    table = BasicTable()
    table.Fields = [
        {"field": "hour", "title": "Часы"},
        {
            "field": "erlang",
            "title": "1. План исходя из Эрланга",
            "paint": True,
            "summary": "sum",
        },
        {
            "field": "fact_operators",
            "title": "2. Кол-во операторов по факту",
            "paint": True,
            "summary": "sum",
        },
        {
            "field": "fact_labor",
            "title": "3. Кол-во операторов по в трудозатратах",
            "paint": True,
            "summary": "sum",
        },
        {
            "field": "erlang_and_fact_operators",
            "title": "Разница: 1 - 2",
            "paint": True,
            "summary": "sum",
        },
        {
            "field": "erlang_and_fact_labor",
            "title": "Разница: 1 - 3",
            "paint": True,
            "summary": "sum",
        },
    ]

    table.Items = data
    return table.getTable()

@op
def monthly_operators(context, data, start_date, report_date):
    def _hour_idx(v):
        s = str(v).strip().replace('T', ' ')
        token = s.split()[-1]
        hh = token.split(':')[0]
        return int(hh) if hh.isdigit() else 999

    start_date = start_date.strftime("%Y-%m-%d")
    report_date = report_date.strftime("%Y-%m-%d")

    items = []
    for x in data:
        y = dict(x)
        y['date'] = str(y.get('date', ''))[:10]
        y['__hnum'] = _hour_idx(y.get('hour', '00:00'))
        y['__hden'] = 1
        items.append(y)

    items.sort(key=lambda r: (datetime.strptime(r['date'], '%Y-%m-%d'), r['__hnum']))

    table = PivotTable()
    table.Items = items
    table.RoundValue = 0
    table.minDate = start_date
    table.maxDate = report_date

    table.ShowSummaryYAxis = True
    table.PaintType = 'desc'
    table.TitleStyle = 'width: 250px;'

    table.HideRow = False
    table.ShowId = False

    table.OrderByTotal = 'none'
    table.OrderByAverage = 'none'

    table.Fields = [
        {'field': 'date', 'type': 'period', 'format': '%d.%m'},
        {'field': 'hour', 'type': 'row', 'title': 'Кол-во'},
        {
            'field': 'fact_operators',
            'type': 'value'
        },
    ]
    return table.getTable()


@op(required_resource_keys={'source_dwh', 'report_utils', 'source_naumen_3', 'source_naumen_1'})
def statuses_login_data_op(context, data, start_date, report_date):
    if isinstance(data, dict) and 'login' in data:
        items = data['login']
    else:
        items = data

    items = _norm_date(items)

    start_date_str  = start_date.strftime("%Y-%m-%d")
    report_date_str = report_date.strftime("%Y-%m-%d")

    statuses_login_data = items if isinstance(items, list) else []

    tables = {}
    for status in potential_fraud_statuses.values():
        subset = [row for row in statuses_login_data if row.get('status') == status]
        context.log.info(f"{status} rows = {len(subset)}")

        table = PivotTable()
        table.Items = subset
        table.RoundValue = 1
        if status == "Кофе":
            table.coffee = True
        table.minDate = start_date_str
        table.maxDate = report_date_str
        table.ShowSummaryYAxis = True

        table.HideRow = False
        table.ShowId = False

        table.ShowProjectTitle = False

        table.PaintType = 'asc'
        table.OrderByTotal = 'desc'
        table.TitleStyle = 'width: 250px;'

        table.Fields = [
            {'field': 'date',     'type': 'period', 'format': '%d.%m'},
            {'field': 'login',    'type': 'row', 'title': 'Логин'},
            {'field': 'duration', 'type': 'value'},
        ]

        table.DescriptionColumns = [
            {'field': 'department',
             'title': 'Группа',
             'style': 'width: 250px; font-weight: bold; '},
        ]

        tables[status] = table.getTable()
        context.log.info(f"{status} = {tables[status]}")

    return tables

@op
def graphic_fullness_data(context, data):
    fields = [
        ("total", "Количество созданных задач"),
        ("filled", "Количество заполненных задач"),
        ("not_filled", "Количество незаполненных задач")
    ]

    color_map = {
        "total": "#222d32",
        "filled": "#e2007a",
        "not_filled": "#b8c7ce",
    }
    result = render_csf_basic_graphs_new(data, fields, color_map=color_map)
    context.log.info(f"graphic_fullness_data = {result}")
    return result

def render_csf_basic_graphs(
    items,
    fields,
    target: Optional[float] = 60,
    is_target: bool = True,
    color_map: Optional[Dict[str, str]] = None,
    default_color: str = '#000',
    label_interval: int = 1,
    ylabel: str = "Значение",
    graph_title: str = "Данный график показывает конверсию по звонкам"
):
    """
    Параметры:
    - items: список словарей с данными
    - fields: список описаний полей, например:
        [("ivr_total", "Поступило на IVR", "#F0047F"), ("ivr_answered", "Отвечено")]
      цвет можно указывать в трёх элементах или через color_map
    - color_map: {'field_name': '#RRGGBB'}
    - default_color: цвет по умолчанию, если не указан
    - label_interval: показывать подписи над точками с шагом label_interval (1 = все подписи)
    """

    if not isinstance(label_interval, int) or label_interval < 1:
        label_interval = 1

    # --- Подготовка полей для BasicLineGraphics ---
    graph_fields = [{'field': 'date', 'title': 'Дни', 'type': 'label'}]
    for f in fields:
        if isinstance(f, (list, tuple)):
            if len(f) == 3:
                field_name, title, color = f
            elif len(f) == 2:
                field_name, title = f
                color = None
            else:
                continue
        else:
            field_name, title, color = f, str(f), None
        graph_fields.append({'field': field_name, 'title': title, 'color': color})

    graphic = BasicLineGraphics(
        graph_title="График по выбранным метрикам",
        x_axes_name="Дни",
        y_axes_name=ylabel,
        target=target,
        stroke='smooth',
        show_tooltips=True,
        y_axes_min=0,
        items=items,
        fields=graph_fields,
        width=1920,
        height=500,
    )
    graphic.make_preparation()

    # --- Настройка фигуры ---
    dpi = 100
    fig, ax = plt.subplots(figsize=(graphic.width / dpi, graphic.height / dpi), dpi=dpi)
    fig.subplots_adjust(top=0.85)
    fig.patch.set_facecolor('#f8f9fa')
    ax.set_facecolor('#ffffff')

    parsed_dates = getattr(graphic, 'label_datetimes', None)
    if not parsed_dates:
        # на всякий случай извлечём из items, если кто-то вызвал без label_datetimes
        parsed_dates = []
        for it in items:
            s = str(it.get('date', '')).strip()
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d.%m.%Y", "%d.%m.%y"):
                try:
                    parsed_dates.append(datetime.strptime(s, fmt))
                    break
                except ValueError:
                    continue
            else:
                parsed_dates.append(datetime.fromisoformat(s))

    if parsed_dates:
        ax.set_xlim(parsed_dates[0] - timedelta(days=1),
                    parsed_dates[-1] + timedelta(days=1))
        ax.set_xticks(parsed_dates)

    ax.xaxis.set_major_locator(mdates.AutoDateLocator(minticks=6, maxticks=10))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%d.%m'))

    def choose_color(field_name: Optional[str], title: str) -> str:
        for gf in graph_fields:
            if gf.get('title') == title and gf.get('field') == field_name:
                if gf.get('color'):
                    return gf.get('color')
        if color_map and field_name in color_map:
            return color_map[field_name]
        return default_color

    for ds in graphic.datasets:
        field_name = None
        for gf in graph_fields:
            if gf.get('title') == ds['name']:
                field_name = gf.get('field')
                break

        color = choose_color(field_name, ds['name'])
        y = ds['data']

        ax.plot(parsed_dates, y, linewidth=2.5, label=ds['name'], color=color)

        # Маркеры и подписи — теперь с шагом label_interval
        for idx, (dt_point, value) in enumerate(zip(parsed_dates, y)):
            if value is None:
                continue

            # рисуем маркер всегда
            ax.plot(dt_point, value, marker='o', markersize=6,
                    markerfacecolor=color, markeredgecolor='white', markeredgewidth=1, zorder=4)

            # подписываем только по интервалу
            if label_interval > 1:
                if (idx % label_interval) != 0:
                    continue

            # формат подписи
            if float(value).is_integer():
                label_text = str(int(value))
            else:
                label_text = f"{value:.1f}"

            ax.annotate(
                label_text, (dt_point, value),
                textcoords="offset points", xytext=(0, 0),
                ha='center', fontsize=8, fontweight='bold',
                bbox=dict(boxstyle="round,pad=0.3", facecolor=color,
                          edgecolor='none', alpha=0.9),
                color='white', zorder=5
            )

    # --- Рисуем целевую линию
    if is_target and (target is not None):
        ax.axhline(y=target, color='#38a169', linestyle='-', linewidth=2, alpha=0.8, zorder=1)
        trans = mtransforms.blended_transform_factory(ax.transAxes, ax.transData)
        ax.text(0, target, 'План',
                transform=trans, ha='left', va='center',
                bbox=dict(boxstyle="round,pad=0.3", facecolor='#38a169', edgecolor='none'),
                color='white', fontweight='bold', fontsize=8,
                clip_on=False, zorder=3)

    all_values = []
    for ds in graphic.datasets:
        for v in ds['data']:
            if v is None:
                continue
            if isinstance(v, float) and math.isnan(v):
                continue
            all_values.append(float(v))
    if all_values or (graphic.target is not None):
        min_val = 0.0
        data_max = max(all_values) if all_values else 0.0
        if graphic.target is not None:
            data_max = max(data_max, float(graphic.target))
        padding = max((data_max - min_val) * 0.15, 6.0)
        y_top = data_max + padding
        if (graphic.target is not None) and (y_top <= float(graphic.target)):
            y_top = float(graphic.target) + padding
        ax.set_ylim(min_val, y_top)
        range_val = y_top - min_val
        band_height = max(6.0, range_val * 0.1)
        band_height = float(int(band_height))
        start_band = math.floor(min_val / band_height) * band_height
        end_band = math.ceil(y_top / band_height) * band_height
        i = start_band
        idx = 0
        while i < end_band:
            if idx % 2 == 1:
                ax.axhspan(i, i + band_height, alpha=0.05, color='gray', zorder=0)
            i += band_height
            idx += 1

    # --- Стилизация и сетка ---
    ax.grid(True, linestyle='-', linewidth=0.5, color='#e2e8f0', alpha=0.7, axis='y')
    ax.set_axisbelow(True)

    ax.set_xticks(parsed_dates)
    x_labels = [dt.strftime('%d.%m') for dt in parsed_dates]
    if len(x_labels) >= 2:
        x_labels[0] = ''
        x_labels[-1] = ''
    ax.set_xticklabels(x_labels, rotation=0, fontsize=10)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%d.%m'))
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    color_weekend_xticklabels(ax)

    # --- Форматирование оси Y ---
    ax.yaxis.set_major_formatter(
        FuncFormatter(lambda x, pos: f'{int(x)}' if (not (isinstance(x, float) and math.isnan(x))) else '')
    )

    ax.set_xlabel(graphic.x_axes_name, fontsize=10, color='#4a5568')
    ax.set_ylabel(graphic.y_axes_name, fontsize=10, color='#4a5568')

    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_color('#e2e8f0')
    ax.spines['bottom'].set_color('#e2e8f0')
    ax.tick_params(axis='x', colors='#4a5568', labelsize=7)
    ax.tick_params(axis='y', colors='#4a5568', labelsize=10)

    # --- Легенда: вывести все элементы в одну горизонтальную строку ---
    ax.text(
        0.0, 1.08, graph_title,
        horizontalalignment='left',
        transform=ax.transAxes,
        fontsize=11,
        color='#2d3748',
        fontweight="bold",
        wrap=True
    )

    n_series = max(1, len(graphic.datasets))
    if n_series > 1:
        ax.legend(
            loc='upper right',
            fontsize=8,
            frameon=False,
            ncol = n_series
        )

    # --- Сохраняем результат в SVG и возвращаем base64 ---
    buf = io.BytesIO()
    fig.savefig(buf, format="svg", bbox_inches="tight")
    plt.close(fig)

    buf.seek(0)
    b64 = base64.b64encode(buf.read()).decode("utf-8")
    return b64

def render_csf_basic_graphs_new(
    data: list[dict],
    fields: list[tuple[str, str]],
    *,
    color_map: dict[str, str] | None = None,
    width: int = 1920,
    height: int = 400,
) -> str:
    """
    Рисует график "полноты анкет" (total / filled / not_filled).
    Возвращает base64 SVG-строку.
    """
    color_map = color_map or {}

    # Конфиг для BasicLineGraphics
    fields_cfg = [{"field": "date", "title": "Дни", "type": "label"}]
    series_keys: list[str] = []
    for key, title in fields:
        fields_cfg.append({"field": key, "title": title, "round": 0})
        series_keys.append(key)

    graphic = BasicLineGraphics(
        graph_title="Данный график показывает количество заполненных и незаполненных анкет",
        x_axes_name="Дни",
        y_axes_name="Количество",
        stroke="smooth",
        show_tooltips=True,
        y_axes_min=0,
        items=data or [],
        fields=fields_cfg,
        width=width,
        height=height,
    )
    graphic.make_preparation()

    # Парсер дат под разные форматы
    def _parse_item_dt(s: str) -> datetime:
        s = str(s).strip()
        for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%d.%m.%Y", "%d-%m-%Y", "%d.%m"):
            try:
                dt = datetime.strptime(s, fmt)
                # если без года (например '01.12') — подставим текущий год
                if fmt == "%d.%m":
                    dt = dt.replace(year=datetime.now().year)
                return dt
            except ValueError:
                continue
        # fallback: пусть matplotlib сам попробует
        return datetime.fromisoformat(s) if "T" in s or "-" in s else datetime.strptime(s, "%d.%m.%Y")

    # Реальные даты на X
    dates = [_parse_item_dt(it["date"]) for it in graphic.items] if graphic.items else []

    dpi = 100
    fig, ax = plt.subplots(figsize=(graphic.width / dpi, graphic.height / dpi), dpi=dpi)
    fig.patch.set_facecolor("#f8f9fa")
    ax.set_facecolor("#ffffff")

    # Рисуем серии (цвета берём из color_map по ключу серии)
    for idx, ds in enumerate(graphic.datasets):
        y = ds["data"]
        key = series_keys[idx] if idx < len(series_keys) else ds["name"]
        line_color = color_map.get(key)
        if graphic.stroke == "smooth":
            ax.plot(dates, y, label=ds["name"], linewidth=2.0,
                    marker="o" if graphic.show_tooltips else None,
                    markersize=5.0 if graphic.show_tooltips else 0,
                    color=line_color)
        else:
            # ступенчатый вариант
            ax.step(dates, y, where="mid", label=ds["name"], color=line_color)

    # X-ось
    ax.set_xmargin(0)
    if dates:
        ax.set_xlim(min(dates), max(dates))
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d.%m"))

    # Y-границы «приятные»
    all_values = [v for ds in graphic.datasets for v in ds["data"] if v is not None]
    if all_values:
        data_min, data_max = min(all_values), max(all_values)
        span = data_max - data_min
        if span <= 0:
            span = max(1.0, abs(data_max) * 0.1)
        raw_step = span / 10
        exp = math.floor(math.log10(raw_step)) if raw_step > 0 else 0
        frac = raw_step / (10 ** exp) if raw_step > 0 else 1
        if   frac <= 1:   nice = 1
        elif frac <= 2:   nice = 2
        elif frac <= 2.5: nice = 2.5
        elif frac <= 5:   nice = 5
        else:             nice = 10
        step = nice * (10 ** exp)
        padding = max(step, span * 0.15)
        y0 = math.floor((data_min - padding) / step) * step
        y1 = math.ceil((data_max + padding) / step) * step
        ax.set_ylim(y0, y1)
        # лёгкая заливка
        i, yy = 0, y0
        while yy < y1:
            if i % 2 == 1:
                ax.axhspan(yy, yy + step, alpha=0.05, color="gray", zorder=0)
            yy += step
            i += 1
    else:
        ax.set_ylim(0, 10)

    # Сетка/стили
    ax.grid(True, linestyle="-", linewidth=0.5, color="#e2e8f0", alpha=0.7, axis="y")
    ax.set_axisbelow(True)
    ax.set_xlabel(graphic.x_axes_name, fontsize=10, color="#4a5568")
    ax.set_ylabel(graphic.y_axes_name, fontsize=10, color="#4a5568")
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    ax.spines["left"].set_color("#e2e8f0")
    ax.spines["bottom"].set_color("#e2e8f0")
    ax.tick_params(axis="x", colors="#4a5568", labelsize=8)
    ax.tick_params(axis="y", colors="#4a5568", labelsize=10)

    # Подсветка выходных, если утилита подключена
    try:
        color_weekend_xticklabels(ax)  # твоя вспомогательная функция
    except Exception:
        pass

    # Заголовок/легенда
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

    # SVG → base64
    buf = io.BytesIO()
    fig.savefig(buf, format="svg", bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")

def _to_ymd(s: Any) -> str:
    if isinstance(s, (date, datetime)):
        return s.strftime("%Y-%m-%d")
    if s is None:
        return ""
    return str(s).strip()[:10]

def _normalize_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in items or []:
        rr = dict(r)
        if "date" in rr:
            rr["date"] = _to_ymd(rr["date"])
        out.append(rr)
    return out

@op(out={"tables": Out(dict)})
def build_projects_tables(context, modules_data: List[Dict[str, Any]]):
    """
    Возвращает словарь base64 SVG для вставки через MailReport.add_figure(...):
      tables["SL — <TITLE>"]      -> <svg_base64>
      tables["Звонки — <TITLE>"]  -> <svg_base64>
    """
    tables: Dict[str, str] = {}

    if not modules_data:
        context.log.warning("build_projects_tables: пустой вход — верну пустой dict")
        yield Output(tables, "tables")
        return

    context.log.info(f"build_projects_tables: проектов={len(modules_data)}")

    for idx, mod in enumerate(modules_data, start=1):
        title = mod.get("project_title", "Без названия")
        rows  = _normalize_items(mod.get("data_joined", []) or [])
        context.log.info(f"[{idx}/{len(modules_data)}] {title}: rows={len(rows)}")

        sl_key = f"SL — {title}"
        try:
            sl_svg_b64 = render_csf_basic_graphs(
                items=rows,
                fields=[("SL", "Service Level")],
                target=80,
                is_target=True,
                ylabel="Процент",
                graph_title="Данный график показывает процент вызовов, которые были приняты операторами в рамках установленной цели"
            )
            tables[sl_key] = sl_svg_b64
            context.log.info(f"  ✓ {sl_key}: SVG готов")
        except Exception as e:
            context.log.warning(f"  ! {sl_key}: ошибка рендера SVG: {e}")
            tables[sl_key] = base64.b64encode("<div>Нет данных</div>".encode("utf-8")).decode("ascii")  # заглушка

        calls_key = f"Звонки — {title}"
        try:
            calls_svg_b64 = render_csf_basic_graphs(
                items=rows,
                fields=[
                    ("ivr_total",          "Поступило на IVR",          "#e2007a"),
                    ("ivr_ivr",            "Завершены в IVR",           "#222d32"),
                    ("total_to_operators", "Распределено на операторов","#49b6d6"),
                    ("answered",           "Отвечено операторами",      "#32a932"),
                    ("lost",               "Потерянные",                "#f59c1a"),
                ],
                target=None,
                is_target=False,
                ylabel="Звонки",
                graph_title="Данный график показывает конверсию по звонкам"
            )
            tables[calls_key] = calls_svg_b64
            context.log.info(f"  ✓ {calls_key}: SVG готов")
        except Exception as e:
            context.log.warning(f"  ! {calls_key}: ошибка рендера SVG: {e}")
            tables[calls_key] = base64.b64encode("<div>Нет данных</div>".encode("utf-8")).decode("ascii")

    context.log.info(f"build_projects_tables: итоговых ключей={len(tables)}; пример: {list(tables.keys())[:4]}")
    yield Output(tables, "tables")

def _to_int(v) -> int:
    try:
        if v is None:
            return 0
        return int(round(float(v)))
    except Exception:
        return 0

def _to_float(v) -> float:
    try:
        return float(v) if v is not None else 0.0
    except Exception:
        return 0.0

@op(out=Out(str), description="Рендерит HTML-таблицу профиля дня (BasicTable) по детальным почасовым данным.")
def render_day_profile(detailed_data):
    rows = []

    get_dagster_logger().info(f"detailed_data = {detailed_data}")
    int_fields = {
        "fact_labor", "fact_operators", "fact_operators_and_fact_labor",
        "ivr_total", "total_to_operators", "answered", "total_queue",
        "product_time", "handling_time", "erlang"
    }

    for r in (detailed_data or []):
        row = dict(r)
        if not row.get("hour") and row.get("date"):
            row["hour"] = row["date"][-8:]

        for k in int_fields:
            if k in row:
                row[k] = _to_int(row.get(k))

        # Occupancy — вычисляем/нормируем как float с 2 знаками
        occ = row.get("occupancy")
        if occ is None:
            pt = row.get("product_time", 0)
            ht = row.get("handling_time", 0)
            row["occupancy"] = round((ht / pt * 100.0), 2) if pt > 0 else 0.0
        else:
            try:
                row["occupancy"] = max(0.0, min(100.0, float(occ)))
            except Exception:
                row["occupancy"] = 0.0

        rows.append(row)

    get_dagster_logger().info(f"rows = {rows}")
    table = BasicTable()
    table.Fields = [
        {"field": "hour", "title": "Часы"},

        # Эрланг — дробный
        {"field": "erlang", "title": "Эрланг", "paint": True, "summary": "sum"},

        # Счётчики — целые (без round!)
        {"field": "fact_labor", "title": "График", "paint": True, "summary": "sum"},
        {"field": "fact_operators", "title": "Факт", "paint": True, "summary": "sum"},
        {
            "field": "fact_operators_and_fact_labor",
            "title": "Дельта Факт и График",
            "paint": True,
            "summary": "sum",
        },
        {"field": "ivr_total", "title": "Поступило на IVR", "paint": True, "summary": "sum"},
        {"field": "total_to_operators", "title": "Распределено на операторов", "paint": True, "summary": "sum"},
        {"field": "answered", "title": "Отвечено операторами", "paint": True, "summary": "sum"},
        {"field": "total_queue", "title": "Завершены в очереди", "paint": True, "summary": "sum"},

        # Процентные метрики — с округлением
        {
            "field": "SL",
            "title": "Service Level",
            "paint": 'desc',
            "round": 1,
            "paint_type": "asc",
            "advancedSummaryAvg": {"numerator": "sla_answered", "denominator": "sla_total", "round": 1},
        },
        {
            "field": "ACD",
            "title": "Handled Calls Rate",
            "paint": 'desc',
            "round": 1,
            "paint_type": "asc",
            "advancedSummaryAvg": {"numerator": "answered", "denominator": "total_to_operators", "round": 1},
        },
        {
            "field": "occupancy",
            "title": "Occupancy, %",
            "paint": 'desc',
            "round": 1,
            "paint_type": "asc",
            "advancedSummaryAvg": {"numerator": "handling_time", "denominator": "product_time", "round": 2},
        },
    ]

    table.Items = rows

    return table.getTable()

@op(out=Out(str), description="Рендерит HTML-таблицу профиля дня (BasicTable) по детальным почасовым данным.")
def render_day_profile_new(detailed_data):
    rows = []

    get_dagster_logger().info(f"detailed_data = {detailed_data}")
    int_fields = {
        "fact_labor", "fact_operators", "fact_operators_and_fact_labor",
        "ivr_total", "total_to_operators", "answered", "total_queue",
        "product_time", "handling_time", "erlang"
    }

    for r in (detailed_data or []):
        row = dict(r)
        if not row.get("hour") and row.get("date"):
            row["hour"] = row["date"][-8:]

        for k in int_fields:
            if k in row:
                row[k] = _to_int(row.get(k))

        # Occupancy — вычисляем/нормируем как float с 2 знаками
        occ = row.get("occupancy")
        if occ is None:
            pt = row.get("product_time", 0)
            ht = row.get("handling_time", 0)
            row["occupancy"] = round((ht / pt * 100.0), 2) if pt > 0 else 0.0
        else:
            try:
                row["occupancy"] = max(0.0, min(100.0, float(occ)))
            except Exception:
                row["occupancy"] = 0.0

        rows.append(row)

    get_dagster_logger().info(f"rows = {rows}")
    table = BasicTable()
    table.Fields = [
        {"field": "hour", "title": "Часы"},

        # Эрланг — дробный
        {"field": "erlang", "title": "Эрланг", "paint": True, "summary": "sum"},

        # Счётчики — целые (без round!)
        {"field": "fact_labor", "title": "График", "paint": True, "summary": "sum"},
        {"field": "fact_operators", "title": "Факт", "paint": True, "summary": "sum"},
        {
            "field": "fact_operators_and_fact_labor",
            "title": "Дельта Факт и График",
            "paint": True,
            "summary": "sum",
        },
        {"field": "ivr_total", "title": "Поступило на IVR", "paint": True, "summary": "sum"},
        {"field": "value", "title": "Поступило на IVR через O!Brand", "paint": True, "summary": "sum"},
        {"field": "total_to_operators", "title": "Распределено на операторов", "paint": True, "summary": "sum"},
        {"field": "answered", "title": "Отвечено операторами", "paint": True, "summary": "sum"},
        {"field": "total_queue", "title": "Завершены в очереди", "paint": True, "summary": "sum"},

        # Процентные метрики — с округлением
        {
            "field": "SL",
            "title": "Service Level",
            "paint": 'desc',
            "round": 1,
            "paint_type": "asc",
            "advancedSummaryAvg": {"numerator": "sla_answered", "denominator": "sla_total", "round": 1},
        },
        {
            "field": "ACD",
            "title": "Handled Calls Rate",
            "paint": 'desc',
            "round": 1,
            "paint_type": "asc",
            "advancedSummaryAvg": {"numerator": "answered", "denominator": "total_to_operators", "round": 1},
        },
        {
            "field": "occupancy",
            "title": "Occupancy, %",
            "paint": 'desc',
            "round": 1,
            "paint_type": "asc",
            "advancedSummaryAvg": {"numerator": "handling_time", "denominator": "product_time", "round": 2},
        },
    ]

    table.Items = rows

    return table.getTable()


@op(required_resource_keys={"report_utils", "mp_daily_config"})
def create_html_op(context, csf_fig1, csf_fig2, csf_fig3, csf_fig4, csf_fig5, csf_fig6, csf_fig7, csf_fig8, csf_fig9, csf_fig10, csf_fig11, csf_fig12, csf_fig13, csf_fig14, csf_fig15, csf_fig16, csf_fig17, csf_tables, csf_tables2, csf_fullness_data):
    report = MailReport()
    report.image_mode = "file"

    APP_ROOT = Path(__file__).resolve().parents[2]

    LOGO_PATH = APP_ROOT / "utils" / "logo_png" / "logo.png"

    report.logo_file_override = str(LOGO_PATH)
    dates = context.resources.report_utils.get_utils()
    report_date = datetime.strptime(dates.ReportDate, "%Y-%m-%d")
    csf_p1 = ""
    csf_p1 += report.set_basic_header(f"ЕЖЕДНЕВНАЯ СТАТИСТИКА ИСС НА {report_date.strftime('%d.%m.%Y')}")
    csf_p1 += report.add_block_header("ВСЕ ПРОЕКТЫ ")
    csf_p1 += report.add_module_header(f"Динамика по SL за 31 день по всем проектам ")
    csf_p1 += report.add_figure(csf_fig1)
    csf_p1 += report.add_module_header(f"Общая динамика звонков за 31 день по всем проектам")
    csf_p1 += report.add_figure(csf_fig2)
    csf_p1 += report.add_block_header(f"Occupancy")
    csf_p1 += report.add_module_header(f"По времени ")
    csf_p1 += report.add_figure(csf_fig3)
    csf_p1 += report.add_module_header(f"Продуктивное время и время для обработки вызова ")
    csf_p1 += report.add_figure(csf_fig4)

    csf_p2 = ""
    csf_p2 += report.add_block_header(f"Статистика по кол-ву операторов и уровня сервиса")
    csf_p2 += report.add_module_header(f"Динамика кол-ва операторов за отчетный день")
    csf_p2 += report.add_content_to_statistic(csf_fig5)
    csf_p2 += report.add_module_header(f"Динамика кол-ва операторов по факту по часам за 31 день")
    csf_p2 += report.add_content_to_statistic(csf_fig6)

    csf_p3 = ""
    csf_p3 += report.add_module_header(f"Динамика кол-ва операторов и уровня сервиса по Линии 707")
    csf_p3 += report.add_content_to_statistic(csf_fig11)
    csf_p3 += report.add_module_header(f"Динамика кол-ва операторов и уровня сервиса по Saima")
    csf_p3 += report.add_content_to_statistic(csf_fig12)

    csf_p4 = ""
    csf_p4 += report.add_module_header(f"Динамика кол-ва операторов и уровня сервиса по О!Банк")
    csf_p4 += report.add_content_to_statistic(csf_fig13)
    csf_p4 += report.add_module_header(f"Динамика кол-ва операторов и уровня сервиса по О!Агент")
    csf_p4 += report.add_content_to_statistic(csf_fig14)

    csf_p5 = ""
    csf_p5 += report.add_module_header(f"Динамика кол-ва операторов и уровня сервиса по О!Терминалы")
    csf_p5 += report.add_content_to_statistic(csf_fig15)
    csf_p5 += report.add_module_header(f"Динамика кол-ва операторов и уровня сервиса по Акча-Булак")
    csf_p5 += report.add_content_to_statistic(csf_fig16)
    csf_p5 += report.add_module_header(f"Динамика кол-ва операторов и уровня сервиса по О!Касса")
    csf_p5 += report.add_content_to_statistic(csf_fig17)


    csf_p6 = ""
    csf_p6 += report.add_block_header(f"Статистика по статусам")
    csf_p6 += report.add_module_header(f"Динамика по кол-ву времени в статусах за 31 день (минуты) ")
    csf_p6 += report.add_module_child_header(f"Потенциально-фродовые статусы ")
    csf_p6 += report.add_content_to_statistic(csf_fig7)
    csf_p6 += report.add_module_child_header(f"Все статусы")
    csf_p6 += report.add_content_to_statistic(csf_fig8)
    csf_p6 += report.add_module_header(f"Динамика по кол-ву выбранных статусов за 31 день")
    csf_p6 += report.add_module_child_header(f"Потенциально-фродовые статусы")
    csf_p6 += report.add_content_to_statistic(csf_fig9)
    csf_p6 += report.add_module_child_header(f"Все статусы")
    csf_p6 += report.add_content_to_statistic(csf_fig10)

    csf_p7 = ""
    csf_p7 += report.add_block_header(f"Детализация по проектам")
    csf_p7 += report.add_module_header(f"ОСНОВНАЯ ЛИНИЯ - 707 ")
    csf_p7 += report.add_module_child_header(f"Динамика по SL за 31 день по всем проектам ")
    csf_p7 += report.add_figure(csf_tables['SL — ОСНОВНАЯ ЛИНИЯ - 707'])
    csf_p7 += report.add_module_child_header(f"Общая динамика звонков за 31 день по всем проектам")
    csf_p7 += report.add_figure(csf_tables['Звонки — ОСНОВНАЯ ЛИНИЯ - 707'])

    csf_p8 = ""
    csf_p8 += report.add_module_header(f"САЙМА - 0706 909 000")
    csf_p8 += report.add_module_child_header("Динамика по SL за 31 день по всем проектам ")
    csf_p8 += report.add_figure(csf_tables['SL — САЙМА - 0706 909 000'])
    csf_p8 += report.add_module_child_header(f"Общая динамика звонков за 31 день по всем проектам ")
    csf_p8 += report.add_figure(csf_tables['Звонки — САЙМА - 0706 909 000'])

    csf_p9 = ""
    csf_p9 += report.add_module_header(f"О! ДЕНЬГИ + O! BANK - 999, 9999, 8008, 0700000999")
    csf_p9 += report.add_module_child_header(f"Динамика по SL за 31 день по всем проектам ")
    csf_p9 += report.add_figure(csf_tables['SL — О! ДЕНЬГИ + O! BANK - 999, 9999, 8008, 0700000999'])
    csf_p9 += report.add_module_child_header(f"Общая динамика звонков за 31 день по всем проектам ")
    csf_p9 += report.add_figure(csf_tables['Звонки — О! ДЕНЬГИ + O! BANK - 999, 9999, 8008, 0700000999'])

    csf_p10 = ""
    csf_p10 += report.add_module_header(f"О! ТЕРМИНАЛЫ - 799")
    csf_p10 += report.add_module_child_header(f"Динамика по SL за 31 день по всем проектам ")
    csf_p10 += report.add_figure(csf_tables['SL — О! ТЕРМИНАЛЫ - 799'])
    csf_p10 += report.add_module_child_header(f"Общая динамика звонков за 31 день по всем проектам ")
    csf_p10 += report.add_figure(csf_tables['Звонки — О! ТЕРМИНАЛЫ - 799'])

    csf_p11 = ""
    csf_p11 += report.add_module_header(f"О! АГЕНТ - 5858")
    csf_p11 += report.add_module_child_header(f"Динамика по SL за 31 день по всем проектам ")
    csf_p11 += report.add_figure(csf_tables['SL — О! АГЕНТ - 5858'])
    csf_p11 += report.add_module_child_header(f"Общая динамика звонков за 31 день по всем проектам ")
    csf_p11 += report.add_figure(csf_tables['Звонки — О! АГЕНТ - 5858'])

    csf_p12 = ""
    csf_p12 += report.add_module_header(f"ОФД - 7878")
    csf_p12 += report.add_module_child_header(f"Динамика по SL за 31 день по всем проектам ")
    csf_p12 += report.add_figure(csf_tables['SL — ОФД - 7878'])
    csf_p12 += report.add_module_child_header(f"Общая динамика звонков за 31 день по всем проектам  ")
    csf_p12 += report.add_figure(csf_tables['Звонки — ОФД - 7878'])

    csf_p13 = ""
    csf_p13 += report.add_module_header(f"АКЧАБУЛАК")
    csf_p13 += report.add_module_child_header(f"Динамика по SL за 31 день по всем проектам ")
    csf_p13 += report.add_figure(csf_tables['SL — АКЧАБУЛАК'])
    csf_p13 += report.add_module_child_header(f"Общая динамика звонков за 31 день по всем проектам ")
    csf_p13 += report.add_figure(csf_tables['Звонки — АКЧАБУЛАК'])

    csf_p14 = ""
    csf_p14 += report.add_block_header(f"Динамика по кол-ву времени в статусах по операторам за 31 день (минуты)")
    csf_p14 += report.add_module_header("Отсутствует ")
    csf_p14 += report.add_content_to_statistic(csf_tables2['Отсутствует'])

    csf_p15 = ""
    csf_p15 += report.add_module_header("Блокировка по инициативе супервайзера  ")
    csf_p15 += report.add_content_to_statistic(csf_tables2['Блокировка по инициативе супервайзера'])
    csf_p15 += report.add_module_header("Обед")
    csf_p15 += report.add_content_to_statistic(csf_tables2['Обед'])

    csf_p16 = ""
    csf_p16 += report.add_module_header("Кофе")
    csf_p16 += report.add_content_to_statistic(csf_tables2['Кофе'])

    csf_p17 = ""
    csf_p17 += report.add_block_header("Динамика по кол-ву анкет")
    csf_p17 += report.add_content_to_statistic(csf_fullness_data)

    # --------------------- Временные файлы ---------------------
    csf_tmp1 = "csf_page1.pdf"
    csf_tmp2 = "csf_page2.pdf"
    csf_tmp3 = "csf_page3.pdf"
    csf_tmp4 = "csf_page4.pdf"
    csf_tmp5 = "csf_page5.pdf"
    csf_tmp6 = "csf_page6.pdf"
    csf_tmp7 = "csf_page7.pdf"
    csf_tmp8 = "csf_page8.pdf"
    csf_tmp9 = "csf_page9.pdf"
    csf_tmp10 = "csf_page10.pdf"
    csf_tmp11 = "csf_page11.pdf"
    csf_tmp12 = "csf_page12.pdf"
    csf_tmp13 = "csf_page13.pdf"
    csf_tmp14 = "csf_page14.pdf"
    csf_tmp15 = "csf_page15.pdf"
    csf_tmp16 = "csf_page16.pdf"
    csf_tmp17 = "csf_page17.pdf"


    # --------------------- Генерация PDF ---------------------
    html_to_pdf_page(html=csf_p1, output_path=csf_tmp1)
    html_to_pdf_page(html=csf_p2, output_path=csf_tmp2, height=1000)
    html_to_pdf_page(html=csf_p3, output_path=csf_tmp3)
    html_to_pdf_page(html=csf_p4, output_path=csf_tmp4)
    html_to_pdf_page(html=csf_p5, output_path=csf_tmp5)
    html_to_pdf_page(html=csf_p6, output_path=csf_tmp6, height=1126)
    html_to_pdf_page(html=csf_p7, output_path=csf_tmp7)
    html_to_pdf_page(html=csf_p8, output_path=csf_tmp8)
    html_to_pdf_page(html=csf_p9, output_path=csf_tmp9)
    html_to_pdf_page(html=csf_p10, output_path=csf_tmp10)
    html_to_pdf_page(html=csf_p11, output_path=csf_tmp11)
    html_to_pdf_page(html=csf_p12, output_path=csf_tmp12)
    html_to_pdf_page(html=csf_p13, output_path=csf_tmp13)
    html_to_pdf_page(html=csf_p14, output_path=csf_tmp14)
    html_to_pdf_page(html=csf_p15, output_path=csf_tmp15)
    html_to_pdf_page(html=csf_p16, output_path=csf_tmp16)
    html_to_pdf_page(html=csf_p17, output_path=csf_tmp17)

    final_pdf = "csf_daily_statistics.pdf"

    merge_pdfs(
        [
            csf_tmp1, csf_tmp2, csf_tmp3, csf_tmp4, csf_tmp5, csf_tmp6, csf_tmp7, csf_tmp8, csf_tmp9, csf_tmp10,
            csf_tmp11, csf_tmp12, csf_tmp13, csf_tmp14, csf_tmp15, csf_tmp16, csf_tmp17
        ],
        final_pdf
    )

    final_path = str(Path(final_pdf).resolve())
    context.log.info(f"final_pdf_path: {final_path}")
    return final_path