import os

import dagster
from utils.report_data_collector import set_jira_issues_data, render_jira_issues_table
import base64
import io
import pandas as pd
import math
from dagster import op, get_dagster_logger
from matplotlib import pyplot as plt

from resources import ReportUtils
from utils.getters.omni_data_getter import OmniDataGetter
from utils.mail.mail_report import MailReport, html_to_pdf_page, merge_pdfs
from pathlib import Path
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, List

from utils.templates.charts.multi_axis_chart import MultiAxisChart
from utils.templates.tables.pivot_table import PivotTable
from utils.templates.tables.table_constructor import TableConstructor
from utils.templates.tables.basic_table import BasicTable
from utils.transformers.mp_report import smart_number, _normalize, _parse_dt_any

def _as_dt(x):
    if hasattr(x, "strftime"):
        return x
    s = str(x)
    # пробуем ISO, потом популярные форматы
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d.%m.%Y"):
            try:
                return datetime.strptime(s, fmt)
            except ValueError:
                pass
    raise ValueError(f"Cannot parse date: {x!r}")

@op(required_resource_keys={'report_utils'})
def main_table_constructor_op(context, main_data_dynamic_general, sl_general_average_data, acd_general_average_data, main_data_dynamic_general_ivr, self_service_rate_current_month_general_array,
                            goo_utilization_rate_date, goo_utilization_rate_date_month_general_rounded, occupancy_current_month_general, occupancy_data_dynamic_general_goo, handling_times_data_dynamic_general,
                            aht_general_average_data, average_ringing_time_current_month_general, average_holding_time_current_month_general,
                            cea_data_dynamic_general, critical_error_accuracy_current_month_general, csi_data_dynamic,
                            csi_general_average_data, conversion_average_data, rc_data_dynamic_main, rc_general_average_data,
                            closed_tasks_data_general_dynamic, closed_tasks_sla_month):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    return main_table_constructor(main_data_dynamic_general, sl_general_average_data, acd_general_average_data, main_data_dynamic_general_ivr, self_service_rate_current_month_general_array,
                            goo_utilization_rate_date, goo_utilization_rate_date_month_general_rounded, occupancy_current_month_general, occupancy_data_dynamic_general_goo, handling_times_data_dynamic_general,
                            aht_general_average_data, average_ringing_time_current_month_general, average_holding_time_current_month_general,
                            cea_data_dynamic_general, critical_error_accuracy_current_month_general, csi_data_dynamic,
                            csi_general_average_data, conversion_average_data, rc_data_dynamic_main, rc_general_average_data,
                            closed_tasks_data_general_dynamic, closed_tasks_sla_month, start_date, end_date, date_list, formatted_dates, month_start, month_end)

@op
def table_handled_time_goo_op(items):
    table_handled_time = BasicTable()
    table_handled_time.Fields = [
        {'field': 'hour', 'title': 'Часы'},

        {'field': 'erlang', 'title': 'Эрланг', 'paint': True, 'summary': 'sum', 'round': 0},
        {'field': 'fact_labor', 'title': 'График', 'paint': True, 'summary': 'sum', 'round': 0},
        {'field': 'fact_operators', 'title': 'Факт', 'paint': True, 'summary': 'sum', 'round': 0},
        {'field': 'fact_operators_and_fact_labor', 'title': 'Дельта Факт и График', 'pain_type': 'desc',
         'customPaint': True, 'summary': 'sum', 'round': 1},

        {'field': 'ivr_total', 'title': 'Поступило на IVR', 'paint': True, 'summary': 'sum', 'round': 0},
        {'field': 'total_to_operators', 'title': 'Распределено на операторов', 'paint': True, 'summary': 'sum',
         'round': 0},
        {'field': 'answered', 'title': 'Отвечено операторами', 'paint': True, 'summary': 'sum', 'round': 0},
        {'field': 'total_queue', 'title': 'Завершены в очереди', 'paint': True, 'summary': 'sum', 'round': 0},

        {'field': 'maximum_waiting_time', 'title': 'Maximum Waiting Time',
         'summary': 'max', 'round': 0, 'paint': True, 'time_format': True},
        {'field': 'max_pickup_time', 'title': 'Maximum Ringing Time',
         'paint': True, 'summary': 'max', 'round': 0, 'time_format': True},
        {'field': 'max_speaking_time', 'title': 'Maximum Speaking Time',
         'paint': True, 'summary': 'max', 'round': 0, 'time_format': True},
        {'field': 'max_holding_time', 'title': 'Maximum Holding Time',
         'paint': True, 'summary': 'max', 'round': 0, 'time_format': True},

        {'field': 'SL', 'title': 'Service Level', 'paint': True, 'paint_type': 'asc',
         'round': 1, 'advancedSummaryAvg': {'numerator': 'sla_answered', 'denominator': 'sla_total', 'round': 1}},
        {'field': 'ACD', 'title': 'Handled Calls Rate', 'paint': True, 'paint_type': 'asc',
         'round': 1, 'advancedSummaryAvg': {'numerator': 'answered', 'denominator': 'total_to_operators', 'round': 1}},
        {'field': 'occupancy', 'title': 'Occupancy', 'paint': True, 'paint_type': 'asc',
         'round': 1, 'advancedSummaryAvg': {'numerator': 'handling_time', 'denominator': 'product_time', 'round': 1}},
        {'field': 'customer_satisfaction_index', 'title': 'Customer Satisfaction Index',
         'paint': True, 'paint_type': 'asc', 'round': 1,
         'advancedSummaryAvg': {'numerator': 'good_marks', 'denominator': 'all_marks', 'round': 1}},
    ]

    # данные
    table_handled_time.Items = items
    html_table = table_handled_time.getTable()
    return html_table

@op
def table_handled_time_gpo_op(items):
    table_handled_time = BasicTable()
    table_handled_time.Fields = [
        {"field": "hour", "title": "Часы"},

        {"field": "erlang", "title": "Эрланг", "round": 1, "summary": "sum", "paint": True},
        {"field": "fact_labor", "title": "График", "round": 1, "summary": "sum", "paint": True},
        {"field": "fact_operators", "title": "Факт", "round": 1, "summary": "sum", "paint": True},
        {"field": "fact_operators_and_fact_labor", "title": "Дельта Факт и График",
         "customPaint": True, "round": 1, "summary": "sum"},

        {"field": "total_chats", "title": "Поступило обращений", "paint": True, "summary": "sum", 'paint_type': 'desc'},
        {"field": "total_handled_chats", "title": "Обработано обращений", "paint": True, "summary": "sum", 'paint_type': 'desc'},
        {"field": "total_by_operator", "title": "Закрыто без задержки (операторами)", "paint": True, "summary": "sum", 'paint_type':'asc'},
        {"field": "bot_handled_chats", "title": "Закрыто с задержкой 12 часов (чат-ботом)", "paint": True,
         "summary": "sum"},

        {"field": "average_reaction_time", "title": "Average Reaction Time",
         "paint": True, "round": 1,
         "avgFor24": {
             "numerator": "average_reaction_time",
             "denominator": "average_reaction_time",
             "time_format": True,
         },
         "time_format": True, 'paint_type': 'desc'},

        {"field": "average_speed_to_answer", "title": "Average Speed To Answer",
         "paint": True, "round": 1,
         "avgFor24": {
             "numerator": "chat_total_reaction_time",
             "denominator": "chat_count_replies",
             "time_format": True,
         },
         "time_format": True, 'paint_type': 'desc'},

        {"field": "customer_satisfaction_index", "title": "Customer Satisfaction Index",
         "paint": "desc", "round": 1,
         "advancedSummaryAvg": {
             "numerator": "good_marks",
             "denominator": "all_marks",
             "round": 1,
         }, "paint_type": 'asc'},
    ]

    table_handled_time.Items = items
    html_table = table_handled_time.getTable()
    return html_table

@op
def table_technical_works_op(technical_works_data_report_day):
    table = BasicTable()
    table.Fields = [
    {'title': 'Дата начала', 'field': 'date_begin', 'date_format': '%d.%m.%Y, %H:%M'},
    {'title': 'Дата завершения', 'field': 'date_end', 'date_format': '%d.%m.%Y, %H:%M'},
    {'title': 'Время простоя', 'field': 'downtime'},
    {'title': 'Сервис', 'field': 'service'},
    {'title': 'Описание', 'field': 'description'},
    {'title': 'Количество обращений', 'field': 'technical_work_count'},
]

    table.Items = technical_works_data_report_day


    for row in table.Items:
        if isinstance(row.get("date_begin"), datetime):
            row["date_begin"] = row["date_begin"].strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(row.get("date_end"), datetime):
            row["date_end"] = row["date_end"].strftime("%Y-%m-%d %H:%M:%S")

    html = table.getTable()
    return html

@op
def omni_messenger_detailed_table_op(items):
    for it in items:
        if "date" in it and isinstance(it["date"], str):
            try:
                it["date"] = datetime.strptime(it["date"], "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d")
            except ValueError:
                pass
    table = PivotTable()
    table.RoundValue = 1
    table.RowTitleStyle = "font-weight: bold; text-align: left"
    table.PaintType = "asc"

    table.HideRow = False

    table.ShowId = False

    table.OrderByTotal = 'desc'
    table.Items = items

    table.DescriptionColumns = []

    table.Fields = [
    {"field": "date", "type": "period", "format": "%d.%m"},
    {"field": "project", "type": "row", "title": "Период"},
    {
        "field": "total",
        "type": "value",
        "round": 1,
    },
]
    html = table.getTable()
    return html

@op
def graphic_calls_dynamic_general_op(items):
    chart = MultiAxisChart(
        graph_title="",
        stack=True,
        y_axes_name="Количество",
        y_axes_min=0,
        y_axes_max=None,
        fields=[
            {"field": "date", "title": "", "type": "label", "stacked": True},
            {"field": "answered", "title": "Вызовы, обработанные операторами", "chartType": "bar", "round": 1},
            {"field": "lost", "title": "Потерянные", "chartType": "bar", "round": 1},
            {"field": "ivr_total", "title": "Все вызовы на проектные линии", "chartType": "line", "round": 1},
        ],
        items=items,
        dot=True,
        base=1
    )
    fig, ax = plt.subplots(
        figsize=(chart.width / chart.dpi, chart.height / chart.dpi),
        dpi=chart.dpi,
    )
    chart.render_graphics(ax)
    buf = io.BytesIO()
    fig.savefig(buf, format="svg", bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")

@op
def table_calls_dynamic_general_op(items, projects_data):
    for it in items:
        it["date"] = it["date"][:10]
    table = PivotTable()
    table.RoundValue = 1
    table.Items = items
    table.ShowSummaryYAxis = False
    table.ShowSummaryXAxis = True
    table.ShowCalculated = False
    table.AllProjectsData = projects_data
    table.OrderByTotal = 'desc'

    table.HideRow = False
    table.ShowId = False

    table.ShowMaxYAxis = False
    table.ShowProjectTitle = False
    table.ShowMinYAxis = False
    table.PaintType = "asc"
    table.ShowTotalPercentage = False
    table.TitleStyle = "width: 250px;"

    table.DescriptionColumns = []

    table.Fields = [
        {
            "field": "date",
            "type": "period",
            "format": "%d.%m",
        },
        {"field": "projectName", "type": "row", "title": "Проект"},
        {"field": "ivr_total", "type": "value", "round": 1},
    ]
    html = table.getTable()
    return html

@op
def table_calls_sl_op(context, items, projects_data):
    return table_calls_indicators(items=items, projects_data=projects_data, field="SL")


@op
def table_calls_acd_op(context, items, projects_data):
    return table_calls_indicators(items=items, projects_data=projects_data, field="ACD")

@op
def table_aht_op(items, projects_data):
    items_norm = _normalize(items)
    projects_norm = _normalize(projects_data["items"])

    table = PivotTable()
    table.RoundValue = 0
    table.Items = items_norm
    table.ShowSummaryYAxis = False
    table.ShowSummaryXAxis = False
    table.ShowCalculated = True
    table.ShowAvgCalculated = True

    table.HideRow = False
    table.ShowId = False

    table.AllProjectsData = projects_norm
    table.OrderByTotal = "desc"
    table.ShowMaxYAxis = False
    table.ShowProjectTitle = False
    table.ShowMinYAxis = False
    table.PaintType = "asc"
    table.TitleStyle = "width: 250px;"

    table.DescriptionColumns = []

    table.Fields = [
        {"field": "date", "type": "period", "format": "%d.%m"},
        {"field": "projectName", "type": "row", "title": "Проект"},
        {"field": "average_handling_time", "type": "value", "round": 1},
    ]
    return table.getTable()

@op
def table_ivr_calls_dynamic_general_op(items, projects_data):
    items = _normalize(items, period_field="date", row_field="projectName")
    projects_data = _normalize(projects_data, period_field="date", row_field="projectName")
    table = PivotTable()
    table.RoundValue = 0
    table.Items = items
    table.ShowSummaryXAxis = False

    table.ShowSummaryYAxis = False
    table.ShowAverageYAxis = True
    table.ShowMinYAxis = False
    table.ShowMaxYAxis = False

    table.HideRow = False
    table.ShowId = False

    table.ShowCalculated = True
    table.AllProjectsData = projects_data
    table.ShowAvgCalculated = True

    table.ShowProjectTitle = False
    table.PaintType = "desc"
    table.ShowTotalPercentage = False
    table.TitleStyle = 'width: 250px;'

    table.DescriptionColumns = []

    table.Fields = [
        {"field": "date", "type": "period", "format": "%d.%m"},
        {"field": "projectName", "type": "row", "title": "Проект"},
        {"field": "SSR", "type": "value", "round": 1},
    ]
    html = table.getTable()
    get_dagster_logger().info(f"html = {html}")
    return html

@op
def table_requests_data_op(items):
    for r in items:
        s = str(r.get("date", "")).strip()
        if not s:
            continue
        try:
            dt = datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S")
        except ValueError:
            dt = datetime.strptime(s[:10], "%Y-%m-%d")
        r["date"] = dt.strftime("%Y-%m-%d 00:00:00")
    table = PivotTable()
    table.Items = items
    table.ShowDoD = True
    table.ShowTop20 = True
    table.topCount = 40
    table.ShowRank = True

    table.HideRow = True
    table.ShowId = False

    table.ShowSummaryXAxis = False
    table.OrderByLastDate = 'desc'
    table.PaintType = "asc"
    table.ShowTotalPercentage = False
    table.TitleStyle = "width: 250px;"
    table.Fields = [
        {"field": "date", "type": "period", "format": "%d.%m", "title": "ID"},
        {"field": "id", "type": "row"},
        {"field": "count", "type": "value"},
    ]
    table.DescriptionColumns = [
        {"field": "project_title", "title": "Проект", "style": "width: 250px;"},
        {"field": "status", "title": "Статус", "style": "width: 250px;"},
        {"field": "request_title", "title": "Запрос", "style": "width: 250px;"},
        {"field": "request_label_title", "title": "Подгруппа", "style": "width: 250px;"},
        {"field": "reason_title", "title": "Причина", "style": "width: 250px;"},
    ]
    html = table.getTable()
    return html

@op
def table_ivr_transitions_op(items):
    table = PivotTable()
    table.Items = items
    table.ShowRank = True
    table.ShowDoD = True

    table.HideRow = True
    table.ShowId = False

    table.SliceCount = False
    table.ShowTop20 = True
    table.OrderByLastDate = 'desc'
    table.PaintType = 'asc'
    table.ShowSummaryXAxis = False
    table.TitleStyle = 'width: 70px;'
    table.Fields = [
        {"field": "date", "type": "period", "format": "%d.%m", "title": "ID"},
        {"field": "id", "type": "row"},
        {"field": "count", "type": "value", "round": 1},
    ]
    table.DescriptionColumns = [
        {"field": "title", "title": "Название", "style": "width: 400px; word-wrap: break-all;"},
        {"field": "description", "title": "Описание", "style": "width: 400px; word-wrap: break-all;"},
    ]
    html = table.getTable()
    return html


@op
def table_digital_ssr_op(day_bot_projects_handled_chats, day_bot_handled_chats):
    for r in day_bot_projects_handled_chats:
        if "date" in r and r["date"]:
            r["date"] = str(r["date"])[:10]

    for r in day_bot_handled_chats:
        if "date" in r and r["date"]:
            r["date"] = str(r["date"])[:10]
    table = PivotTable()
    table.RoundValue = 1
    table.Items = day_bot_projects_handled_chats
    table.ShowSummaryYAxis = False
    table.ShowSummaryXAxis = False
    table.ShowCalculated = True
    table.ShowAvgCalculated = True
    table.AllProjectsData = day_bot_handled_chats
    table.ShowMaxYAxis = False
    table.ShowProjectTitle = False
    table.ShowMinYAxis = False

    table.HideRow = False
    table.ShowId = False

    table.OrderByAverage = "desc"
    table.PaintType = "desc"
    table.ShowTotalPercentage = False
    table.TitleStyle = "width: 250px;"

    table.DescriptionColumns = []

    table.Fields = [
        {
            "field": "date",
            "type": "period",
            "format": "%d.%m",
        },
        {"field": "project", "type": "row"},
        {"field": "bot_handled_chats_percentage", "type": "value", "round": 1},
    ]
    return table.getTable()

@op
def table_csi_all_projects_op(csi_data_by_project_name):
    for r in csi_data_by_project_name:
        if r.get("date"):
            r["date"] = str(r["date"])[:10]

    table = PivotTable()
    table.RoundValue = 1
    table.Items = csi_data_by_project_name
    table.ShowSummaryYAxis = False
    table.ShowSummaryXAxis = False
    table.ShowMaxYAxis = False
    table.ShowProjectTitle = False
    table.ShowMinYAxis = False

    table.HideRow = False
    table.ShowId = False

    table.PaintType = "desc"
    table.ShowTotalPercentage = False
    table.TitleStyle = "width: 250px;"

    table.DescriptionColumns = []

    table.Fields = [
        {
            "field": "date",
            "type": "period",
            "format": "%d.%m",
        },
        {"field": "group_title", "type": "row", "title": "Проект"},
        {"field": "customer_satisfaction_index", "type": "value", "round": 1},
    ]
    return table.getTable()

@op
def table_cea_all_projects_op(cea_data_dynamic_by_groups, cea_data_dynamic_general):
    get_dagster_logger().info(f"cea_data in table = {cea_data_dynamic_by_groups}")
    items = cea_data_dynamic_by_groups["items"]
    get_dagster_logger().info(f"items after filter= {items}")
    for r in items:
        if r.get("date"):
            r["date"] = _parse_dt_any(r["date"]).strftime("%Y-%m-%d")
        if not r.get("group_title"):
            r["group_title"] = r.get("projectName") or "Без группы"
        for k, v in list(r.items()):
            if isinstance(v, Decimal):
                r[k] = float(v)
            elif isinstance(v, float) and math.isnan(v):
                r[k] = None
    # projects (для расчётной строки)
    projects = cea_data_dynamic_general["items"]
    for r in projects:
        if r.get("date"):
            r["date"] = _parse_dt_any(r["date"]).strftime("%Y-%m-%d")
        if not r.get("group_title"):
            r["group_title"] = r.get("projectName") or "Без группы"
        for k, v in list(r.items()):
            if isinstance(v, Decimal):
                r[k] = float(v)
            elif isinstance(v, float) and math.isnan(v):
                r[k] = None

    table = PivotTable()
    table.RoundValue = 1
    table.Items = items
    table.ShowSummaryYAxis = False
    table.ShowSummaryXAxis = False
    table.ShowCalculated = True
    table.ShowAvgCalculated = True
    table.AllProjectsData = projects
    table.PaintType = "desc"

    table.HideRow = False
    table.ShowId = False

    table.OrderByTotal = "asc"
    table.ShowMaxYAxis = False
    table.ShowProjectTitle = False
    table.ShowMinYAxis = False
    table.ShowTotalPercentage = False
    table.TitleStyle = "width: 250px;"

    table.DescriptionColumns = []

    table.Fields = [
        {"field": "date", "type": "period", "format": "%d.%m"},
        {"field": "group_title", "type": "row", "title": "Проект"},
        {"field": "critical_error_accuracy", "type": "value", "round": 1},
    ]

    return table.getTable()

@op
def table_occupancy_by_position_op(aggregated_data_by_position_id, all_occupancy_date):
    for r in aggregated_data_by_position_id:
        if r.get("date"):
            r["date"] = _parse_dt_any(r["date"]).strftime("%Y-%m-%d")
        if not r.get("PositionName"):
            r["PositionName"] = "Без позиции"
        for k, v in list(r.items()):
            if isinstance(v, Decimal):
                r[k] = float(v)
            elif isinstance(v, float) and math.isnan(v):
                r[k] = None

    if isinstance(all_occupancy_date, dict):
        projects = all_occupancy_date.get("items", [])
    else:
        projects = all_occupancy_date
    for r in projects:
        if r.get("date"):
            r["date"] = _parse_dt_any(r["date"]).strftime("%Y-%m-%d")
        if not r.get("PositionName"):
            r["PositionName"] = "Без позиции"
        for k, v in list(r.items()):
            if isinstance(v, Decimal):
                r[k] = float(v)
            elif isinstance(v, float) and math.isnan(v):
                r[k] = None

    table = PivotTable()
    table.RoundValue = 1
    table.Items = aggregated_data_by_position_id
    table.ShowSummaryYAxis = False
    table.ShowSummaryXAxis = False
    table.ShowCalculated = True
    table.ShowAvgCalculated = True
    table.AllProjectsData = projects
    table.ShowId = False

    table.HideRow = False

    table.ShowMaxYAxis = False
    table.ShowProjectTitle = False
    table.ShowMinYAxis = False
    table.PaintType = "desc"
    table.ShowTotalPercentage = False
    table.TitleStyle = "width: 250px;"
    table.Fields = [
        {"field": "date", "type": "period", "format": "%d.%m"},
        {"field": "PositionName", "type": "row", "title": "Проект"},
        {"field": "occupancy", "type": "value", "round": 1},
    ]

    return table.getTable()

@op
def table_utilization_rate_by_position_op(aggregated_data_by_position_id, all_utilization_rate_date):
    for r in aggregated_data_by_position_id:
        if r.get("date"): r["date"] = _parse_dt_any(r["date"]).strftime("%Y-%m-%d")
        if not r.get("PositionName"): r["PositionName"] = "Без позиции"
        for k, v in list(r.items()):
            if isinstance(v, Decimal): r[k] = float(v)
            elif isinstance(v, float) and math.isnan(v): r[k] = None

    for r in all_utilization_rate_date:
        if r.get("date"): r["date"] = _parse_dt_any(r["date"]).strftime("%Y-%m-%d")
        if not r.get("PositionName"): r["PositionName"] = "Без позиции"
        if "utilization_rate" not in r and "occupancy" in r:
            r["utilization_rate"] = r["occupancy"]
        for k, v in list(r.items()):
            if isinstance(v, Decimal): r[k] = float(v)
            elif isinstance(v, float) and math.isnan(v): r[k] = None

    table = PivotTable()
    table.RoundValue = 1
    table.Items = aggregated_data_by_position_id
    table.ShowSummaryYAxis = False
    table.ShowSummaryXAxis = False
    table.ShowCalculated = True
    table.ShowAvgCalculated = True
    table.AllProjectsData = all_utilization_rate_date
    table.ShowId = False

    table.HideRow = False

    table.ShowMaxYAxis = False
    table.ShowProjectTitle = False
    table.ShowMinYAxis = False
    table.PaintType = "desc"
    table.ShowTotalPercentage = False
    table.TitleStyle = "width: 250px;"
    table.Fields = [
        {"field": "date", "type": "period", "format": "%d.%m"},
        {"field": "PositionName", "type": "row", "title": "Проект"},
        {"field": "utilization_rate", "type": "value", "round": 1},
    ]
    return table.getTable()

@op
def table_repeat_calls_op(graph_aggregated_data, rc_data_dynamic_main):
    for r in graph_aggregated_data:
        if r.get("date"):
            r["date"] = _parse_dt_any(r["date"]).strftime("%Y-%m-%d")
        if not r.get("projectName"):
            r["projectName"] = "Без проекта"
        for k, v in list(r.items()):
            if isinstance(v, Decimal):
                r[k] = float(v)
            elif isinstance(v, float) and math.isnan(v):
                r[k] = None


    projects = rc_data_dynamic_main.get("items", rc_data_dynamic_main)
    for r in projects:
        if r.get("date"):
            r["date"] = _parse_dt_any(r["date"]).strftime("%Y-%m-%d")
        if not r.get("projectName"):
            r["projectName"] = "Без проекта"
        for k, v in list(r.items()):
            if isinstance(v, Decimal):
                r[k] = float(v)
            elif isinstance(v, float) and math.isnan(v):
                r[k] = None

    table = PivotTable()
    table.RoundValue = 1
    table.Items = graph_aggregated_data
    table.ShowSummaryYAxis = False
    table.ShowSummaryXAxis = False
    table.ShowCalculated = True
    table.ShowAvgCalculated = True
    table.AllProjectsData = projects
    table.ShowId = False

    table.HideRow = False

    table.ShowMaxYAxis = False
    table.ShowProjectTitle = False
    table.ShowMinYAxis = False
    table.PaintType = "asc"
    table.ShowTotalPercentage = False
    table.TitleStyle = "width: 250px;"
    table.Fields = [
        {"field": "date", "type": "period", "format": "%d.%m"},
        {"field": "projectName", "type": "row", "title": "Проект"},
        {"field": "repeat_calls", "type": "value", "round": 1},
    ]
    return table.getTable()

@op
def graphic_sl_dynamic_general_op(items):
    return single_line_maker_op(items=items, y_axes_name="Процент принятых", plan=80, y_axes_max=100, field="SL")

@op
def graphic_hcr_dynamic_general_op(items):
    return single_line_maker_op(items=items, y_axes_name="Процент обработанных", plan=90, y_axes_max=100, field="ACD")

@op
def graphic_aht_dynamic_general_op(items):
    return single_line_maker_op(items=items['items'], y_axes_name="Секунды", plan=110, y_axes_max=150, y_axes_min=50, field='average_handling_time')

@op
def graphic_ssr_dynamic_general_op(items):
    return single_line_maker_op(items=items, y_axes_name="% абонентов, обработанных в IVR", plan=60, y_axes_max=100, field='SSR')

@op
def graphic_bot_ssr_general_op(items):
    return single_line_maker_op(items=items, y_axes_name="% чатов, обработанных чат-ботом", plan=10, y_axes_max=100, field='bot_handled_chats_percentage')

@op
def graphic_csi_op(items):
    return single_line_maker_op(items=items, y_axes_name='Проценты', plan=80, y_axes_max=100, field='customer_satisfaction_index')

@op
def graphic_cea_op(items):
    return single_line_maker_op(items=items['items'], y_axes_name='Проценты', plan=97, y_axes_max=100, field='critical_error_accuracy')

@op
def graphic_occupancy_op(items):
    return single_line_maker_op(items=items, graph_title='% звонков за которыми последовали повторные вызовы', y_axes_name='Проценты', plan=70, y_axes_max=100, field='occupancy')

@op
def graphic_urd_dynamic_general_op(items):
    return single_line_maker_op(items=items, y_axes_name='Проценты принятых', plan=86, y_axes_max=120, field='utilization_rate')

@op
def graphic_rc_dynamic_general_op(items):
    return single_line_maker_op(items=items['items'], graph_title='% звонков за которыми последовали повторные вызовы', y_axes_name='Проценты', plan=10, y_axes_max=50, field='repeat_calls')

@op
def graphic_sl_by_segments_op(context, main_data_dynamic_general_arpu):
    arpu_levels = ["TOP", "Very High", "High", "Middle", "Low", "New Arpu"]
    result = {}

    for arpu in arpu_levels:
        data = [r for r in main_data_dynamic_general_arpu if r.get("arpu") == arpu]
        max_val = round(max(r["total"] for r in data) / 80) * 100 if data else None

        chart = MultiAxisChart(
            y_axes_name="Количество", y_axes_min=0, y_axes_max=max_val,
            fields=[
                {"field": "date", "title": "", "type": "label"},
                {"field": "total", "title": "Количество обращений", "chartType": "line", "round": 1},
                {"field": "service_level", "title": "Service Level", "chartType": "grouped_bars"},
                {"field": "answered_calls_rate", "title": "Handled Calls Rate", "chartType": "grouped_bars"},
            ],
            items=data,
            dot=True
        )

        chart.combine(2, max_percent=300, min_percent=0);
        chart.set_colour_pack(4)

        # Отрисовка графика
        fig, ax = plt.subplots(
            figsize=(chart.width / chart.dpi, chart.height / chart.dpi),
            dpi=chart.dpi,
        )
        chart.render_graphics(ax)

        buf = io.BytesIO()
        fig.savefig(buf, format="svg", bbox_inches="tight")
        plt.close(fig)
        buf.seek(0)
        result[arpu] = base64.b64encode(buf.read()).decode("utf-8")

    return result

@op
def graphic_sl_by_languages_op(main_data_dynamic_general_language):
    def _is_nan(x):
        return isinstance(x, float) and math.isnan(x)
    languages = ["EN", "KG", "RU", "UZ"]
    charts = {}

    for lang in languages:
        data = [r.copy() for r in main_data_dynamic_general_language if r.get("language") == lang]
        # NaN -> None
        for r in data:
            for k in ("total", "service_level", "answered_calls_rate"):
                v = r.get(k)
                if _is_nan(v): r[k] = None
        totals = [r["total"] for r in data if r.get("total") is not None]
        if not totals:  # нет валидных точек — пропустим
            continue
        max_val = int(math.ceil(max(totals) / 100.0) * 100)

        chart = MultiAxisChart(
            y_axes_name="Количество", y_axes_min=0, y_axes_max=max_val,
            fields=[
                {"field": "date", "title": "", "type": "label"},
                {"field": "total", "title": "Количество обращений", "chartType": "line", "round": 1},
                {"field": "service_level", "title": "Service Level", "chartType": "grouped_bars"},
                {"field": "answered_calls_rate", "title": "Handled Calls Rate", "chartType": "grouped_bars"},
            ],
            items=data, dot=True
        )
        chart.combine(2, max_percent=300, min_percent=0);
        chart.set_colour_pack(4)
        print(f"arpu={lang}, max_val={max_val}, items={len(data)}")

        # Отрисовка графика
        fig, ax = plt.subplots(
            figsize=(chart.width / chart.dpi, chart.height / chart.dpi),
            dpi=chart.dpi,
        )
        chart.render_graphics(ax)

        buf = io.BytesIO()
        fig.savefig(buf, format="svg", bbox_inches="tight")
        plt.close(fig)
        buf.seek(0)
        charts[lang] = base64.b64encode(buf.read()).decode("utf-8")
    return charts


@op(required_resource_keys={"report_utils"})
def report_pdf_general(context, fig1, fig2, fig3, fig4, fig5, fig6, fig7, fig8, fig9, fig10, sl_general_average_data, fig11, fig12, acd_general_average_data, fig13, fig14,
                       aht_general_average_data, fig15, fig16, ssr_general_average_data, fig17, fig18, month_bot_handled_chats, fig19, fig20, csi_general_average_data, fig21, fig22, cea_current_month_general, fig23, fig24, occupancy_data_month_general, fig25, fig26, utilization_rate_date_month_general, fig27, fig28,
                       rc_general_average_data, fig29, fig30, jira_sl_tables, incidents_table,
                       arpu_figs, languages_figs):

    report = MailReport()
    report.image_mode = "file"

    APP_ROOT = Path(__file__).resolve().parents[2]
    LOGO_PATH = APP_ROOT / "utils" / "logo_png" / "logo.png"

    report.logo_file_override = str(LOGO_PATH)
    dates = context.resources.report_utils.get_utils()

    end_date = dates.EndDate

    report_date = datetime.strptime(dates.ReportDate, "%Y-%m-%d")

    p1 = ""
    p1 += report.set_basic_header(f"ОБЩАЯ ЕЖЕДНЕВНАЯ СТАТИСТИКА ИСС НА {report_date.strftime('%d.%m.%Y')}")
    p1 += report.add_block_header("Срез по основным показателям.")
    p1 += report.add_module_header("Все проекты. ГОО")
    p1 += report.add_content_to_statistic(fig1)

    p1 += report.add_content_to_statistic(
        f"""
        <div style="width:100%; text-align:center; margin:10px 0;">
          <a href="https://nod.o.kg/statistics/projects/details?date={end_date}"
          
             target="_blank" rel="noopener"
          
             style="color:#000 !important; text-decoration:underline;">
            Подробнее по всем проектам отдельно по ссылке
          </a>
        </div>
        """
    )

    p1 += report.add_module_header("Все проекты. ГПО")
    p1 += report.add_content_to_statistic(fig2)

    p2 = ""
    p2 += report.add_block_header("Профиль дня. Все проекты.")
    p2 += report.add_module_header("ГОО")
    p2 += report.add_content_to_statistic(fig3)
    p2 += report.add_module_header("ГПО")
    p2 += report.add_content_to_statistic(fig4)
    header_day_str = dates.FormattedDates[-1] if dates.FormattedDates else (_as_dt(dates.EndDate) - timedelta(days=1)).strftime(
        '%d.%m.%Y')
    p2 += report.add_block_header(f"Таблица актуальных аварийно-плановых работ на {header_day_str}")
    p2 += report.add_content_to_statistic(fig5)

    p4 = ""
    p4 += report.add_block_header("ТОП 40 запросов OMNI")
    p4 += report.add_content_to_statistic(fig6)

    p4 += report.add_content_to_statistic(
        f"""
            <div style="width:100%; text-align:center; margin:10px 0;">
              <a href="https://nod.o.kg/statistics/omni/requests?date={end_date}&type=All"
                 style="color:#000 !important; text-decoration:underline;">
                Подробнее по всем проектам по ссылке
              </a>
            </div>
            """
    )

    p4 += report.add_content_to_statistic(
        f"""
            <div style="width:100%; text-align:center; margin:10px 0;">
              <a href="https://nod.o.kg/statistics/project/requests?date={end_date}&type=All"
                 style="color:#000 !important; text-decoration:underline;">
                Подробнее отдельно по всем проектам по ссылке
              </a>
            </div>
            """
    )

    p4 += report.add_block_header("Сводка по переходам IVR. Линия - 707.")
    p4 += report.add_content_to_statistic(fig7)

    p4 += report.add_content_to_statistic(
        f"""
                <div style="width:100%; text-align:center; margin:10px 0;">
                  <a href="https://nod.o.kg/statistics/ivr/transitions?date={end_date}"
                     style="color:#000 !important; text-decoration:underline;">
                    Подробнее по другим проектам по ссылке
                  </a>
                </div>
                """
    )

    p6 = ""
    p6 += report.add_module_header("Сводка по обработанным чатам")
    p6 += report.add_content_to_statistic(fig8)
    p6 += report.add_block_header("Статистика по основным показателям")
    p6 += report.add_module_header("Конверсия вызовов. Все проекты.")
    p6 += report.add_figure(fig9)
    p6 += report.add_module_header("Все вызовы на проектные линии")
    p6 += report.add_content_to_statistic(fig10)

    p7 = ""
    p7 += report.add_module_header("Service Level. Все проекты. ", f"Цель: ≥ 80%, Факт: {sl_general_average_data} % принятых")
    p7 += report.add_figure(fig11)
    p7 += report.add_content_to_statistic(fig12)

    p8 = ""
    p8 += report.add_module_header("Handled Calls Rate. Все проекты", f"Цель: ≥ 90%, Факт: {acd_general_average_data} % принятых")
    p8 += report.add_figure(fig13)
    p8 += report.add_content_to_statistic(fig14)

    p9 = ""
    p9 += report.add_module_header("Average Handling Time. Все проекты.", f"Цель: ≤ 110 сек., Факт: {aht_general_average_data} сек.")
    p9 += report.add_figure(fig15)
    p9 += report.add_content_to_statistic(fig16)

    p10 = ""
    p10 += report.add_module_header("IVR Self Service Rate. Все проекты.", f"Цель: ≥ 60%, Факт: {ssr_general_average_data} % принятых")
    p10 += report.add_figure(fig17)
    p10 += report.add_content_to_statistic(fig18)

    p11 = ""
    p11 += report.add_module_header("Bot Self Service Rate", f"Цель: ≥ 10%, Факт: {month_bot_handled_chats} % принятых")
    p11 += report.add_figure(fig19)
    p11 += report.add_content_to_statistic(fig20)

    p12 = ""
    p12 += report.add_module_header("Customer Satisfaction Index. Линия - 707.", f"Цель: ≥ 80 %, Факт: {csi_general_average_data} %")
    p12 += report.add_figure(fig21)
    p12 += report.add_content_to_statistic(fig22)

    p13 = ""
    p13 += report.add_module_header("Critical Error Accuracy", f"Цель: ≥ 97 %, Факт: {cea_current_month_general} %")
    p13 += report.add_figure(fig23)
    p13 += report.add_content_to_statistic(fig24)

    p14 = ""
    p14 += report.add_module_header("Occupancy (Загруженность)", f"Цель: ≤ 70 %, Факт: {occupancy_data_month_general} %")
    p14 += report.add_figure(fig25)
    p14 += report.add_content_to_statistic(fig26)

    p15 = ""
    p15 += report.add_module_header("Utilization Rate", f"Цель: ≥ 86%, Факт: {utilization_rate_date_month_general} % принятых")
    p15 += report.add_figure(fig27)
    p15 += report.add_content_to_statistic(fig28)

    p16 = ""
    p16 += report.add_module_header("Repeat Calls. Все проекты.", f"Цель: ≤ 10 %, Факт: {rc_general_average_data} %")
    p16 += report.add_figure(fig29)
    p16 += report.add_content_to_statistic(fig30)

    p17 = ""
    p17 += report.add_block_header("Общая динамика SLA по закрытым задачам")
    p17 += report.add_module_header("Линия - 707/0705 700 700 ")
    p17 += report.add_figure(jira_sl_tables['Линия - 707/0705 700 700'])
    p17 += report.add_module_header("Saima - 0706 909 000 ")
    p17 += report.add_figure(jira_sl_tables["Saima - 0706 909 000"])
    p17 += report.add_module_header('O!Bank + О!Деньги (Jira NUR) - 999/0700 000 999 ')
    p17 += report.add_figure(jira_sl_tables["O!Bank + О!Деньги (Jira NUR) - 999/0700 000 999"])
    p17 += report.add_module_header("О! Терминал - 799/0702 000 799 ")
    p17 += report.add_figure(jira_sl_tables['О! Терминал - 799/0702 000 799'])

    p18 = ""
    p18 += report.add_module_header("О! Агент - 5858/0505 58 55 58 ")
    p18 += report.add_figure(jira_sl_tables['О! Агент - 5858/0505 58 55 58'])
    p18 += report.add_module_header("Акча Булак - 405/0702 000 405 ")
    p18 += report.add_figure(jira_sl_tables['Акча Булак - 405/0702 000 405'])
    p18 += report.add_module_header("О!Касса - 7878/0501 78 00 78 ")
    p18 += report.add_figure(jira_sl_tables['О!Касса - 7878/0501 78 00 78'])
    p18 += report.add_module_header("ТОП 20 созданных инцидентов. Все проекты ")
    p18 += report.add_content_to_statistic(incidents_table)

    p18 += report.add_content_to_statistic(
        f"""
                    <div style="width:100%; text-align:center; margin:10px 0;">
                      <a href="https://nod.o.kg/statistics/created/issues?date={end_date}"
                         style="color:#000 !important; text-decoration:underline;">
                        Подробнее по ссылке
                      </a>
                    </div>
                    """
    )

    p19 = ""
    p19 += report.add_block_header("Уровень сервиса по сегментам. Линия - 707.")
    p19 += report.add_module_header("ПЛАТНАЯ ЛИНИЯ - 708")
    p19 += report.add_figure(arpu_figs['TOP'])
    p19 += report.add_module_header("VERY HIGH")
    p19 += report.add_figure(arpu_figs['Very High'])
    p19 += report.add_module_header("HIGH")
    p19 += report.add_figure(arpu_figs['High'])

    p20 = ""
    p20 += report.add_module_header("MIDDLE")
    p20 += report.add_figure(arpu_figs['Middle'])
    p20 += report.add_module_header("LOW")
    p20 += report.add_figure(arpu_figs['Low'])
    p20 += report.add_module_header("NEW ARPU")
    p20 += report.add_figure(arpu_figs['New Arpu'])

    p21 = ""
    p21 += report.add_block_header("Уровень сервиса по языкам. Линия - 707.")
    p21 += report.add_module_header("EN")
    p21 += report.add_figure(languages_figs['EN'])
    p21 += report.add_module_header("KG")
    p21 += report.add_figure(languages_figs['KG'])
    p21 += report.add_module_header("RU")
    p21 += report.add_figure(languages_figs['RU'])
    p21 += report.add_module_header("UZ")
    p21 += report.add_figure(languages_figs['UZ'])

    general_tmp1 = "general_page1.pdf"
    general_tmp2 = "general_page2.pdf"
    general_tmp4 = "general_page4.pdf"
    general_tmp6 = "general_page6.pdf"
    general_tmp7 = "general_page7.pdf"
    general_tmp8 = "general_page8.pdf"
    general_tmp9 = "general_page9.pdf"
    general_tmp10 = "general_page10.pdf"
    general_tmp11 = "general_page11.pdf"
    general_tmp12 = "general_page12.pdf"
    general_tmp13 = "general_page13.pdf"
    general_tmp14 = "general_page14.pdf"
    general_tmp15 = "general_page15.pdf"
    general_tmp16 = "general_page16.pdf"
    general_tmp17 = "general_page17.pdf"
    general_tmp18 = "general_page18.pdf"
    general_tmp19 = "general_page19.pdf"
    general_tmp20 = "general_page20.pdf"
    general_tmp21 = "general_page21.pdf"


    html_to_pdf_page(html=p1, output_path=general_tmp1, height=660)
    html_to_pdf_page(html=p2, output_path=general_tmp2)
    html_to_pdf_page(html=p4, output_path=general_tmp4)
    html_to_pdf_page(html=p6, output_path=general_tmp6, height=670)
    html_to_pdf_page(html=p7, output_path=general_tmp7)
    html_to_pdf_page(html=p8, output_path=general_tmp8)
    html_to_pdf_page(html=p9, output_path=general_tmp9)
    html_to_pdf_page(html=p10, output_path=general_tmp10)
    html_to_pdf_page(html=p11, output_path=general_tmp11)
    html_to_pdf_page(html=p12, output_path=general_tmp12)
    html_to_pdf_page(html=p13, output_path=general_tmp13)
    html_to_pdf_page(html=p14, output_path=general_tmp14)
    html_to_pdf_page(html=p15, output_path=general_tmp15)
    html_to_pdf_page(html=p16, output_path=general_tmp16)
    html_to_pdf_page(html=p17, output_path=general_tmp17)
    html_to_pdf_page(html=p18, output_path=general_tmp18)
    html_to_pdf_page(html=p19, output_path=general_tmp19)
    html_to_pdf_page(html=p20, output_path=general_tmp20)
    html_to_pdf_page(html=p21, output_path=general_tmp21)

    final_pdf = "general_daily_statistics.pdf"

    merge_pdfs([general_tmp1, general_tmp2, general_tmp4, general_tmp6, general_tmp7, general_tmp8, general_tmp9, general_tmp10, general_tmp11, general_tmp12, general_tmp13, general_tmp14, general_tmp15, general_tmp16, general_tmp17, general_tmp18, general_tmp19, general_tmp20, general_tmp21], final_pdf)

    final_path = str(Path(final_pdf).resolve())
    context.log.info(f"final_pdf_path: {final_path}")
    return final_path

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

def main_table_constructor(main_data_dynamic_general, sl_general_average_data, acd_general_average_data, main_data_dynamic_general_ivr, self_service_rate_current_month_general_array,
                            goo_utilization_rate_date, goo_utilization_rate_date_month_general_rounded, occupancy_current_month_general, occupancy_data_dynamic_general_goo, handling_times_data_dynamic_general,
                            aht_general_average_data, average_ringing_time_current_month_general, average_holding_time_current_month_general,
                            cea_data_dynamic_general, critical_error_accuracy_current_month_general, csi_data_dynamic,
                            csi_general_average_data, conversion_average_data, rc_data_dynamic_main, rc_general_average_data,
                            closed_tasks_data_general_dynamic, closed_tasks_sla_month, start_date, end_date, date_list, formatted_dates, month_start, month_end):

    get_dagster_logger().info(f"start_date = {start_date}")
    get_dagster_logger().info(f"end_date = {end_date}")
    get_dagster_logger().info(f"date_list = {date_list}")
    get_dagster_logger().info(f"formatted_dates = {formatted_dates}")
    get_dagster_logger().info(f"month_start {month_start}, month_end {month_end}")
    task = TableConstructor()
    # Шапка
    task.genCellTH('Показатель', 'word-wrap: break-word; width: 250px;')
    task.genCellTH('План')
    task.multiGenCellTH(formatted_dates)
    task.genCellTH('Среднее значение', 'word-wrap: break-word; width: 160px;')
    task.genRow()

    # Титульная строка блока
    task.genRowTitle("Уровень сервиса")

    # Service Level ROW
    task.genCellTD('Service Level', "font-weight: bold;")
    task.genCellTD('≥ 80 %')

    sl_values = get_data_in_dates(
        items=main_data_dynamic_general['items'],
        dates=date_list,
        date_key='date',
        data_key='SL',
        ndigits=1
    )
    task.multiGenCellTD(
        sl_values,
        None,
        {
            "plan": 80,
            "operator": "<",
            "style": "background: #FF4D6E; ",
        }
    )

    task.genCellTD(smart_number(sl_general_average_data), "font-weight: bold;")
    task.genRow()

    # Handled Calls Rate
    task.genCellTD('Handled Calls Rate', "font-weight: bold;")
    task.genCellTD('≥ 90 %')

    acd_values = get_data_in_dates(
        items=main_data_dynamic_general['items'],
        dates=date_list,
        date_key='date',
        data_key='ACD',
        ndigits=1
    )
    task.multiGenCellTD(
        acd_values,
        None,
        {
            "plan": 90,
            "operator": "<",
            "style": "background: #FF4D6E; ",
        }
    )

    task.genCellTD(smart_number(acd_general_average_data), "font-weight: bold;")
    task.genRow()

    # IVR Self Service Rate
    task.genCellTD('IVR Self Service Rate', "font-weight: bold;")
    task.genCellTD('≥ 60 %')

    ssr_values = get_data_in_dates(
        items=main_data_dynamic_general_ivr['items'],
        dates=date_list,
        date_key='date',
        data_key='SSR',
        ndigits=1
    )

    task.multiGenCellTD(
        ssr_values,
        None,
        {
            "plan": 60,
            "operator": "<",
            "style": "background: #FF4D6E; ",
        }
    )

    task.genCellTD(smart_number(self_service_rate_current_month_general_array), "font-weight: bold;")
    task.genRow()

    # Заголовок раздела «Эффективность»
    task.genRowTitle("Эффективность")

    # Utilization Rate
    task.genCellTD('Utilization Rate', "font-weight: bold;")
    task.genCellTD('≥ 86 %.')

    utilization_values = get_data_in_dates(
        items=goo_utilization_rate_date,
        dates=date_list,
        date_key='date',
        data_key='utilization_rate',
        ndigits=1
    )

    task.multiGenCellTD(
        utilization_values,
        None,
        {
            "plan": 86,
            "operator": "<",
            "style": "background: #FF4D6E; ",
        }
    )
    task.genCellTD(smart_number(goo_utilization_rate_date_month_general_rounded), "font-weight: bold;")
    task.genRow()

    # Occupancy
    task.genCellTD('Occupancy', "font-weight: bold;")
    task.genCellTD('≥ 70 %.')

    occupancy_values = get_data_in_dates(
        items = occupancy_data_dynamic_general_goo,
        dates=date_list,
        date_key='date',
        data_key='occupancy',
        ndigits=1
    )

    task.multiGenCellTD(
        occupancy_values,
        None,
        {
            "plan": 70,
            "operator": "<",
            "style": "background: #FF4D6E; ",
        }
    )

    task.genCellTD(smart_number(occupancy_current_month_general), "font-weight: bold;")
    task.genRow()

    # Average Handling Time (AHT)
    task.genCellTD('Average Handling Time', "font-weight: bold;")
    task.genCellTD('≤ 110 сек.')

    aht_values = get_data_in_dates(
        items=handling_times_data_dynamic_general['items'],
        dates=date_list,
        date_key='date',
        data_key='average_handling_time',
        ndigits=1
    )
    aht_values = [int(float(v)) if v not in (None, "") else 0 for v in aht_values]

    task.multiGenCellTD(
        aht_values,
        None,
        {
            "plan": 110,
            "operator": ">",
            "style": "background: #FF4D6E; ",
        }
    )

    task.genCellTD(int(aht_general_average_data), "font-weight: bold;")
    task.genRow()

    # Average Ringing Time (ART)
    task.genCellTD('Average Ringing Time', "font-weight: bold;")
    task.genCellTD('≤ 3 сек.')

    art_values = get_data_in_dates(
        items=handling_times_data_dynamic_general['items'],
        dates=date_list,
        date_key='date',
        data_key='average_ringing_time',
        ndigits=1
    )
    art_values = [int(float(v)) if v not in (None, "") else 0 for v in art_values]

    task.multiGenCellTD(
        art_values,
        None,
        {
            "plan": 3,
            "operator": ">",
            "style": "background: #FF4D6E; ",
        }
    )

    task.genCellTD(int(average_ringing_time_current_month_general), "font-weight: bold;")
    task.genRow()

    # Average Holding Time
    task.genCellTD('Average Holding Time', "font-weight: bold;")
    task.genCellTD('≤ 30 сек.')

    holding_values = get_data_in_dates(
        items=handling_times_data_dynamic_general['items'],
        dates=date_list,
        date_key='date',
        data_key='average_holding_time',
        ndigits=1
    )
    holding_values = [int(float(v)) if v not in (None, "") else 0 for v in holding_values]

    task.multiGenCellTD(
        holding_values,
        None,
        {
            "plan": 30,
            "operator": ">",
            "style": "background: #FF4D6E; ",
        }
    )

    task.genCellTD(int(average_holding_time_current_month_general), "font-weight: bold;")
    task.genRow()

    # Заголовок раздела «Качество»
    task.genRowTitle("Качество")

    # Critical Error Accuracy (CEA)
    task.genCellTD('Critical Error Accuracy', "font-weight: bold;")
    task.genCellTD('≥ 97 %')

    cea_values = get_data_in_dates(
        items=cea_data_dynamic_general['items'],
        dates=date_list,
        date_key='date',
        data_key='critical_error_accuracy',
        ndigits=1
    )

    task.multiGenCellTD(
        cea_values,
        None,
        {
            "plan": 97,
            "operator": "<",
            "style": "background: #FF4D6E; ",
        }
    )

    task.genCellTD(smart_number(critical_error_accuracy_current_month_general), "font-weight: bold;")
    task.genRow()

    # Customer Satisfaction Index (CSI)
    task.genCellTD('Customer Satisfaction Index', "font-weight: bold;")
    task.genCellTD('≥ 80 %')

    csi_values = get_data_in_dates(
        items=csi_data_dynamic,  # в PHP: $CSIDataDynamic (без ['items'])
        dates=date_list,
        date_key='date',
        data_key='customer_satisfaction_index',
        ndigits=1
    )

    task.multiGenCellTD(
        csi_values,
        None,
        {
            "plan": 80,
            "operator": "<",
            "style": "background: #FF4D6E; ",
        }
    )

    task.genCellTD(smart_number(csi_general_average_data), "font-weight: bold;")
    task.genRow()

    # CSI Conversion
    task.genCellTD('Конверсия Customer Satisfaction Index', "font-weight: bold;")
    task.genCellTD('≥ 10 %')

    csi_conv_values = get_data_in_dates(
        items=csi_data_dynamic,  # тот же источник, поле 'conversion'
        dates=date_list,
        date_key='date',
        data_key='conversion',
        ndigits=1
    )

    task.multiGenCellTD(
        csi_conv_values,
        None,
        {
            "plan": 10,
            "operator": "<",
            "style": "background: #FF4D6E; ",
        }
    )

    task.genCellTD(smart_number(conversion_average_data), "font-weight: bold;")
    task.genRow()

    # Repeat Calls (RC)
    task.genCellTD('Repeat Calls', "font-weight: bold;")
    task.genCellTD('≤ 10 %')

    rc_values = get_data_in_dates(
        items=rc_data_dynamic_main['items'],
        dates=date_list,
        date_key='date',
        data_key='repeat_calls',
        ndigits=1
    )

    task.multiGenCellTD(
        rc_values,
        None,
        {
            "plan": 10,
            "operator": ">",
            "style": "background: #FF4D6E; ",
        }
    )

    task.genCellTD(smart_number(rc_general_average_data), "font-weight: bold;")
    task.genRow()

    # Task Solving Speed (SLA)
    task.genCellTD('Task Solving Speed', "font-weight: bold;")
    task.genCellTD('≥ 97 %')

    sla_values = get_data_in_dates(
        items=closed_tasks_data_general_dynamic['items'],
        dates=date_list,
        date_key='date',
        data_key='sla',
        ndigits=1
    )

    task.multiGenCellTD(
        sla_values,
        None,
        {
            "plan": 97,
            "operator": "<",
            "style": "background: #FF4D6E; ",
        }
    )

    task.genCellTD(smart_number(closed_tasks_sla_month), "font-weight: bold;")
    task.genRow()

    html = task.renderTable()
    return html


@op
def render_gpo_table_op(
    context,
    digital_self_service_rate,
    self_service_rate,
    mp_data_messengers_aggregated,
    month_average_reaction_time,
    month_average_speed_to_answer,
    gpo_utilization_rate_date,
    gpo_utilization_rate_date_month_general_rounded,
    cea_data,
    month_average_cea_data,
    csi_data,
    month_average_csi_data,
    month_average_conversion_csi_data
):
    def safe_float(val):
        try:
            return float(str(val).replace('%', '').strip())
        except (ValueError, TypeError):
            return 0.0

    def extract_float_series(items, dates, date_key, data_key, ndigits=1):
        raw = get_data_in_dates(items, dates, date_key, data_key, ndigits)
        return [safe_float(x) for x in raw]

    def ensure_list(x):
        """Превращает dict в dict['items'] если нужно, иначе возвращает x как есть."""
        if isinstance(x, dict):
            return x.get("items", [])
        return x or []

    # --- 1. Строим список дат ---
    candidate_dates = set()
    for src in [
        digital_self_service_rate,
        mp_data_messengers_aggregated,
        gpo_utilization_rate_date,
        (cea_data or {}).get("items") if isinstance(cea_data, dict) else cea_data,
        csi_data,
    ]:
        if not src:
            continue
        for it in src:
            d = it.get("date")
            if not d:
                continue
            try:
                candidate_dates.add(_normalize_to_ymd(str(d)))  # ISO YYYY-MM-DD
            except Exception:
                pass

    date_list = sorted(candidate_dates)[-8:]  # последние 8 ISO дат
    formatted_dates = [datetime.strptime(d, "%Y-%m-%d").strftime("%d.%m") for d in date_list]

    context.log.info(f"date_list (ISO): {date_list}")
    context.log.info(f"formatted_dates (UI): {formatted_dates}")

    # --- 2. Строим таблицу ---
    task = TableConstructor()
    task.genCellTH("Показатель", "word-wrap: break-word; width: 250px;")
    task.genCellTH("План")
    task.multiGenCellTH(formatted_dates)
    task.genCellTH("Среднее значение", "word-wrap: break-word; width: 160px;")
    task.genRow()

    # Digital Self Service Rate
    if digital_self_service_rate:
        task.genCellTD("Bot Self Service Rate", "font-weight: bold;")
        task.genCellTD("≥ 10%")
        task.multiGenCellTD(
            extract_float_series(ensure_list(digital_self_service_rate), date_list, "date", "self_service_rate", 1),
            None,
            {"plan": 10, "operator": "<", "style": "background: #FF4D6E; "}
        )
        task.genCellTD(round(safe_float(self_service_rate), 1), "font-weight: bold;")
        task.genRow()

    # Эффективность
    task.genRowTitle("Эффективность")

    # ART
    task.genCellTD("Average Reaction Time", "font-weight: bold;")
    task.genCellTD("≤ 60 сек.")
    task.multiGenCellTD(
        extract_float_series(ensure_list(mp_data_messengers_aggregated), date_list, "date", "average_reaction_time", 1),
        None,
        {"plan": 60, "operator": ">", "style": "background: #FF4D6E; "},
    )
    task.genCellTD(round(safe_float(month_average_reaction_time), 1), "font-weight: bold;")
    task.genRow()

    # ASTA
    task.genCellTD("Average Speed To Answer", "font-weight: bold;")
    task.genCellTD("≤ 60 сек")
    task.multiGenCellTD(
        extract_float_series(ensure_list(mp_data_messengers_aggregated), date_list, "date", "average_speed_to_answer", 1),
        None,
        {"plan": 60, "operator": ">", "style": "background: #FF4D6E; "},
    )
    task.genCellTD(round(safe_float(month_average_speed_to_answer), 1), "font-weight: bold;")
    task.genRow()

    # Utilization Rate
    task.genCellTD("Utilization Rate", "font-weight: bold;")
    task.genCellTD("≥ 86 %.")
    task.multiGenCellTD(
        extract_float_series(ensure_list(gpo_utilization_rate_date), date_list, "date", "utilization_rate", 1),
        None,
        {"plan": 86, "operator": "<", "style": "background: #FF4D6E; "},
    )
    task.genCellTD(round(safe_float(gpo_utilization_rate_date_month_general_rounded), 1), "font-weight: bold;")
    task.genRow()

    # Качество
    task.genRowTitle("Качество")

    # CEA
    task.genCellTD("Critical Error Accuracy", "font-weight: bold;")
    task.genCellTD("≥ 97 %")
    task.multiGenCellTD(
        extract_float_series(ensure_list(cea_data), date_list, "date", "critical_error_accuracy", 1),
        None,
        {"plan": 97, "operator": "<", "style": "background: #FF4D6E; "},
    )
    task.genCellTD(round(safe_float(month_average_cea_data), 1), "font-weight: bold;")
    task.genRow()

    # CSI
    task.genCellTD("Customer Satisfaction Index", "font-weight: bold;")
    task.genCellTD("≥ 80 %")
    task.multiGenCellTD(
        extract_float_series(ensure_list(csi_data), date_list, "date", "customer_satisfaction_index", 1),
        None,
        {"plan": 80, "operator": "<", "style": "background: #FF4D6E; "},
    )
    task.genCellTD(round(safe_float(month_average_csi_data), 1), "font-weight: bold;")
    task.genRow()

    # CSI Conversion
    task.genCellTD("Конверсия Customer Satisfaction Index", "font-weight: bold;")
    task.genCellTD("≥ 10 %")
    task.multiGenCellTD(
        extract_float_series(ensure_list(csi_data), date_list, "date", "conversion", 1),
        None,
        {"plan": 10, "operator": "<", "style": "background: #FF4D6E; "},
    )
    task.genCellTD(round(safe_float(month_average_conversion_csi_data), 1), "font-weight: bold;")
    task.genRow()

    return task.renderTable()

@op
def render_jira_issues_table_op(items):
    return render_jira_issues_table(items)


def table_calls_indicators(items, projects_data, field, paint_type="desc", avg=True):
    for d in projects_data:
        s = str(d.get("date", ""))[:10]
        try:
            d["date"] = datetime.strptime(s, "%d-%m-%Y").strftime("%Y-%m-%d")
        except ValueError:
            d["date"] = s


    norm_items = []
    for it in items:
        it = dict(it)  # копия, чтобы не портить оригинал

        # 1) Дата: 'YYYY-MM-DD HH:MM:SS' -> 'YYYY-MM-DD'
        s = str(it.get("date", ""))
        try:
            dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
            it["date"] = dt.strftime("%Y-%m-%d")
        except ValueError:
            it["date"] = s[:10]

        # 2) Целевое поле: Decimal -> float
        v = it.get(field)
        if isinstance(v, Decimal):
            it[field] = float(v)

        norm_items.append(it)

    # --- дальше твой код, только подставляем norm_items ---
    table = PivotTable()
    table.RoundValue = 1
    table.Items = norm_items
    table.ShowSummaryYAxis = False
    table.ShowSummaryXAxis = not avg
    table.ShowCalculated = avg
    table.ShowAvgCalculated = True
    table.AllProjectsData = projects_data
    table.OrderByAverage = 'desc'

    table.HideRow = False
    table.ShowId = False

    table.ShowMaxYAxis = False
    table.ShowProjectTitle = False
    table.ShowMinYAxis = False
    table.PaintType = paint_type
    table.ShowTotalPercentage = False
    table.TitleStyle = "width: 250px;"

    table.DescriptionColumns = []

    table.Fields = [
        {"field": "date", "type": "period", "format": "%d.%m"},
        {"field": "projectName", "type": "row", "title": "Проект"},
        {"field": field, "type": "value", "round": 1},
    ]
    return table.getTable()


def single_line_maker_op(items, y_axes_name, field, plan=None, y_axes_max=100, y_axes_min=0, graph_title=""):
    chart = MultiAxisChart(
        graph_title=graph_title,
        y_axes_name=y_axes_name,
        y_axes_min=y_axes_min,
        y_axes_max=y_axes_max,
        target=plan,
        fields=[
            {"field": "date", "title": "", "type": "label"},
            {
                "field": field,
                "title": "",
                "chartType": "line",
                "round": 1,
            },
        ],
        items=items,
        dot=True
    )

    fig, ax = plt.subplots(
        figsize=(chart.width / chart.dpi, chart.height / chart.dpi),
        dpi=chart.dpi,
    )
    chart.render_graphics(ax)

    buf = io.BytesIO()
    fig.savefig(buf, format="svg", bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")