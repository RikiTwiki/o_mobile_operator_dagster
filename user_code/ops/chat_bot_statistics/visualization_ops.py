import base64
import io
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional, Any
from typing import Dict, Any, List

from dagster import op, get_dagster_logger
from matplotlib import pyplot as plt
import matplotlib.transforms as mtransforms

from matplotlib import pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import FuncFormatter
from utils.getters.omni_data_getter import OmniDataGetter
from utils.mail.mail_report import MailReport, html_to_pdf_page, merge_pdfs
from utils.templates.charts.basic_line_graphics import BasicLineGraphics, color_weekend_xticklabels
from utils.templates.charts.multi_axis_chart import MultiAxisChart
from utils.templates.tables.pivot_table import PivotTable
import math
import numpy as np
from scipy.interpolate import make_interp_spline
from matplotlib.ticker import FuncFormatter
from utils.templates.tables.pivot_table import PivotTable
from utils.transformers.mp_report import _parse_dt_any, smart_number


@op
def graphic_sl_op(items):
    return render_Mixed_Graph(items)


@op
def render_single_line_graph_op(items):
    return render_single_line_graph(
        items,
        pointer=10,
        title="Данный график показывает процент обработанных обращений.",
        y_axes_name="Процент %",
        key="bot_handled_chats_percentage",
    )


@op
def render_basic_table_op(items):

    if not items:
        return _empty_basic_table_html()

    return render_basic_table(items, True)


@op
def render_basic_table_without_error(items):
    return render_basic_table(items, False)


@op
def multilines_graph(items):
    fields = {
        ("nur", "Nur"),
        ("bank", "O!Bank"),
        ("saima", "Saima")
    }

    color_map = {
        "nur": "#F0047F",
        "bank": "#000000",
        "saima": "#90A4AE"
    }

    result = render_basic_straight_graphs(items,
                                     fields,
                                     color_map=color_map,
                                     is_target=False,
                                     ylabel="",
                                     graph_title=""
                                     )

    return result

@op
def multilines_graph2(items):
    fields = {
        ("nur", "Nur"),
        ("bank", "O!Bank"),
        ("saima", "Saima")
    }

    color_map = {
        "nur": "#F0047F",
        "bank": "#000000",
        "saima": "#90A4AE"
    }

    result = render_basic_straight_graphs(items,
                                     fields,
                                     color_map=color_map,
                                     is_target=False,
                                     ylabel="",
                                     graph_title="",
                                     y_max = 40
                                     )

    return result

@op
def multilines_graph3(items):
    fields = {
        ("nur", "Nur"),
        ("bank", "O!Bank"),
        ("saima", "Saima")
    }

    color_map = {
        "nur": "#F0047F",
        "bank": "#000000",
        "saima": "#90A4AE"
    }

    result = render_basic_straight_graphs(items,
                                     fields,
                                     color_map=color_map,
                                     is_target=False,
                                     ylabel="",
                                     graph_title="",
                                     y_max = 10
                                     )

    return result

@op
def request_table_op(items):
    return request_table(items)


@op
def render_total_31_days_detailed_op(items, result_items):
    return render_total_31_days_detailed(items,
                                         result_items,
                                         result = 'avg',
                                         key = 'bot_handled_chats_percentage'
                                        )


@op
def render_cea_mix_graph_op(items):

    if not items:
        return _render_empty_graph_b64("Данный график показывает кол-во обработанных чатов")

    return render_cea_mix_graph(items)


@op
def render_cost_tokens_line_graph_op(items):
    return render_cost_tokens_line_graph(items)


@op
def render_tokens_line_graph_op(items):
    return render_tokens_line_graph(items)


@op
def render_cost_tokens_line_graph_nur_op(cost_usd_by_token):
    key = "nur"

    if not isinstance(cost_usd_by_token, dict) or not cost_usd_by_token.get(key):
        return _render_empty_graph_b64(graph_title="")

    fields = {
        ("input_cost", "Input Cost"),
        ("output_cost", "Output Cost"),
    }

    color_map = {
        "input_cost": "#F0047F",
        "output_cost": "#90A4AE"
    }

    graph = render_basic_straight_graphs(cost_usd_by_token[key],
                                         fields,
                                         color_map=color_map,
                                         is_target=False,
                                         ylabel="",
                                         graph_title=""
                                         )
    return graph

@op
def render_cost_tokens_line_graph_bank_op(cost_usd_by_token):
    key = "bank"

    if not isinstance(cost_usd_by_token, dict) or not cost_usd_by_token.get(key):
        return _render_empty_graph_b64(graph_title="")

    fields = {
        ("input_cost", "Input Cost"),
        ("output_cost", "Output Cost"),
    }

    color_map = {
        "input_cost": "#F0047F",
        "output_cost": "#90A4AE"
    }

    graph = render_basic_straight_graphs(cost_usd_by_token[key],
                                         fields,
                                         color_map=color_map,
                                         is_target=False,
                                         ylabel="",
                                         graph_title=""
                                         )
    return graph

@op
def render_cost_tokens_line_graph_saima_op(cost_usd_by_token):
    key = "saima"

    if not isinstance(cost_usd_by_token, dict) or not cost_usd_by_token.get(key):
        return _render_empty_graph_b64(graph_title="")

    fields = {
        ("input_cost", "Input Cost"),
        ("output_cost", "Output Cost"),
    }

    color_map = {
        "input_cost": "#F0047F",
        "output_cost": "#90A4AE"
    }

    graph = render_basic_straight_graphs(cost_usd_by_token[key],
                                         fields,
                                         color_map=color_map,
                                         is_target=False,
                                         ylabel="",
                                         graph_title=""
                                         )
    return graph

@op
def render_tokens_line_graph_nur_op(cost_tokens):
    key = "nur"

    if not isinstance(cost_tokens, dict) or not cost_tokens.get(key):
        return _render_empty_graph_b64(graph_title="")

    fields = {
        ("input_tokens", "Input Tokens"),
        ("output_tokens", "Output Tokens"),
        ("input_cached_tokens", "Input Cached Tokens"),
    }

    color_map = {
        "input_tokens": "#F0047F",
        "output_tokens": "#90A4AE",
        "input_cached_tokens": "#000000"
    }

    return render_basic_straight_graphs(
        cost_tokens[key],
        fields,
        is_target=False,
        ylabel="",
        graph_title="",
        color_map=color_map,
    )

@op
def render_tokens_line_graph_bank_op(cost_tokens):
    key = "bank"

    if not isinstance(cost_tokens, dict) or not cost_tokens.get(key):
        return _render_empty_graph_b64(graph_title="")

    fields = {
        ("input_tokens", "Input Tokens"),
        ("output_tokens", "Output Tokens"),
        ("input_cached_tokens", "Input Cached Tokens"),
    }

    color_map = {
        "input_tokens": "#F0047F",
        "output_tokens": "#90A4AE",
        "input_cached_tokens": "#000000"
    }

    graph = render_basic_straight_graphs(cost_tokens[key],
                                         fields,
                                         color_map=color_map,
                                         is_target=False,
                                         ylabel="",
                                         graph_title=""
                                         )
    return graph

@op
def render_tokens_line_graph_saima_op(cost_tokens):
    key = "saima"

    if not isinstance(cost_tokens, dict) or not cost_tokens.get(key):
        return _render_empty_graph_b64(graph_title="")

    fields = {
        ("input_tokens", "Input Tokens"),
        ("output_tokens", "Output Tokens"),
        ("input_cached_tokens", "Input Cached Tokens"),
    }

    color_map = {
        "input_tokens": "#F0047F",
        "output_tokens": "#90A4AE",
        "input_cached_tokens": "#000000"
    }

    graph = render_basic_straight_graphs(cost_tokens[key],
                                         fields,
                                         color_map=color_map,
                                         is_target=False,
                                         ylabel="",
                                         graph_title=""
                                         )
    return graph



@op(required_resource_keys={"report_utils"})
def report_pdf(context, chat_fig1, chat_fig2, chat_fig3, chat_fig4, chat_fig5, chat_fig6, chat_fig7, chat_fig8, chat_fig9, chat_fig10,
               chat_fig11, chat_fig12, chat_fig13, chat_fig14, chat_fig15, chat_fig16, chat_fig17, chat_fig18, chat_fig19, chat_fig20,
               chat_fig21, chat_fig22, chat_fig23, chat_fig24, chat_fig25, chat_fig26, chat_fig27, chat_fig28 ):

    report = MailReport()
    report.image_mode = "file"

    APP_ROOT = Path(__file__).resolve().parents[2]

    LOGO_PATH = APP_ROOT / "utils" / "logo_png" / "logo.png"

    report.logo_file_override = str(LOGO_PATH)
    dates = context.resources.report_utils.get_utils()

    end_date = dates.EndDate

    report_date = datetime.strptime(dates.ReportDate, "%Y-%m-%d")
    chat_p1 = ""
    chat_p1 += report.set_basic_header(f"ОТЧЕТ ПО ЧАТ-БОТУ НА {report_date.strftime('%d.%m.%Y')}")
    chat_p1 += report.add_block_header("Статистика по обработанным чатам")
    chat_p1 += report.add_module_header("Все проекты")
    chat_p1 += report.add_figure(chat_fig1)
    chat_p1 += report.add_module_header("Процент автоматизации ")
    chat_p1 += report.add_figure(chat_fig2)
    chat_p1+= report.add_content_to_statistic(chat_fig3)

    chat_p2 = ""
    chat_p2 += report.add_module_header("Saima")
    chat_p2 += report.add_figure(chat_fig4)
    chat_p2 += report.add_module_header("O!Bank + О!Деньги")
    chat_p2 += report.add_figure(chat_fig5)
    chat_p2 += report.add_module_header("AkchaBulak")
    chat_p2 += report.add_figure(chat_fig6)
    chat_p2 += report.add_module_header("Nur")
    chat_p2 += report.add_figure(chat_fig7)
    chat_p2 += report.add_module_header("O!Brand")
    chat_p2 += report.add_figure(chat_fig8)

    chat_p3 = ""
    chat_p3 += report.add_block_header("BEA")
    chat_p3 += report.add_module_header("Динамика оцененных чатов за 31 день")
    chat_p3 += report.add_module_child_header("Все проекты")
    chat_p3 += report.add_figure(chat_fig9)
    chat_p3 += report.add_module_child_header("Nur")
    chat_p3 += report.add_figure(chat_fig10)
    chat_p3 += report.add_module_child_header("O!Bank")
    chat_p3 += report.add_figure(chat_fig11)
    chat_p3 += report.add_module_child_header("Saima")
    chat_p3 += report.add_figure(chat_fig12)

    chat_p4 = ""
    chat_p4 += report.add_module_header("Запросы и причины чатов с ошибками ")
    chat_p4 += report.add_module_child_header("Nur")
    chat_p4 += report.add_content_to_statistic(chat_fig13)
    chat_p4 += report.add_module_child_header("Saima")
    chat_p4 += report.add_content_to_statistic(chat_fig14)
    chat_p4 += report.add_module_child_header("Bank")
    chat_p4 += report.add_content_to_statistic(chat_fig15)
    chat_p4 += report.add_module_header("Чаты с замечаниями")
    chat_p4 += report.add_content_to_statistic(chat_fig16)

    chat_p5 = ""
    chat_p5 += report.add_block_header("COST")
    chat_p5 += report.add_module_header("Расходы по всем проектам (USD)")
    chat_p5 += report.add_figure(chat_fig17)
    chat_p5 += report.add_module_header("Стоимость затрат за один диалог закрытый чат-ботом (центы)")
    chat_p5 += report.add_figure(chat_fig18)
    chat_p5 += report.add_module_header("Стоимость затрат за один ответ чат-бота (центы)")
    chat_p5 += report.add_figure(chat_fig19)

    chat_p6 = ""
    chat_p6 += report.add_module_header("Nur")
    chat_p6 += report.add_module_child_header("Расходы USD")
    chat_p6 += report.add_figure(chat_fig20)
    chat_p6 += report.add_module_child_header("Расход токенов (млн)")
    chat_p6 += report.add_figure(chat_fig21)

    chat_p7 = ""
    chat_p7 += report.add_module_header("Bank")
    chat_p7 += report.add_module_child_header("Расходы USD")
    chat_p7 += report.add_figure(chat_fig22)
    chat_p7 += report.add_module_child_header("Расход токенов (млн)")
    chat_p7 += report.add_figure(chat_fig23)

    chat_p8 = ""
    chat_p8 += report.add_module_header("Saima")
    chat_p8 += report.add_module_child_header("Расходы USD")
    chat_p8 += report.add_figure(chat_fig24)
    chat_p8 += report.add_module_child_header("Расход токенов (млн)")
    chat_p8 += report.add_figure(chat_fig25)

    chat_p9 = ""
    chat_p9 += report.add_block_header("ТОП40 запросов OMNI в чатах, закрытых специалистами")
    chat_p9 += report.add_module_header("Saima")
    chat_p9 += report.add_content_to_statistic(chat_fig26)

    chat_p9 += report.add_content_to_statistic(
        f"""
                        <div style="width:100%; text-align:center; margin:10px 0;">
                          <a href="https://nod.o.kg/statistics/project/requests?date={end_date}&type=Saima"
                             style="color:#000 !important; text-decoration:underline;">
                            Подробнее по ссылке
                          </a>
                        </div>
                        """
    )

    chat_p10 = ""
    chat_p10 += report.add_module_header("O!Bank + О!Деньги")
    chat_p10 += report.add_content_to_statistic(chat_fig27)

    chat_p10 += report.add_content_to_statistic(
        f"""
                        <div style="width:100%; text-align:center; margin:10px 0;">
                          <a href="https://nod.o.kg/statistics/project/requests?date={end_date}&type=Bank"
                             style="color:#000 !important; text-decoration:underline;">
                            Подробнее по ссылке
                          </a>
                        </div>
                        """
    )

    chat_p11 = ""
    chat_p11 += report.add_module_header("Nur")
    chat_p11 += report.add_content_to_statistic(chat_fig28)

    chat_p11 += report.add_content_to_statistic(
        f"""
                        <div style="width:100%; text-align:center; margin:10px 0;">
                          <a href="https://nod.o.kg/statistics/project/requests?date={end_date}&type=Nur"
                             style="color:#000 !important; text-decoration:underline;">
                            Подробнее по ссылке
                          </a>
                        </div>
                        """
    )

    chat_tmp1 = "chat_page1.pdf"
    chat_tmp2 = "chat_page2.pdf"
    chat_tmp3 = "chat_page3.pdf"
    chat_tmp4 = "chat_page4.pdf"
    chat_tmp5 = "chat_page5.pdf"
    chat_tmp6 = "chat_page6.pdf"
    chat_tmp7 = "chat_page7.pdf"
    chat_tmp8 = "chat_page8.pdf"
    chat_tmp9 = "chat_page9.pdf"
    chat_tmp10 = "chat_page10.pdf"
    chat_tmp11 = "chat_page11.pdf"


    html_to_pdf_page(html = chat_p1, output_path=chat_tmp1, height=1050)
    html_to_pdf_page(html=chat_p2, output_path=chat_tmp2)
    html_to_pdf_page(html=chat_p3, output_path=chat_tmp3)
    html_to_pdf_page(html=chat_p4, output_path=chat_tmp4)
    html_to_pdf_page(html=chat_p5, output_path=chat_tmp5)
    html_to_pdf_page(html=chat_p6, output_path=chat_tmp6)
    html_to_pdf_page(html=chat_p7, output_path=chat_tmp7)
    html_to_pdf_page(html=chat_p8, output_path=chat_tmp8)
    html_to_pdf_page(html=chat_p9, output_path=chat_tmp9)
    html_to_pdf_page(html=chat_p10, output_path=chat_tmp10)
    html_to_pdf_page(html=chat_p11, output_path=chat_tmp11)

    final_pdf = "chat_bot_daily_statistics.pdf"

    merge_pdfs([chat_tmp1, chat_tmp2, chat_tmp3, chat_tmp4, chat_tmp5, chat_tmp6, chat_tmp7, chat_tmp8, chat_tmp9, chat_tmp10, chat_tmp11], final_pdf)

    final_path = str(Path(final_pdf).resolve())
    context.log.info(f"final_pdf_path: {final_path}")
    return final_path



def render_Mixed_Graph(items) -> str:
    pointer = 1
    day_data_items = items

    for item in day_data_items:
        item['date_dt'] = datetime.strptime(item['date'], '%Y-%m-%d %H:%M:%S')
    day_data_items.sort(key=lambda x: x['date_dt'])
    for item in day_data_items:
        del item['date_dt']

    # Вычисляем максимальное значение
    #max_val = 100
    #for item in day_data_items:
    #    item['bot_handled_chats'] = item.get('bot_handled_chats', 0)
    #    if item.get('total_chats', 0) > max_val:
    #        max_val = item['total_chats']

    # Параметры можно менять здесь
    chart = MultiAxisChart(
        graph_title="Данный график показывает кол-во обработанных обращений.",
        x_axes_name="",
        y_axes_name="Кол-во",
        stack=True,
        show_tooltips=False,
        height=400,
        pointer=pointer,
        y_axes_min=0,
        y_axes_max=None,
        fields=[
            {"field": "date", "title": "", "type": "label"},
            {"field": "operator_answers_count", "title": "Кол-во чатов обработанных специалистом", "chartType": "bar",
             "round": 1},
                {"field": "bot_answers_count", "title": "Кол-во чатов обработанных чат-ботом", "chartType": "bar",
             "round": 1},
            {"field": "total_chats", "title": "Кол-во поступивших чатов", "chartType": "line", "round": 1},
        ],
        items=day_data_items,
        dot=True
    )
    chart.set_colour_pack(5)
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



def render_single_line_graph(    items: List[Dict[str, Any]],
    pointer: int,
    title: str,
    y_axes_name: str,
    key: str,  # ← хотите точки? передай True
):
    # 1) сортируем по дате
    for it in items:
        it["_dt"] = _parse_dt_any(it.get("date", ""))
    items.sort(key=lambda x: x["_dt"])
    for it in items:
        del it["_dt"]

    # 2) максимум (без падения на '69.0')
    max_val = 4
    for it in items:
        val = smart_number(it.get(key, 0))
        if val > max_val:
            max_val = val
    y_max_php_like = round(float(max_val) * 1.2)

    # 3) собираем объект графика
    chart = MultiAxisChart(
        graph_title=title,
        x_axes_name="",
        y_axes_name=y_axes_name,
        y_axes_min=0,
        y_axes_max=100,
        pointer=pointer,
        show_tooltips=False,
        height=400,
        fields=[
            {"field": "date", "title": "", "type": "label"},
            {"field": key, "title": "", "chartType": "line", "round": 1},
        ],
        items=items,
        dot=True,   # если добавил флаг в __init__
    )
    chart.set_colour_pack(5)
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

def _empty_basic_table_html() -> str:
    return (
        "<div class='pivot-table empty'>"
        "<table>"
        "<thead><tr>"
        "<th>Дата</th><th>ID</th><th>Count</th>"
        "<th>Проект</th><th>Тип ошибки</th><th>Запрос</th><th>Причина</th>"
        "</tr></thead>"
        "<tbody>"
        "<tr><td colspan='7' style='text-align:center;opacity:.7'>Нет данных за период</td></tr>"
        "</tbody>"
        "</table>"
        "</div>"
    )

def render_basic_table(items: List[Dict[str, Any]], error: bool):

    if not items:
        return _empty_basic_table_html()

    result: List[Dict[str, Any]] = []
    unique_ids: Dict[str, int] = {}

    for it in items:
        date_norm = _parse_dt_any(it["date"]).strftime("%Y-%m-%d")

        if error:
            key = f"{it['project']}-{it['mistake']}-{it['requesttitle']}-{it['reasontitle']}"
            if key not in unique_ids:
                unique_ids[key] = len(unique_ids) + 1
            result.append({
                "id": unique_ids[key],
                "date": date_norm,
                "project": it["project"],
                "mistake": it["mistake"],
                "requesttitle": it["requesttitle"],
                "reasontitle": it["reasontitle"],
                "count": it["daily_control_count"],
            })
        else:
            key = f"{it['project']}-{it['mistake']}"
            if key not in unique_ids:
                unique_ids[key] = len(unique_ids) + 1
            result.append({
                "id": unique_ids[key],
                "date": date_norm,
                "project": it["project"],
                "mistake": it["mistake"],
                "count": it["daily_control_count"],
            })

    table = PivotTable()
    table.Items = result
    table.OrderByTotal = 'desc'
    table.ShowTop20 = True
    table.ShowDoD = True
    table.topCount = 40
    table.ShowId = False
    table.ShowRank = True
    table.ShowSummaryXAxis = False
    table.PaintType = "asc"
    table.ShowTotalPercentage = False
    table.Fields = [
        {"field": "date", "type": "period", "format": "%d.%m", "title": "ID"},
        {"field": "id", "type": "row"},
        {"field": "count", "type": "value"},
    ]
    table.DescriptionColumns = [
        {"field": "project", "title": "Проект", "style": "width: 250px;"},
        {"field": "mistake", "title": "Тип ошибки", "style": "width: 250px;"},
    ]
    if error:
        table.DescriptionColumns.extend([
            {"field": "requesttitle", "title": "Запрос", "style": "width: 250px;"},
            {"field": "reasontitle", "title": "Причина", "style": "width: 250px;"},
        ])
    return table.getTable()


def render_cost_line_graph(items, fields=["nur", "bank", "saima"], y_max_override=None, base=50):
    max_val = 0

    # Округляем и пересчитываем максимум для всех полей
    for item in items:
        for field in fields:
            if field in item:
                item[field] = round(item[field], 1)
                if item[field] > max_val:
                    max_val = item[field]

    # Применяем логику округления, аналогичную второму примеру
    max_val = round(max_val / 10) * 10

    # Если y_max_override передан, используем его
    if y_max_override:
        max_val = y_max_override
        print(max_val)

    # Проверяем, если минимум слишком низкий, ставим значение ниже на 5 или 10 единиц
    y_min = max(0, max_val - 10)

    graph = MultiAxisChart(
        graph_title="Расходы",
        y_axes_name="",
        x_axes_name="",
        y_axes_min=y_min,  # обновленный минимум
        y_axes_max=max_val,  # обновленный максимум
        show_tooltips=False,
        height=400,
        fields=[
            {"field": "date",  "title": "", "type": "label", "stacked": False},
            {"field": "nur",   "title": "Nur", "chartType": "line", "round": 1},
            {"field": "bank",  "title": "Bank", "chartType": "line", "round": 1},
            {"field": "saima", "title": "Saima", "chartType": "line", "round": 1},
        ],
        items=items,
        dot=True,
        base = base
    )
    graph.set_colour_pack(5)

    fig, ax = plt.subplots(
        figsize=(graph.width / graph.dpi, graph.height / graph.dpi),
        dpi=graph.dpi,
    )
    graph.render_graphics(ax)
    buf = io.BytesIO()
    fig.savefig(buf, format="svg", bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")

def _empty_request_pivot_html() -> str:
    return (
        "<div class='pivot-table empty'>"
        "<table>"
        "<thead><tr>"
        "<th>Дата</th><th>ID</th><th>Count</th>"
        "<th>Проект</th><th>Статус</th><th>Запрос</th>"
        "<th>Подгруппа</th><th>Причина</th>"
        "</tr></thead>"
        "<tbody>"
        "<tr><td colspan='8' style='text-align:center;opacity:.7'>Нет данных за период</td></tr>"
        "</tbody>"
        "</table>"
        "</div>"
    )

def request_table(items):
    # 1) Пусто? Возвращаем пустую таблицу сразу
    if not items:
        return _empty_request_pivot_html()

    result = []
    unique_ids = {}

    for item in items:
        get = (lambda k: item.get(k)) if isinstance(item, dict) else (lambda k: getattr(item, k, None))

        # Пропускаем строки без обязательных полей
        try:
            date_raw = get("date")
            if not date_raw:
                continue
            dt = _parse_dt_any(date_raw)
            date_str = dt.strftime("%Y-%m-%d")
        except Exception:
            # Если дата кривая — просто пропускаем строку
            continue

        key = f"{get('request_title')}-{get('request_label_title')}-{get('reason_title')}-{get('project_title')}-{get('request_status_title')}"
        if key not in unique_ids:
            unique_ids[key] = len(unique_ids) + 1

        project_title = get("project_title")
        project_title = "Линия 707" if project_title == "General" else project_title

        result.append({
            "id": unique_ids[key],
            "date": date_str,
            "project_title": project_title,
            "request_title": get("request_title"),
            "reason_title": get("reason_title"),
            "request_label_title": get("request_label_title"),
            "count": get("count"),
            "status": get("request_status_title"),
        })

    # 2) После нормализации ничего не осталось — тоже пустая таблица
    if not result:
        return _empty_request_pivot_html()

    # 3) Иначе строим сводную
    table = PivotTable()
    table.Items = result
    table.OrderByTotal = 'desc'
    table.ShowDoD = True
    table.ShowTop20 = True
    table.topCount = 40
    table.ShowId = False

    table.HideRow = True

    table.ShowRank = True
    table.ShowSummaryXAxis = False

    table.ShowProjectTitle = False

    table.PaintType = "asc"
    table.ShowTotalPercentage = False
    table.TitleStyle = "width: 250px;"

    table.Fields = [
        {"field": "date", "type": "period", "format": "%d.%m", "title": "ID"},
        {"field": "id",   "type": "row"},
        {"field": "count","type": "value"},
    ]

    table.DescriptionColumns = [
        {"field": "project_title",       "title": "Проект",    "style": "width: 250px;"},
        {"field": "status",              "title": "Статус",    "style": "width: 250px;"},
        {"field": "request_title",       "title": "Запрос",    "style": "width: 250px;"},
        {"field": "request_label_title", "title": "Подгруппа", "style": "width: 250px;"},
        {"field": "reason_title",        "title": "Причина",   "style": "width: 250px;"},
    ]

    return table.getTable()


def date_only(value: Any) -> Optional[str]:
    """
    Convert a datetime-like value to 'YYYY-MM-DD' string.
    Returns None for None/empty input or raises ValueError if unparsable.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")

    s = str(value).strip()
    if not s:
        return None

    # common formats
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except Exception:
            pass

    # fallback: take first 10 chars if it looks like YYYY-MM-DD
    if len(s) >= 10:
        maybe = s[:10]
        try:
            datetime.strptime(maybe, "%Y-%m-%d")
            return maybe
        except Exception:
            pass

    raise ValueError(f"Unrecognized date format: {value!r}")


def render_total_31_days_detailed(items,
                                  result_items,
                                  result,
                                  key) -> str:
    """
    Нормализует поле 'date' в обоих списках в формат 'YYYY-MM-DD' и вызывает PivotTable.
    Пропускает записи с нераспознанной датой.
    """
    def safe_date(v: Any) -> Optional[str]:
        try:
            return date_only(v)
        except Exception:
            return None

    def normalize_list(src: List[Any], date_field: str = 'date', row_field: str = 'project') -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for item in src:
            # приводим к dict если нужно
            if isinstance(item, dict):
                d = item.copy()
            else:
                try:
                    d = dict(item)
                except Exception:
                    # неконвертируемый элемент — пропускаем
                    continue

            # нормализуем дату
            d[date_field] = safe_date(d.get(date_field))
            if d[date_field] is None:
                # пропускаем записи без корректной даты
                continue

            # если нет project, попробуем взять project_id или project_name
            if not d.get(row_field):
                for alt in ('project', 'project_name', 'name', 'project_id'):
                    if alt in d and d.get(alt) is not None:
                        d[row_field] = d[alt]
                        break

            out.append(d)
        return out

    # Нормализуем входные списки
    processed_items = normalize_list(items, date_field='date', row_field='project')
    processed_results = normalize_list(result_items, date_field='date', row_field='project')

    # Если нет данных — вернём пустую таблицу (или можно бросить ошибку)
    if not processed_items:
        return "<!-- No data after date normalization -->"

    jira_sla_pivot = PivotTable()
    jira_sla_pivot.RoundValue = 1
    jira_sla_pivot.RowTitleStyle = "font-weight: bold; text-align: left"
    jira_sla_pivot.PaintType = 'asc'
    jira_sla_pivot.OrderByTotal = 'desc'
    jira_sla_pivot.Items = processed_items

    # Подставляем AllProjectsData только если нужны рассчитанные значения
    if result == 'sum':
        jira_sla_pivot.Fields = [
            {'field': 'date', 'type': 'period', 'format': '%d.%m'},
            {'field': 'project', 'type': 'row'},
            {'field': key, 'type': 'value'},
        ]
    elif result == 'avg':
        jira_sla_pivot.ShowSummaryYAxis = False
        jira_sla_pivot.ShowSummaryXAxis = False
        jira_sla_pivot.ShowAvgCalculated = True
        jira_sla_pivot.ShowCalculated = True
        jira_sla_pivot.PaintType = 'asc'
        jira_sla_pivot.AllProjectsData = processed_results
        jira_sla_pivot.AdvancedAverageMultiplication = 1
        jira_sla_pivot.Fields = [
            {'field': 'date', 'type': 'period', 'format': '%d.%m'},
            {'field': 'project', 'type': 'row'},
            {'field': key, 'type': 'value'},
        ]
    else:
        # на случай других режимов — ставим базовые поля
        jira_sla_pivot.Fields = [
            {'field': 'date', 'type': 'period', 'format': '%d.%m'},
            {'field': 'project', 'type': 'row'},
            {'field': key, 'type': 'value'},
        ]

    return jira_sla_pivot.getTable()

def _render_empty_graph_b64(title: str = "", width: int = 1920, height: int = 700) -> str:
    dpi = 100
    fig, ax = plt.subplots(figsize=(width / dpi, height / dpi), dpi=dpi)
    fig.patch.set_facecolor('#f8f9fa')
    ax.set_facecolor('#ffffff')
    if title:
        ax.set_title(title, fontsize=12, fontweight="bold", loc="left", pad=18)
    ax.text(0.5, 0.5, 'Нет данных', ha='center', va='center',
            fontsize=12, color='#4a5568', fontweight='bold', transform=ax.transAxes)
    ax.set_xticks([]); ax.set_yticks([])
    for s in ax.spines.values():
        s.set_visible(False)
    buf = io.BytesIO()
    fig.savefig(buf, format="svg", bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")

def render_cea_mix_graph(items: List[Dict[str, Any]]) -> str:
    """
    Left Y axis: percentages 0..100 (BEA line)
    Right Y axis: counts (bars: all_listened, mistakes_count)
    Returns base64 PNG.
    """
    # --- Подготовка данных (форматирование дат и приведение к int) ---
    dates = []
    all_listened = []
    mistakes = []
    bea = []

    if not items:
        return _render_empty_graph_b64("Данный график показывает кол-во обработанных чатов")

    def _fmt_date(raw):
        if isinstance(raw, datetime):
            return raw.strftime("%d.%m")
        s = str(raw)
        fmt = "%Y-%m-%d %H:%M:%S" if len(s) > 10 else "%Y-%m-%d"
        return datetime.strptime(s, fmt).strftime("%d.%m")

    for row in items:
        dates.append(_fmt_date(row.get("date")))
        all_listened.append(int(row.get("all_listened", 0) or 0))
        mistakes.append(int(row.get("mistakes_count", 0) or 0))
        bea.append(float(row.get("critical_error_accuracy", 0.0) or 0.0))

    n = len(dates)
    if n == 0:
        raise ValueError("No items provided")
    x = np.arange(n)

    # --- Figure size (увеличенная высота) ---
    fig_w = 1920 / 100.0
    fig_h = 700 / 100.0
    fig, ax = plt.subplots(figsize=(fig_w, fig_h), dpi=100)

    # --- Bars on RIGHT axis (counts) ---
    ax2 = ax.twinx()

    # Grouped bars: all_listened (шире) + mistakes_count (узкий)
    total_width = 0.75
    # базовая ширина, затем делим: большая колонка и маленькая
    w_big = total_width * 0.6
    w_small = total_width * 0.6
    # offsets to center group on x
    offsets = np.array([-(w_big + w_small) / 2 + w_big / 2,
                        -(w_big + w_small) / 2 + w_big + w_small / 2])

    color_all = "#F0047F"   # pink
    color_mist = "#C8C8C8"  # light gray

    bars_all = ax2.bar(x + offsets[0], all_listened, width=w_big, label="Всего оценено", color=color_all, zorder=2)
    bars_mist = ax2.bar(x + offsets[1], mistakes, width=w_small, label="Ошибки", color=color_mist, zorder=2)

    # подписи над столбцами — используем координаты ax2 (counts)
    max_count = max(all_listened) if all_listened else 0
    max_count = max(max_count, max(mistakes) if mistakes else 0)
    label_gap_counts = max(max_count * 0.02, 1.0)
    for bar, val in zip(bars_all, all_listened):
        if val > 0:
            ax2.text(bar.get_x() + bar.get_width() / 2, val + label_gap_counts,
                     f"{int(val):,}".replace(",", " "), ha="center", va="bottom",
                     fontsize=10, fontweight="bold", color="black", zorder=5)
    for bar, val in zip(bars_mist, mistakes):
        if val > 0:
            ax2.text(bar.get_x() + bar.get_width() / 2, val + label_gap_counts,
                     f"{int(val):,}".replace(",", " "), ha="center", va="bottom",
                     fontsize=10, fontweight="bold", color="black", zorder=5)

    # X ticks/labels on bottom axis (ax)
    ax.set_xticks(x)
    ax.set_xticklabels(dates, rotation=45, fontsize=10)

    # выделим weekend жирным шрифтом, если возможно распарсить дату
    for i, row in enumerate(items):
        raw = row.get("date")
        try:
            if isinstance(raw, datetime):
                dt = raw
            else:
                s = str(raw)
                fmt = "%Y-%m-%d %H:%M:%S" if len(s) > 10 else "%Y-%m-%d"
                dt = datetime.strptime(s, fmt)
            if dt.weekday() >= 5:
                lab = ax.get_xticklabels()[i]
                lab.set_fontweight("bold")
        except Exception:
            pass

    # --- LINE on LEFT axis (percent BEA) ---
    bea_arr = np.array(bea, dtype=float)
    line_max = 0.0
    # сглаживание если >=4 точек
    if n >= 4:
        x_smooth = np.linspace(x.min(), x.max(), 2000)
        spline = make_interp_spline(x, bea_arr, k=3)
        y_smooth = spline(x_smooth)
        # Сначала рисуем столбцы (на ax2), затем линию на ax - так линия выше при большем zorder
        ax.plot(x_smooth, y_smooth, color="#333333", linewidth=2.4, zorder=11, label="BEA")
        ax.scatter(x, bea_arr, marker='o', s=42, edgecolors='white', linewidths=0.8,
                   facecolors="#333333", zorder=12)
        line_max = float(y_smooth.max())
    else:
        ax.plot(x, bea_arr, color="#333333", linewidth=2.4, zorder=11, label="BEA")
        ax.scatter(x, bea_arr, marker='o', s=42, edgecolors='white', linewidths=0.8,
                   facecolors="#333333", zorder=12)
        line_max = float(bea_arr.max() if bea_arr.size else 100.0)

    # подписи значений рядом с точками линии (на левой оси)
    offset_pct = max(line_max * 0.015, 0.5)
    for xi, yi in zip(x, bea_arr):
        ax.text(xi, yi + offset_pct, f"{yi:.0f}", ha="center", va="bottom",
                fontsize=10, fontweight="bold", color="#333333", zorder=13)

    # --- Оформление осей и легенда ---
    # левая ось: проценты 0..100
    ax.set_ylim(0, 110)
    ax.set_ylabel("Проценты", fontsize=12)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda y, pos: f"{int(round(y)):d}"))

    # правая ось: количество — подбираем верх с паддингом
    PAD_PCT = 0.25
    PAD_MIN = 10
    top_auto = max_count * (1 + PAD_PCT)
    top_raw = max(top_auto, max_count + PAD_MIN)
    # округлим до ближайших 50
    BASE = 50
    top_counts = math.ceil(top_raw / BASE) * BASE if top_raw > 0 else 50
    ax2.set_ylim(0, top_counts)
    ax2.set_ylabel("Количество", fontsize=12)
    # форматирование правой оси: округление до сотен
    ax2.yaxis.set_major_formatter(FuncFormatter(lambda y, pos: f"{round(y, -2):.0f}"))
    ax2.grid(False)

    # общая сетка по левой оси (или можно по ax2) — оставим по левому процентному рисунку
    ax.grid(axis="y", linestyle="--", linewidth=0.5)
    ax.set_axisbelow(True)

    # легенда: собираем из обеих осей и поднимаем над линией
    handles, labels = ax.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    handles.extend(h2)
    labels.extend(l2)
    legend = ax.legend(handles, labels, loc="upper center", bbox_to_anchor=(0.5, 1.12),
                       ncol=max(1, len(labels)), frameon=False, fontsize=10)
    for text in legend.get_texts():
        text.set_color("#4A4A4A")

    # Заголовок и отступы
    ax.set_title("Данный график показывает кол-во обработанных чатов",
                 fontsize=12, fontweight="bold", loc="left", pad=18)

    for spine in ax.spines.values():
        spine.set_visible(False)
    for spine in ax2.spines.values():
        spine.set_visible(False)

    ax2.get_yaxis().set_visible(False)
    ax2.spines["right"].set_visible(False)
    ax2.set_ylabel("")

    plt.tight_layout()
    plt.subplots_adjust(top=0.90)

    # Сохраняем в PNG и возвращаем base64
    buf = io.BytesIO()
    fig.savefig(buf, format="svg", bbox_inches="tight")
    buf.seek(0)
    b64 = base64.b64encode(buf.read()).decode("utf-8")
    return b64


def render_cost_tokens_line_graph(items: List[Dict[str, Any]]) -> str:
    max_val = 40

    # округляем и пересчитываем максимум
    for item in items:
        item["input_cost"] = round(item.get("input_cost", 0), 1)
        item["output_cost"] = round(item.get("output_cost", 0), 1)

        if item["input_cost"] > max_val:
            max_val = item["input_cost"]

    max_val /= 10
    max_val = round(max_val) * 10 + 10

    graph = MultiAxisChart(
    graph_title="Расходы по токенам",
    y_axes_name="",
    x_axes_name="",
    y_axes_min=0,
    y_axes_max=max_val,
    show_tooltips=False,
    height=400,
    fields=[
        {"field": "date",        "title": "",                        "type": "label", "stacked": False},
        {"field": "input_cost",  "title": "Расходы на input токены",  "chartType": "line", "round": 1},
        {"field": "output_cost", "title": "Расходы на output токены", "chartType": "line", "round": 1},
    ],
    items=items,
    dot=True
)

    graph.set_colour_pack(5)

    fig, ax = plt.subplots(
        figsize=(graph.width / graph.dpi, graph.height / graph.dpi),
        dpi=graph.dpi,
    )
    graph.render_graphics(ax)

    buf = io.BytesIO()
    fig.savefig(buf, format="svg", bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")


def render_tokens_line_graph(items: List[Dict[str, Any]]) -> str:
    # --- сортируем по дате (если даты в формате YYYY-MM-DD или datetime) ---
    def _to_dt(x):
        if isinstance(x, datetime):
            return x
        s = str(x)
        fmt = "%Y-%m-%d %H:%M:%S" if len(s) > 10 else "%Y-%m-%d"
        return datetime.strptime(s, fmt)

    items = sorted(items, key=lambda r: _to_dt(r.get("date")))

    # --- нормализуем/округляем и вычисляем максимум по всем сериям ---
    max_val = 50
    for row in items:
        row["input_tokens"] = round(float(row.get("input_tokens", 0) or 0), 1)
        row["output_tokens"] = round(float(row.get("output_tokens", 0) or 0), 1)
        row["input_cached_tokens"] = round(float(row.get("input_cached_tokens", 0) or 0), 1)

        max_val = max(max_val,
                      row["input_tokens"],
                      row["output_tokens"],
                      row["input_cached_tokens"])

    # приводим к вашему прежнему способу расчёта YAxesMax
    max_val /= 10.0
    max_val = round(max_val) * 10 + 10

    graph = MultiAxisChart(
        graph_title="Расходы",
        y_axes_name="",
        x_axes_name="",
        y_axes_min=0,
        y_axes_max=max_val,
        show_tooltips=False,
        height=400,
        fields=[
            {"field": "date", "title": "", "type": "label"},
            {"field": "input_tokens", "title": "Input tokens", "chartType": "line", "round": 1},
            {"field": "output_tokens", "title": "Output tokens", "chartType": "line", "round": 1},
            {"field": "input_cached_tokens", "title": "Input cached tokens", "chartType": "line", "round": 1},
        ],
        items=items,
        dot=True
    )
    graph.set_colour_pack(5)

    fig, ax = plt.subplots(
        figsize=(graph.width / graph.dpi, graph.height / graph.dpi),
        dpi=graph.dpi,
    )
    graph.render_graphics(ax)

    buf = io.BytesIO()
    fig.savefig(buf, format="svg", bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    svg_bytes = buf.read()
    buf.close()
    b64 = base64.b64encode(svg_bytes).decode("utf-8")

    return b64

def _render_empty_graph_b64(graph_title: str = "", width: int = 1920, height: int = 500) -> str:
    """Возвращает пустой SVG-график (base64) с подписью 'Нет данных'."""
    dpi = 100
    fig, ax = plt.subplots(figsize=(width / dpi, height / dpi), dpi=dpi)
    fig.patch.set_facecolor('#f8f9fa')
    ax.set_facecolor('#ffffff')
    ax.text(0.5, 0.5, 'Нет данных', ha='center', va='center',
            fontsize=12, color='#4a5568', fontweight='bold', transform=ax.transAxes)
    # заголовок сверху слева
    if graph_title:
        ax.text(0.0, 1.08, graph_title, transform=ax.transAxes, ha='left',
                fontsize=11, color='#2d3748', fontweight="bold", wrap=True)
    # чистая область
    ax.set_xticks([]); ax.set_yticks([])
    for s in ax.spines.values():
        s.set_visible(False)
    buf = io.BytesIO()
    fig.savefig(buf, format="svg", bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")

def render_basic_straight_graphs(
    items,
    fields,
    target = 60,
    is_target = True,
    color_map = None,
    default_color = '#000',
    label_interval = 1,
    ylabel = "",
    graph_title = "",
    y_max = None
):

    if not items:
        return _render_empty_graph_b64(graph_title)

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

    parsed_dates: List[datetime] = []
    parse_error = False
    for lbl in graphic.labels:
        s = (lbl or '').strip().split()[0] if lbl else ''
        dt = None
        if not s:
            parse_error = True
            break
        for fmt in ('%d.%m.%Y', '%d.%m', '%d.%m.%y', '%Y-%m-%d'):
            try:
                dt = datetime.strptime(s, fmt)
                if fmt == '%d.%m':
                    dt = dt.replace(year=datetime.now().year)
                break
            except Exception:
                continue
        if dt is None:
            try:
                dt = datetime.fromisoformat(s)
            except Exception:
                dt = None
        if dt is None:
            parse_error = True
            break
        parsed_dates.append(dt)

    if len(parsed_dates) >= 3:
        xticks = parsed_dates[1:-1]
    else:
        xticks = parsed_dates

    ax.set_xticks(xticks)
    x_labels = [dt.strftime('%d.%m') for dt in xticks]
    ax.set_xticklabels(x_labels, rotation=0, fontsize=10)

    if xticks:
        if len(xticks) >= 2:
            step = xticks[1] - xticks[0]
            left_pad  = xticks[0] - step * 1.7
            right_pad = xticks[-1] + step * 1.7
        else:
            left_pad  = xticks[0] - timedelta(hours=12)
            right_pad = xticks[0] + timedelta(hours=12)
        ax.set_xlim(left_pad, right_pad)

    ax.xaxis.set_major_formatter(mdates.DateFormatter('%d.%m'))
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))

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

        # 👇 приоритет за пользовательским Y_max
        if y_max is not None:
            y_top = float(y_max)

        ax.set_ylim(min_val, y_top)

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
