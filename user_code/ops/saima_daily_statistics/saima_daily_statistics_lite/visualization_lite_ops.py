import math
from datetime import datetime
from pathlib import Path

from dagster import op, get_dagster_logger

# Константы ID проектов (нужны для тех. работ и графиков)
from ops.saima_daily_statistics.process_ops import omni_project_ids

# Базовые инструменты генерации PDF и отчетов
from utils.mail.mail_report import MailReport, html_to_pdf_page, merge_pdfs

# Инструменты для сборки таблиц и отрисовки данных
from utils.report_data_collector import (
    get_task_title,           # Создает заголовок таблицы показателей
    get_task_row_lite,             # Добавляет строку в таблицу показателей (Голос)
    render_pst_daily_profile_table_lite,
    render_pst_daily_profile_table_new,# Отрисовка таблицы (Текст)
    render_technical_works_table,    # Отрисовка тех. работ
    gen_chart_line,
    gen_task_row_new
)

from utils.templates.tables.table_constructor_indents import TableConstructorIndents

from utils.report_data_collector import render_pst_daily_profile_table_lite

from utils.report_data_collector import get_task_title_lite

@op(required_resource_keys={'report_utils'})
def voice_indicators_saima_lite_op(context, main_data_dynamic, service_level_month_general,
                                   handled_calls_month_general, handling_times_data,
                                   average_handling_time_month_rounded_value, csi_data,
                                   csi_month_value, ost_rc_data, ost_rc_month_value,
                                   repeat_calls_by_ivr, average_repeat_ivr_pct):

    dates = context.resources.report_utils.get_utils()
    date_list = dates.Dates
    formatted_dates = dates.FormattedDates

    task = get_task_title_lite(formatted_dates)

    voice_indicators = [
        ['Service Level', '%', main_data_dynamic, 'SL', 80, '<', service_level_month_general],
        ['Handled Calls Rate', '%', main_data_dynamic, 'ACD', 90, '<', handled_calls_month_general],
        ['Average Handling Time', 'сек.', handling_times_data['items'], 'average_handling_time', 30, '>',
         average_handling_time_month_rounded_value],
        ['Customer Satisfaction Index', '%', csi_data, 'customer_satisfaction_index', 80, '<', csi_month_value],
        ['Repeat Calls по операторам', '%', ost_rc_data['items'], 'repeat_calls', 10, '>', ost_rc_month_value],
        ['Repeat Calls по IVR', '%', repeat_calls_by_ivr, 'repeat_calls', 10, '>', average_repeat_ivr_pct],
    ]

    for indicator in voice_indicators:

        get_task_row_lite(task, *indicator, date_list=date_list)

    return task.renderTable()


@op(required_resource_keys={'report_utils'})
def text_indicators_lite_op(context, mp_data_messengers_aggregated, month_average_reaction_time,
                            month_average_speed_to_answer, pst_csi_data, pst_csi_month_value):
    dates = context.resources.report_utils.get_utils()
    date_list = dates.Dates
    formatted_dates = dates.FormattedDates

    text_indicators = [
        ['Average Reaction Time', 'сек.', mp_data_messengers_aggregated, 'average_reaction_time', 60, '>',
         month_average_reaction_time],
        ['Average Speed To Answer', 'сек.', mp_data_messengers_aggregated, 'average_speed_to_answer', 60, '>',
         month_average_speed_to_answer],
        ['Customer Satisfaction Index', '%', pst_csi_data, 'customer_satisfaction_index', 80, '<', pst_csi_month_value],
    ]

    task = render_pst_daily_profile_table_lite(text_indicators, formatted_dates, date_list)

    # МЕНЯЕМ ШАПКУ ТАБЛИЦЫ ТЕКСТА

    return task.renderTable()

@op(required_resource_keys={'report_utils'})
def voice_indicators_general_saima_lite_op(context, data_omni_saima):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    voice_indicators_general = [
        ['Количество обращений, закрытых на уровне 1 линии', data_omni_saima['items'], 'closed_by_first_line',
         math.floor(data_omni_saima['avg_closed_by_first_line'] + 0.5)],
        ['Количество обращений, переданных другим отделам', data_omni_saima['items'], 'jira_issue', math.floor(data_omni_saima['avg_jira_issue'] + 0.5)],
        ['Доля обращений, закрытых на уровне 1 линии', data_omni_saima['items'], 'portion', math.floor(data_omni_saima['avg_portion'] + 0.5)],
        ['Доля обращений, переданных другим отделам', data_omni_saima['items'], 'proportion_of_escalated',
         math.floor(data_omni_saima['avg_proportion_of_escalated'] + 0.5)],
    ]

    task = render_pst_daily_profile_table_new(voice_indicators_general, formatted_dates, date_list)
    return task.renderTable()

@op(required_resource_keys={"report_utils"})
def report_pdf_saima_lite(context, fig1, table1, fig2, fig3, fig4, fig7,fig8, graph, service_level_month, **kwargs):
    report = MailReport()
    report.image_mode = "file"
    LOGO_PATH = "/opt/dagster/app/utils/logo_png/logo.png"
    report.logo_file_override = str(LOGO_PATH)

    dates = context.resources.report_utils.get_utils()
    report_date = datetime.strptime(dates.ReportDate, "%Y-%m-%d")

    # СТРАНИЦА 1
    p1 = ""
    p1 += report.set_basic_header(f"SAIMA | ЕЖЕДНЕВНАЯ СТАТИСТИКА КЦ НА {report_date.strftime('%d.%m.%Y')}.")
    p1 += report.add_block_header("Заявки в Jira")
    p1 += report.add_module_header("Общая динамика SLA по закрытым задачам")
    p1 += report.add_figure(fig1)

    # СТРАНИЦА 2
    p2 = """
    <style>
        table, th, td, div, span, p {
            font-family: 'Arial', 'Helvetica', sans-serif !important;
        }
    </style>
    """
    p2 += report.add_module_header("Количество заявок CUSTOMER SERVICES и SLA за последние 30 дней")
    p2 += f'<div style="font-family: Arial, sans-serif;">{report.add_content_to_statistic(table1)}</div>'
    # СТРАНИЦА 3
    p3 = """
    <style>
        table { font-family: Arial, sans-serif; }
        th { background-color: #2d3748; color: white; padding: 8px; }
        td { padding: 8px; border-bottom: 1px solid #e2e8f0; }
        
        tr:nth-child(even) { background-color: #f2f2f2 !important; }
        
        * { font-family: 'Arial', sans-serif !important; }
    </style>
    """
    p3 += report.add_block_header("Срез по основным показателям КЦ")
    p3 += report.add_module_header("Голос")
    p3 += report.add_content_to_statistic(fig2)
    p3 += report.add_module_header("Текст")
    p3 += report.add_content_to_statistic(fig3)
    p3 += report.add_module_header("Общее")
    p3 += report.add_content_to_statistic(fig4)

    # СТРАНИЦА 4
    p4 = ""
    p4 += report.add_block_header(f"Таблица актуальных аварийно-плановых работ на {report_date.strftime('%d.%m.%Y')}")
    p4 += report.add_content_to_statistic(fig7)
    # Заглдесь просто добавит пустую область, если список пуст
    p4 += report.add_block_header("ТОП40 причин обращений")
    p4 += report.add_content_to_statistic(fig8)

    # СТРАНИЦА 5
    p5 = ""
    p5 += report.add_block_header('Статистика по основным показателям')
    p5 += report.add_module_header('Общая динамика звонков за 31 день')
    p5 += report.add_figure(graph)

    tmp_files = ["p1.pdf", "p2.pdf", "p3.pdf", "p4.pdf", "p5.pdf"]
    html_pages = [p1, p2, p3, p4, p5]

    for i, html in enumerate(html_pages):
        h = 600 if i == 0 else 900 # Немного увеличим высоту для надежности
        html_to_pdf_page(html=html, output_path=tmp_files[i], height=h)

    final_pdf = "saima_daily_statistics_lite.pdf"
    merge_pdfs(tmp_files, final_pdf)

    return str(Path(final_pdf).resolve())