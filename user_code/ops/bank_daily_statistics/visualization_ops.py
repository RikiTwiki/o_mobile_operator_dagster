import os
from datetime import datetime, timedelta
from pathlib import Path


from dagster import op, get_dagster_logger

from resources import ReportUtils
from utils.mail.mail_report import MailReport, html_to_pdf_page, merge_pdfs
from utils.report_data_collector import get_task_title, gen_task_row, render_pst_daily_profile_table, \
    render_day_profile, render_pst_daily_profile, render_pst_handled_time_table, render_technical_works_table, \
    render_client_requests, render_chat_data, gen_chart_line, render_pivot_table, add_chart_module, \
    render_ivr_transitions, render_jira_graph

connection = 'naumen3'
ost_position_ids = [36]
pst_position_ids = [46]
mp_project_ids = [5, 11, 15]
omni_project_ids = [5, 11, 15]
ost_time_table_view_id = [9]
pst_time_table_view_id = [18]
promt_ids = [148, 147, 146, 145, 144, 143]
groups = ["Money", "Bank", "O!Bank"]
channels = ['messaging-platform', 'cp']
callers = ['8008', '996770988330', '555988330', '996770988330', '0706988330', '9999', '8833', '999', '996700000999']
jira_projects = {
            "Money": {
                "title": "О! Деньги - 999/0700 000 999",
                "id": 8,
                "topic_view_id": 7,
                "jira_project_id": "10304",
                "jira_project_key": ["PAYM", "APP"],
            }
        }

project_replacements = {
            "O! Money - WhatsApp": "O!Деньги - WhatsApp",
            "O! Money - Telegram": "O!Деньги - Telegram",
            "Halyk Bank - WhatsApp": "Халык - WhatsApp",
            "Halyk Bank - halykbank.kg": "Халык - halykbank.kg",
            "Halyk Bank - Telegram": "Халык - Telegram",
        }



@op(required_resource_keys={'report_utils'})
def voice_indicators_title_op(context, main_data_dynamic, service_level_current_month_general, handled_calls_current_month_general,
                           handling_times_data, average_handling_time_current_month_rounded_value, average_ringing_time_current_month_rounded_value, average_holding_time_current_month_rounded_value,
                           cea_data, cea_current_month_value, csi_data, csi_current_month_value, csi_conversion_current_month_value,
                           ost_ur_data, ost_ur_month_data_rounded, ost_occupancy_data, ost_occupancy_month_data_rounded, ost_rc_data,
                           ost_rc_current_month_value):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    task = get_task_title(formatted_dates)

    voice_indicators = [
        {
            "title": "Service Level",
            "label": "≥ 80 %",
            "items": main_data_dynamic,
            "field": "SL",
            "target": 80,
            "cmp": "<",
            "current": service_level_current_month_general,
        },
        {
            "title": "Handled Calls Rate (Процент принятых вызовов)",
            "label": "≥ 90 %",
            "items": main_data_dynamic,
            "field": "ACD",
            "target": 90,
            "cmp": "<",
            "current": handled_calls_current_month_general,
        },
        {
            "title": "Average Handling Time",
            "label": "≤ 110 сек.",
            "items": handling_times_data["items"],
            "field": "average_handling_time",
            "target": 110,
            "cmp": ">",
            "current": average_handling_time_current_month_rounded_value,
        },
        {
            "title": "Average Ringing Time",
            "label": "≤ 3 сек.",
            "items": handling_times_data["items"],
            "field": "average_ringing_time",
            "target": 3,
            "cmp": ">",
            "current": average_ringing_time_current_month_rounded_value,
        },
        {
            "title": "Average Holding Time",
            "label": "≤ 30 сек.",
            "items": handling_times_data["items"],
            "field": "average_holding_time",
            "target": 30,
            "cmp": ">",
            "current": average_holding_time_current_month_rounded_value,
        },
        {
            "title": "Critical Error Accuracy",
            "label": "≥ 97 %.",
            "items": cea_data["items"],
            "field": "critical_error_accuracy",
            "target": 97,
            "cmp": "<",
            "current": cea_current_month_value,
        },
        {
            "title": "Customer Satisfaction Index",
            "label": "≥ 80 %.",
            "items": csi_data,
            "field": "customer_satisfaction_index",
            "target": 80,
            "cmp": "<",
            "current": csi_current_month_value,
        },
        {
            "title": "Конверсия Customer Satisfaction Index",
            "label": "≥ 10 %.",
            "items": csi_data,
            "field": "conversion",
            "target": 10,
            "cmp": "<",
            "current": csi_conversion_current_month_value,
        },
        {
            "title": "Utilization Rate",
            "label": "≥ 86 %.",
            "items": ost_ur_data,
            "field": "utilization_rate",
            "target": 86,
            "cmp": "<",
            "current": ost_ur_month_data_rounded,
        },
        {
            "title": "Occupancy",
            "label": "≥ 70 %.",
            "items": ost_occupancy_data["items"],
            "field": "occupancy",
            "target": 70,
            "cmp": "<",
            "current": ost_occupancy_month_data_rounded,
        },
        {
            "title": "Repeat Calls",
            "label": "≤ 10 %.",
            "items": ost_rc_data["items"],
            "field": "repeat_calls",
            "target": 10,
            "cmp": ">",
            "current": ost_rc_current_month_value,
        },
    ]

    context.log.info(f"date_list: {date_list}")

    for ind in voice_indicators:
        gen_task_row(
            task=task,
            indicator=ind["title"],
            plan=ind["label"],
            items=ind["items"],
            abbreviation=ind["field"],
            plan_val=ind["target"],
            operator=ind["cmp"],
            month_val=ind["current"],
            date_list=date_list,
        )

    return task.renderTable()

@op(required_resource_keys={'report_utils'})
def pst_text_indicators_op(
    context,
    mp_data_messengers_aggregated,
    month_average_reaction_time,
    month_average_speed_to_answer,
    pst_ur_data,
    pst_ur_month_data_rounded,
    pst_cea_data,
    pst_cea_current_month_value,
    pst_csi_data,
    pst_csi_current_month_value,
    pst_csi_conversion_current_month_value
):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    text_indicators = [
        ['Average Reaction Time', '≤ 60 сек.', mp_data_messengers_aggregated, 'average_reaction_time', 60, '>', round(month_average_reaction_time, 1)],
        ['Average Speed To Answer', '≤ 60 сек.', mp_data_messengers_aggregated, 'average_speed_to_answer', 60, '>', round(month_average_speed_to_answer, 1)],
        ['Utilization Rate', '≥ 86 %.', pst_ur_data, 'utilization_rate', 86, '>', round(pst_ur_month_data_rounded, 1)],
        ['Critical Error Accuracy', '≥ 97 %.', pst_cea_data['items'], 'critical_error_accuracy', 97, '<', round(pst_cea_current_month_value, 1)],
        ['Customer Satisfaction Index', '≥ 80 %.', pst_csi_data, 'customer_satisfaction_index', 80, '<', round(pst_csi_current_month_value, 1)],
        ['Конверсия Customer Satisfaction Index', '≥ 10 %.', pst_csi_data, 'conversion', 10, '<', round(pst_csi_conversion_current_month_value, 1)],
    ]

    context.log.info(date_list)

    task = render_pst_daily_profile_table(text_indicators, formatted_dates, date_list)
    return task.renderTable()

@op
def handled_time_table_op(detailed_daily_data):
    return render_day_profile(detailed_daily_data)

@op
def table_handled_time_op(pst_detailed_daily_data):
    return render_pst_handled_time_table(pst_detailed_daily_data)

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'source_cp', 'report_utils'})
def table_technical_works_op1(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    return render_technical_works_table(conn=context.resources.source_cp, period='hour', project_ids=omni_project_ids, key=None, start_date=start_date, end_date=end_date, report_date=report_date)

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'source_cp', 'report_utils'})
def client_requests_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    return render_client_requests(conn=context.resources.source_cp, project_ids=omni_project_ids, date_list=date_list, end_date=end_date)

@op
def render_ivr_transitions_op(items):
    return render_ivr_transitions(items)


@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def mp_chat_pivot_table_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    return render_chat_data(conn=context.resources.source_dwh, period='day', project_ids=mp_project_ids, pst_position_ids=pst_position_ids, project_replacements=project_replacements, start_date=start_date, end_date=end_date)

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1'})
def graphic_calls_dynamic_general_op1(context, items):
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
    return gen_chart_line(items, fields, color_map=color_map, is_target=False, ylabel='Звонки', graph_title='Данный график показывает конверсию по звонкам.')

@op
def table_calls_dynamic_general_op1(context, main_data_dynamic_filtered):
    return render_pivot_table(main_data_dynamic_filtered['ivr'], val='ivr_total')

@op
def graphic_sl_bank_op(context, main_data_dynamic):
    return add_chart_module(target=80, graph_title='', axes_name='Процент принятых', items=main_data_dynamic, field='SL')

@op
def table_sl_bank_op(context, main_data_dynamic_filtered, main_data_dynamic):
    return render_pivot_table(main_data_dynamic_filtered['custom'], val="SL", sum=main_data_dynamic)

@op
def graphic_acd_bank_op(context, main_data_dynamic):
    return add_chart_module(target=90, graph_title='', axes_name='Процент принятых', items=main_data_dynamic, field='ACD')

@op
def table_acd_bank_op(context, main_data_dynamic_filtered, main_data_dynamic):
    return render_pivot_table(main_data_dynamic_filtered['custom'], val="ACD", sum=main_data_dynamic)

@op
def graphic_aht_bank_op(context, merged_aht):
    return add_chart_module(target=130, graph_title='', axes_name='Секунды', items=merged_aht['items'], field='average_handling_time', axes_max=210, axes_min=50)

@op
def table_aht_bank_op(context, aht_by_project, merged_aht):
    return render_pivot_table(aht_by_project, val="average_handling_time", sum=merged_aht['items'])

@op
def graphic_ivr_ssr_bank_op(context, main_data_dynamic):
    return add_chart_module(target=60, graph_title='', axes_name='% звонков, обработанных в IVR', items=main_data_dynamic, field='SSR')

@op
def table_ivr_ssr_bank_op(context, main_data_dynamic_filtered, main_data_dynamic):
    return render_pivot_table(main_data_dynamic_filtered['ivr'], val="SSR", sum=main_data_dynamic)

@op
def graphic_rc_bank_op(context, merged_ost_rc_data):
    return add_chart_module(target=10, graph_title='', axes_name='% звонков за которыми последовали повторные вызовы', items=merged_ost_rc_data['items'], field='repeat_calls', axes_max=50)

@op
def table_rc_bank_op(context, merged_ost_rc_data_filtered, merged_ost_rc_data):
    return render_pivot_table(merged_ost_rc_data_filtered, val="repeat_calls", sum=merged_ost_rc_data['items'])

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'source_dwh_prod', 'report_utils'})
def jira_data_graph_bank_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    tables = {}
    for project in jira_projects:
        jira_data_graph = render_jira_graph(conn=context.resources.source_dwh_prod,  project=jira_projects[project], start_date=start_date, end_date=end_date)
        tables[project] = jira_data_graph
    return tables

@op(required_resource_keys={"report_utils"})
def bank_daily_statistics_report(context, fig1, fig2, fig3, fig4, fig5, fig6, fig7, fig8, fig9, fig10, fig11, fig12, fig13, fig14, fig15, fig16, fig17, fig18, fig19, fig20, fig21):
    report = MailReport()
    report.image_mode = "file"

    APP_ROOT = Path(__file__).resolve().parents[2]
    LOGO_PATH = APP_ROOT / "utils" / "logo_png" / "logo.png"

    report.logo_file_override = str(LOGO_PATH)
    dates = context.resources.report_utils.get_utils()
    report_date = datetime.strptime(dates.ReportDate, "%Y-%m-%d")

    p1 = ""
    p1 += report.set_basic_header(f"ОБЩАЯ ЕЖЕДНЕВНАЯ СТАТИСТИКА О!BANK НА {report_date.strftime('%d.%m.%Y')}")
    p1 += report.add_block_header("Срез по основным показателям.")
    p1 += report.add_module_header("Голос")
    p1 += report.add_content_to_statistic(fig1)
    p1 += report.add_module_header("Текст")
    p1 += report.add_content_to_statistic(fig2)

    p2 = ""
    p2 += report.add_block_header("Профиль дня.")
    p2 += report.add_module_header("Голос")
    p2 += report.add_content_to_statistic(fig3)
    p2 += report.add_module_header("Текст")
    p2 += report.add_content_to_statistic(fig4)
    p2 += report.add_block_header("Таблица актуальных аварийно-плановых работ")
    p2 += report.add_content_to_statistic(fig5)

    p3 = ""
    p3 += report.add_block_header("ТОП40 причин обращений")
    p3 += report.add_content_to_statistic(fig6)
    p3 += report.add_block_header("Сводка по переходам IVR")
    p3 += report.add_module_header("O!Bank + O!Деньги ")
    p3 += report.add_content_to_statistic(fig7)
    p3 += report.add_block_header("Сводка по проектам CP")
    p3 += report.add_content_to_statistic(fig8)

    p4 = ""
    p4 += report.add_block_header("Статистика по основным показателям")
    p4 += report.add_module_header("Общая динамика звонков за 31 день")
    p4 += report.add_figure(fig9)
    p4 += report.add_content_to_statistic(fig10)

    p5 = ""
    p5 += report.add_module_header("Service Level.")
    p5 += report.add_figure(fig11)
    p5 += report.add_content_to_statistic(fig12)

    p6 = ""
    p6 += report.add_module_header("Handled Calls Rate.")
    p6 += report.add_figure(fig13)
    p6 += report.add_content_to_statistic(fig14)

    p7 = ""
    p7 += report.add_module_header("Average Handling Time.")
    p7 += report.add_figure(fig15)
    p7 += report.add_content_to_statistic(fig16)

    p8 = ""
    p8 += report.add_module_header("IVR Self Service Rate.")
    p8 += report.add_figure(fig17)
    p8 += report.add_content_to_statistic(fig18)

    p9 = ""
    p9 += report.add_module_header("Repeat Calls. Все проекты.")
    p9 += report.add_figure(fig19)
    p9 += report.add_content_to_statistic(fig20)
    p9 += report.add_block_header("Общая динамика SLA по закрытым задачам " )
    p9 += report.add_module_header('О! Деньги - 999/0700 000 999 ')
    p9 += report.add_figure(fig21["Money"])

    # Генерация PDF из HTML
    bank_tmp1 = "bank_page1.pdf"
    bank_tmp2 = "bank_page2.pdf"
    bank_tmp3 = "bank_page3.pdf"
    bank_tmp4 = "bank_page4.pdf"
    bank_tmp5 = "bank_page5.pdf"
    bank_tmp6 = "bank_page6.pdf"
    bank_tmp7 = "bank_page7.pdf"
    bank_tmp8 = "bank_page8.pdf"
    bank_tmp9 = "bank_page9.pdf"

    html_to_pdf_page(html=p1, output_path=bank_tmp1, height=500)
    html_to_pdf_page(html=p2, output_path=bank_tmp2)
    html_to_pdf_page(html=p3, output_path=bank_tmp3)
    html_to_pdf_page(html=p4, output_path=bank_tmp4, height=660)
    html_to_pdf_page(html=p5, output_path=bank_tmp5)
    html_to_pdf_page(html=p6, output_path=bank_tmp6)
    html_to_pdf_page(html=p7, output_path=bank_tmp7)
    html_to_pdf_page(html=p8, output_path=bank_tmp8)
    html_to_pdf_page(html=p9, output_path=bank_tmp9)

    final_pdf = "bank_daily_statistics.pdf"

    # Слияние во ФИНАЛЬНЫЙ абсолютный путь
    merge_pdfs([bank_tmp1, bank_tmp2, bank_tmp3, bank_tmp4, bank_tmp5, bank_tmp6, bank_tmp7, bank_tmp8, bank_tmp9], final_pdf)

    final_path = str(Path(final_pdf).resolve())
    context.log.info(f"final_pdf_path: {final_path}")
    return final_path