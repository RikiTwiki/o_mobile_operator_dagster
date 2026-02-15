import json
import os
from datetime import datetime, timedelta
from pathlib import Path

from dagster import op, get_dagster_logger

from ops.saima_daily_statistics.process_ops import omni_project_ids, mp_project_ids, pst_position_ids
from resources import ReportUtils
from utils.mail.mail_report import MailReport, html_to_pdf_page, merge_pdfs
from utils.report_data_collector import render_jira_graph, get_jira_sla_data_new, render_jira_issues_table_new, \
    render_jira_issues_status_table_new, get_task_title, get_task_row, render_pst_daily_profile_table, \
    render_pst_daily_profile_table_new, render_day_profile, render_pst_handled_time_table, render_technical_works_table, \
    render_client_requests, render_ivr_transitions, render_chat_data, gen_chart_line, add_chart_module
from utils.templates.tables.table_constructor_indents import TableConstructorIndents

jira_project = {
    "title": "Saima - 0706 909 000",
    "id": 12,
    "topic_view_id": 1,
    "jira_project_id": "11005",
    "jira_project_key": ["MS", "CSA", "MA"],
}

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'source_dwh_prod', 'report_utils'})
def jira_data_graph_saima_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    return render_jira_graph(conn=context.resources.source_dwh_prod,
                             project=jira_project, start_date=start_date, end_date=end_date)

# @op(required_resource_keys={'source_dwh', 'source_jira_nur', 'source_jira_saima', 'source_dwh_prod'})
@op(required_resource_keys={'source_dwh_prod', 'report_utils'})
def sl_saima_jira_cs_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    data = get_jira_sla_data_new(dwh_conn=context.resources.source_dwh_prod, jira_nur=context.resources.source_dwh_prod, # Поменять на context.resources.source_jira_nur
                                 jira_saima=context.resources.source_dwh_prod, # Поменять на context.resources.source_jira_saima
                                 jira="jirasd_saima", project_keys=['CS'], start_date=start_date, end_date=end_date)

    data = json.loads(data)

    excluded_types = [
        "Тех лист: На исправление",
        "Task",
        "На включение xDSL",
        "ТП Сотрудник 550 + О!TV от Сайма Телеком",
        "Перерасчет в биллинге",
        "ОКП: Добавление адреса в справочник",
        "Платное подключение",
        "Смена технологии",
        "Аварийные работы",
        "Ак-Ордо / Ала-Тоо 3: Проблема",
        "Не работает беспроводной интернет",
    ]

    filtered = [
        item for item in data
        if item.get("parent_issue_type") not in excluded_types
    ]

    if isinstance(filtered, dict):
        filtered = list(filtered.values())

    grouped = {}

    for item in filtered:
        key = item["parent_issue_type"]

        if key not in grouped:
            grouped[key] = {
                "parent_issue_type": key,
                "total_count": [],
                "subtask_type": [],
                "subtasks_count": [],
                "open_subtasks_count": [],
                "closed_subtasks_count": [],
                "ontime_closed_subtasks_count": [],
                "sla_plan_days": [],
                "sla_fact_days": [],
            }

        grouped[key]["total_count"].append(item["total_count"])
        grouped[key]["subtask_type"].append(item["subtask_type"])
        grouped[key]["subtasks_count"].append(item["subtasks_count"])
        grouped[key]["open_subtasks_count"].append(item["open_subtasks_count"])
        grouped[key]["closed_subtasks_count"].append(item["closed_subtasks_count"])
        grouped[key]["ontime_closed_subtasks_count"].append(item["ontime_closed_subtasks_count"])
        grouped[key]["sla_plan_days"].append(item["sla_plan_days"])
        grouped[key]["sla_fact_days"].append(item["sla_fact_days"])

    cs_sla = list(grouped.values())

    for g in cs_sla:
        rows = list(zip(
            g["total_count"],
            g["subtasks_count"],
            g["subtask_type"],
            g["open_subtasks_count"],
            g["closed_subtasks_count"],
            g["ontime_closed_subtasks_count"],
            g["sla_plan_days"],
            g["sla_fact_days"],
        ))
        rows.sort(key=lambda r: (r[0], r[1]), reverse=True)

        (
            g["total_count"],
            g["subtasks_count"],
            g["subtask_type"],
            g["open_subtasks_count"],
            g["closed_subtasks_count"],
            g["ontime_closed_subtasks_count"],
            g["sla_plan_days"],
            g["sla_fact_days"],
        ) = map(list, zip(*rows)) if rows else ([], [], [], [], [], [], [], [])

    def first_total(g):
        lst = g.get("total_count", [])
        return lst[0] if lst else 0

    cs_sla.sort(key=first_total, reverse=True)

    task = TableConstructorIndents()

    headers = [
        "Основная заявка",
        "Подзадача",
        "Всего заявок",
        "Поступило подзадач",
        "Незакрыто подзадач",
        "Закрыто подзадач",
        "Закрыто в срок",
        "Плановый SLA в Jira (часы)",
        "Фактический SLA в Jira (часы)",
        "SLA, %",
        "SLA, который сообщаем абоненту по основной заявке",
    ]

    for h in headers:
        task.genCellTH(h)
    task.genRow()

    for groupedRow in cs_sla:
        task.genDataRowGrouped(groupedRow)

    html = task.renderTable()
    return html

@op(required_resource_keys={'source_dwh_prod', 'report_utils'})
def sl_saima_jira_ms_ma_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    data = get_jira_sla_data_new(
        dwh_conn=context.resources.source_dwh_prod,
        jira_nur=context.resources.source_dwh_prod, # Поменять на context.resources.source_jira_nur
        jira_saima=context.resources.source_dwh_prod, # Поменять на context.resources.source_jira_saima
        jira="jirasd_saima",
        project_keys=["MS", "MA"],
        start_date=start_date,
        end_date=end_date,
    )
    data = json.loads(data) or []
    if isinstance(data, dict):
        data = data.get("items", list(data.values()))
    data = [x for x in data if isinstance(x, dict)]

    excluded_types = {
        "Тех лист: На исправление", "Task", "На включение xDSL",
        "ТП Сотрудник 550 + О!TV от Сайма Телеком", "Перерасчет в биллинге",
        "ОКП: Добавление адреса в справочник", "Платное подключение",
        "Смена технологии", "Аварийные работы", "Ак-Ордо / Ала-Тоо 3: Проблема",
        "Не работает беспроводной интернет",
    }
    filtered = [it for it in data if it.get("parent_issue_type") not in excluded_types]
    grouped: dict[str, dict] = {}
    for item in filtered:
        key = item["parent_issue_type"]
        g = grouped.setdefault(key, {
            "parent_issue_type": key,
            "total_count": [], "subtask_type": [], "subtasks_count": [],
            "open_subtasks_count": [], "closed_subtasks_count": [],
            "ontime_closed_subtasks_count": [], "sla_plan_days": [], "sla_fact_days": [],
        })
        for fld in ("total_count", "subtask_type", "subtasks_count", "open_subtasks_count",
                    "closed_subtasks_count", "ontime_closed_subtasks_count",
                    "sla_plan_days", "sla_fact_days"):
            g[fld].append(item[fld])

    cs_sla = list(grouped.values())
    for g in cs_sla:
        rows = list(zip(g["total_count"], g["subtasks_count"], g["subtask_type"],
                        g["open_subtasks_count"], g["closed_subtasks_count"],
                        g["ontime_closed_subtasks_count"], g["sla_plan_days"], g["sla_fact_days"]))
        rows.sort(key=lambda r: (int(r[0]), int(r[1])), reverse=True)
        (g["total_count"], g["subtasks_count"], g["subtask_type"], g["open_subtasks_count"],
         g["closed_subtasks_count"], g["ontime_closed_subtasks_count"],
         g["sla_plan_days"], g["sla_fact_days"]) = map(list, zip(*rows)) if rows else ([],) * 8
    cs_sla.sort(key=lambda g: (g["total_count"][0] if g["total_count"] else 0), reverse=True)

    task = TableConstructorIndents()
    headers = [
        "Основная заявка", "Подзадача", "Всего заявок", "Поступило подзадач",
        "Незакрыто подзадач", "Закрыто подзадач", "Закрыто в срок",
        "Плановый SLA в Jira (часы)", "Фактический SLA в Jira (часы)",
        "SLA, %", "SLA, который сообщаем абоненту по основной заявке",
    ]
    for h in headers: task.genCellTH(h)
    task.genRow()


    for groupedRow in cs_sla:
        task.genDataRowGrouped(groupedRow)

    html = task.renderTable()
    return html

@op
def render_jira_data_saima_ms_op(context, items):
    return render_jira_issues_table_new(items)


@op
def render_unique_ms_op(context, items):
    tables = []
    for group in items:
        get_dagster_logger().info(f"group = {group}")
        table = render_jira_issues_status_table_new(group)
        table.TableStyle = (
        "border:1px solid #32383e; border-collapse:collapse; font-size:12; "
        "width:100%; table-layout:fixed;"
    )
        tables.append(table.getTable())
    get_dagster_logger().info(f"length = {len(tables)}")
    return tables

@op
def render_unique_ma_op(context, items):
    table = render_jira_issues_status_table_new(items, 'ma')
    table.TableStyle = "border: 1px solid  #32383e; border-collapse: collapse; font-size: 12;"
    return table.getTable()

@op(required_resource_keys={'report_utils'})
def voice_indicators_saima_op(context, main_data_dynamic, service_level_month_general, handled_calls_month_general,
                              handling_times_data, average_handling_time_month_rounded_value, average_ringing_time_month_rounded_value,
average_holding_time_mont_rounded_value, cea_data, cea_month_value, csi_data, csi_month_value, csi_conversion_month_value,
ost_rc_data, ost_rc_month_value, repeat_calls_by_ivr, average_repeat_ivr_pct
                              ):
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
        ['Service Level', '≥ 80 %', main_data_dynamic, 'SL', 80, '<', service_level_month_general],
        ['Handled Calls Rate (Процент принятых вызовов)', '≥ 90 %', main_data_dynamic, 'ACD', 90, '<',
         handled_calls_month_general],
        ['Average Handling Time', '≤ 190 сек.', handling_times_data['items'], 'average_handling_time', 190, '>',
         average_handling_time_month_rounded_value],
        ['Average Ringing Time', '≤ 3 сек.', handling_times_data['items'], 'average_ringing_time', 3, '>',
         average_ringing_time_month_rounded_value],
        ['Average Holding Time', '≤ 30 сек.', handling_times_data['items'], 'average_holding_time', 30, '>',
         average_holding_time_mont_rounded_value],
        ['Critical Error Accuracy', '≥ 97 %.', cea_data['items'], 'critical_error_accuracy', 97, '<', cea_month_value],
        ['Customer Satisfaction Index', '≥ 80 %.', csi_data, 'customer_satisfaction_index', 80, '<', csi_month_value],
        ['Конверсия Customer Satisfaction Index', '≥ 10 %.', csi_data, 'conversion', 10, '<',
         csi_conversion_month_value],
        ['Repeat Calls по операторам', '≤ 10 %.', ost_rc_data['items'], 'repeat_calls', 10, '>', ost_rc_month_value],
        ['Repeat Calls по IVR', '≤ 10 %.', repeat_calls_by_ivr, 'repeat_calls', 10, '>', average_repeat_ivr_pct],
    ]

    for indicator in voice_indicators:
        get_task_row(task, *indicator, date_list=date_list)

    return task.renderTable()

@op(required_resource_keys={'report_utils'})
def text_indicators_op(context, mp_data_messengers_aggregated, month_average_reaction_time,
month_average_speed_to_answer, pst_cea_data, pst_cea_month_value, pst_csi_data,
pst_csi_month_value, pst_csi_conversion_month_value
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
        ['Average Reaction Time', '≤ 60 сек.', mp_data_messengers_aggregated, 'average_reaction_time', 60, '>',
         month_average_reaction_time],
        ['Average Speed To Answer', '≤ 60 сек.', mp_data_messengers_aggregated, 'average_speed_to_answer', 60, '>',
         month_average_speed_to_answer],
        ['Critical Error Accuracy', '≥ 97 %.', pst_cea_data['items'], 'critical_error_accuracy', 97, '<',
         pst_cea_month_value],
        ['Customer Satisfaction Index', '≥ 80 %.', pst_csi_data, 'customer_satisfaction_index', 80, '<',
         pst_csi_month_value],
        ['Конверсия Customer Satisfaction Index', '≥ 10 %.', pst_csi_data, 'conversion', 10, '<',
         pst_csi_conversion_month_value],
    ]

    task = render_pst_daily_profile_table(text_indicators, formatted_dates, date_list)
    return task.renderTable()

@op(required_resource_keys={'report_utils'})
def voice_indicators_general_saima_op(context, data_omni_saima):
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
         data_omni_saima['avg_closed_by_first_line']],
        ['Количество обращений, переданных другим отделам', data_omni_saima['items'], 'jira_issue', data_omni_saima['avg_jira_issue']],
        ['Доля обращений, закрытых на уровне 1 линии', data_omni_saima['items'], 'portion', data_omni_saima['avg_portion']],
        ['Доля обращений, переданных другим отделам', data_omni_saima['items'], 'proportion_of_escalated',
         data_omni_saima['avg_proportion_of_escalated']],
    ]

    task = render_pst_daily_profile_table_new(voice_indicators_general, formatted_dates, date_list)
    return task.renderTable()

@op
def handled_time_table_saima_op(detailed_daily_data):
    return render_day_profile(detailed_daily_data, key=False)

@op
def table_handle_time_saima_op(pst_detailed_daily_data):
    return render_pst_handled_time_table(pst_detailed_daily_data, False)

@op(required_resource_keys={'source_dwh', 'source_cp', 'report_utils'})
def table_technical_works_saima_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    return render_technical_works_table(conn=context.resources.source_cp, period='hour', project_ids=omni_project_ids, key='Saima', start_date=start_date, end_date=end_date, report_date=report_date)

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'source_cp', 'report_utils'})
def client_requests_saima_op(context):
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
def render_ivr_transitions_data_saima_op(context, items):
    return render_ivr_transitions(items, project='Saima')

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def mp_chat_pivot_table_saima_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    return render_chat_data(conn=context.resources.source_dwh, period='day', project_ids=mp_project_ids, pst_position_ids=pst_position_ids, start_date=start_date, end_date=end_date)

@op
def graphic_calls_dynamic_saima_op(context, main_data_dynamic):
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
    return gen_chart_line(main_data_dynamic, fields, color_map=color_map, is_target=False, ylabel='Звонки',
                          graph_title='Данный график показывает конверсию по звонкам.')

@op
def graphic_sl_saima_op(main_data_dynamic):
    return add_chart_module(target=80, graph_title='', axes_name='Процент принятых', items=main_data_dynamic, field='SL')

@op
def graphic_acd_saima_op(main_data_dynamic):
    return add_chart_module(target=90, graph_title='', axes_name='Процент принятых', items=main_data_dynamic, field='ACD')

@op
def graphic_aht_saima_op(context, handling_times_data):
    return add_chart_module(target=190, graph_title='', axes_name='Секунды', items=handling_times_data['items'], field='average_handling_time', axes_max=210, axes_min=50)

@op
def graphic_ivr_ssr_saima_op(main_data_dynamic):
    return add_chart_module(target=60, graph_title='', axes_name='% абонентов, обработанных в IVR', items=main_data_dynamic, field='SSR')

@op
def graphic_rc_saima_op( ost_rc_data):
    return add_chart_module(target=10, graph_title='', axes_name='% звонков за которыми последовали повторные вызовы', items=ost_rc_data['items'], field='repeat_calls', axes_max=120)


@op(required_resource_keys={"report_utils"})
def report_pdf_saima(context, saima_graphic, fig1, table1, table2, table3, table4, table5, tables, table6, tables2, fig2, fig3,
                     fig4, fig5, fig6, fig7, fig8, fig9, fig10, graph, graph1, graph2, graph3, graph4, graph5,
                     service_level_month, handled_calls_month, average_handling_time_month,
                     self_service_level_month, ost_rc_month):
    report = MailReport()
    report.image_mode = "file"

    APP_ROOT = Path(__file__).resolve().parents[2]
    LOGO_PATH = APP_ROOT / "utils" / "logo_png" / "logo.png"

    report.logo_file_override = str(LOGO_PATH)
    dates = context.resources.report_utils.get_utils()
    report_date = datetime.strptime(dates.ReportDate, "%Y-%m-%d")

    p1 = ""
    p1 += report.set_basic_header(f"ЕЖЕДНЕВНАЯ СТАТИСТИКА SAIMA НА {report_date.strftime('%d.%m.%Y')}")
    p1 += report.add_block_header("Статистика по обработанным чатам")
    p1 += report.add_module_header("Saima")
    p1 += report.add_figure(saima_graphic)
    p1 += report.add_block_header("Заявки в Jira")
    p1 += report.add_module_header("Общая динамика SLA по закрытым задачам")
    p1 += report.add_figure(fig1)


    p2 = ""
    p2 += report.add_module_header("Количество заявок и SLA по типам за последние 30 дней")
    p2 += report.add_module_child_header("CUSTOMER SERVICES")
    p2 += report.add_content_to_statistic(table1)
    p2 += report.add_module_child_header("MAIN SERVICES и MASS ACCIDENTS ")
    p2 += report.add_content_to_statistic(table2)

    p3 = ""
    p3 += report.add_module_header("ТОП 10 созданных основных заявок")
    p3 += report.add_module_child_header("MAIN SERVICES")
    p3 += report.add_content_to_statistic(table3)
    p3 += report.add_module_child_header("MASS ACCIDENTS")
    p3 += report.add_content_to_statistic(table4)
    p3 += report.add_module_child_header("CUSTOMER SERVICES")
    p3 += report.add_content_to_statistic(table5)

    p4 = ""
    p4 += report.add_module_header("Неотработанные заявки в Jira, созданные после 01.01.2025")
    p4 += report.add_module_child_header("MAIN SERVICES")
    for i in range(len(tables)):
        p4 += report.add_content_to_statistic(tables[i])
    p4 += report.add_module_child_header("MASS ACCIDENTS")
    p4 += report.add_content_to_statistic(table6)
    p4 += report.add_module_child_header("CUSTOMER SERVICES")
    for i in range(len(tables2)):
        p4 += report.add_content_to_statistic(tables2[i])

    p5 = ""
    p5 += report.add_block_header("Срез по основным показателям КЦ")
    p5 += report.add_module_header("Голос")
    p5 += report.add_content_to_statistic(fig2)
    p5 += report.add_module_header("Текст")
    p5 += report.add_content_to_statistic(fig3)
    p5 += report.add_module_header("Общее")
    p5 += report.add_content_to_statistic(fig4)

    p6 = ""
    p6 += report.add_block_header("Профиль дня")
    p6 += report.add_module_header("Голос")
    p6 += report.add_content_to_statistic(fig5)
    p6 += report.add_module_header("Текст")
    p6 += report.add_content_to_statistic(fig6)

    p7 = ""
    p7 += report.add_block_header(f"Таблица актуальных аварийно-плановых работ на {report_date.strftime('%d.%m.%Y')}")
    p7 += report.add_content_to_statistic(fig7)
    p7 += report.add_block_header("ТОП40 причин обращений")
    p7 += report.add_content_to_statistic(fig8)

    p8 =""
    p8 += report.add_block_header("Сводка по переходам IVR.")
    p8 += report.add_content_to_statistic(fig9)
    p8 += report.add_block_header("Сводка по проектам CP.")
    p8 += report.add_content_to_statistic(fig10)
    p8 += report.add_block_header('Статистика по основным показателям')
    p8 += report.add_module_header('Общая динамика звонков за 31 день')
    p8 += report.add_figure(graph)

    p9 = ""
    p9 += report.add_module_header("Service Level", f"Цель: ≥ 80%, Факт: {service_level_month} % принятых")
    p9 += report.add_figure(graph1)
    p9 += report.add_module_header("Handled Calls Rate", f"Цель: ≥ 90%, Факт: {handled_calls_month} % принятых")
    p9 += report.add_figure(graph2)
    p9 += report.add_module_header("Average Handling Time", f"Цель: ≤ 190 сек., Факт: {average_handling_time_month} сек.")
    p9 += report.add_figure(graph3)
    p9 += report.add_module_header("IVR Self Service Rate", f"Цель: ≥ 60%, Факт: {self_service_level_month} % принятых")
    p9 += report.add_figure(graph4)
    p9 += report.add_module_header("Repeat Calls. Все проекты", f"Цель: ≤ 10 %, Факт: {ost_rc_month} %")
    p9 += report.add_figure(graph5)

    saima_tmp1 = "saima_page1.pdf"
    saima_tmp2 = 'saima_page2.pdf'
    saima_tmp3 = 'saima_page3.pdf'
    saima_tmp4 = 'saima_page4.pdf'
    saima_tmp5 = 'saima_page5.pdf'
    saima_tmp6 = 'saima_page6.pdf'
    saima_tmp7 = 'saima_page7.pdf'
    saima_tmp8 = 'saima_page8.pdf'
    saima_tmp9 = 'saima_page9.pdf'

    html_to_pdf_page(html=p1, output_path=saima_tmp1, height=500)
    html_to_pdf_page(html=p2, output_path=saima_tmp2)
    html_to_pdf_page(html=p3, output_path=saima_tmp3)
    html_to_pdf_page(html=p4, output_path=saima_tmp4)
    html_to_pdf_page(html=p5, output_path=saima_tmp5, height=620)
    html_to_pdf_page(html=p6, output_path=saima_tmp6)
    html_to_pdf_page(html=p7, output_path=saima_tmp7)
    html_to_pdf_page(html=p8, output_path=saima_tmp8)
    html_to_pdf_page(html=p9, output_path=saima_tmp9)

    final_pdf = "saima_daily_statistics.pdf"

    merge_pdfs([saima_tmp1, saima_tmp2, saima_tmp3, saima_tmp4, saima_tmp5, saima_tmp6, saima_tmp7, saima_tmp8, saima_tmp9], final_pdf)

    final_path = str(Path(final_pdf).resolve())
    context.log.info(f"final_pdf_path: {final_path}")
    return final_path