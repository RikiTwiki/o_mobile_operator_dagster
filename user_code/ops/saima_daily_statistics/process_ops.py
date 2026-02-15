import json
from datetime import timedelta, datetime, date

import requests
from dagster import op, get_dagster_logger, In, Out, Failure

from resources import ReportUtils
from utils.getters.jira_data_getter_next import JiraDataGetterNext
from utils.getters.omni_data_getter import OmniDataGetter
from utils.report_data_collector import get_main_aggregation_data, get_average_handling_time_data, get_pst_cea_data, \
    get_csi_data, aggregate_data, get_rc_data, get_daily_detailed_data, get_mp_data, get_jira_sla_data_new, \
    get_rc_by_ivr, render_pst_daily_profile, render_client_requests, get_saima_ivr_transitions_data, \
    get_main_aggregation_data_with_9999

from zoneinfo import ZoneInfo

from utils.mail.mail_sender import ReportsApiConfig, ReportInstanceUploader

import os

from typing import Dict, Any

connection = 'naumen'

prompt_ids = [53, 57, 58, 59, 60, 61, 62, 63, 64, 65]
ost_position_ids = [39]
pst_position_ids = [41]
mp_project_ids = [1]
omni_project_ids = [1]
ost_time_table_view_id = [16]
pst_time_table_view_id = [17]
groups = ['Saima']
channels = ['cp']
jira_project_keys = {
    "MS": "MAIN SERVICES",
    "MA": "MASS ACCIDENTS",
    "CS": "CUSTOMER SERVICES",
}
jira_project = {
    "title": "Saima - 0706 909 000",
    "id": 12,
    "topic_view_id": 1,
    "jira_project_id": "11005",
    "jira_project_key": ["MS", "CSA", "MA"],
}

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'source_dwh_prod', 'report_utils'})
def main_data_dynamic_saima_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    main_data_dynamic = get_main_aggregation_data(conn=context.resources.source_dwh, group=groups, period='day', start_date=start_date, end_date=end_date, report_date=report_date)

    main_data_with_9999_dynamic = get_main_aggregation_data_with_9999(conn=context.resources.source_naumen_3, data=main_data_dynamic, group=groups, period='day', start_date=start_date, end_date=end_date, report_date=report_date)

    context.log.info(f"main_data_with_9999_dynamic: {main_data_with_9999_dynamic}")

    context.log.info(f"main_data_dynamic: {main_data_dynamic}")
    return main_data_with_9999_dynamic

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'source_dwh_prod', 'report_utils'})
def main_data_month_general_saima_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    main_data_dynamic_month = get_main_aggregation_data(conn=context.resources.source_dwh, group=groups, period='month', start_date=start_date, end_date=end_date, report_date=report_date)
    context.log.info(f"main_data_dynamic_month: {main_data_dynamic_month}")
    return main_data_dynamic_month

@op
def service_level_month_saima_op(context, main_data_month_general_saima):
    service_level_month_general = round(main_data_month_general_saima[0]["SL"], 1)
    context.log.info(f"service_level_month_general: {service_level_month_general}")
    return service_level_month_general

@op
def handled_calls_month_saima_op(context, main_data_month_general_saima):
    handled_calls_month_general = round(main_data_month_general_saima[0]["ACD"], 1)
    context.log.info(f"handled_calls_month_general: {handled_calls_month_general}")
    return handled_calls_month_general

@op
def self_service_level_month_saima_op(context, main_data_month_general_saima):
    self_service_level_month_general = round(main_data_month_general_saima[0]["SSR"], 1)
    context.log.info(f"self_service_level_month_general: {self_service_level_month_general}")
    return self_service_level_month_general

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'source_dwh_prod', 'report_utils'})
def handling_times_data_saima_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    handled_calls_month = get_average_handling_time_data(conn=context.resources.source_dwh, project=groups, period='day', start_date=start_date, end_date=end_date)
    context.log.info(f"handled_calls_day: {handled_calls_month}")
    return handled_calls_month

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'source_dwh_prod', 'report_utils'})
def handling_times_month_data_saima_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    handling_times_month = get_average_handling_time_data(conn=context.resources.source_dwh, project=groups, period='month', start_date=start_date, end_date=end_date)
    handling_times_month = aggregate_data(handling_times_month['items'])
    context.log.info(f"handling_times_month: {handling_times_month}")
    return handling_times_month

@op
def average_handling_time_month_saima_op(context, handling_times_month):
    q = handling_times_month.get("quantity", 0) or 0
    average_handling_time_month_saima = round(
        ((handling_times_month.get("sum_speaking_time", 0) + handling_times_month.get("sum_wrapup_time", 0)) / q) if q else 0.0,
        1,
    )
    context.log.info(f"average_handling_time_month_saima: {average_handling_time_month_saima}")
    return average_handling_time_month_saima

@op
def average_ringing_time_month_saima_op(context, handling_times_month):
    d = handling_times_month
    q = d.get("quantity", 0) or 0
    average_ringing_time_month_saima = round(
        (d.get("sum_pickup_time", 0) / q) if q else 0.0,
        1,
    )
    context.log.info(f"average_ringing_time_month_saima: {average_ringing_time_month_saima}")
    return average_ringing_time_month_saima

@op
def average_holding_time_month_saima_op(context, handling_times_month):
    d = handling_times_month
    q_check = d.get("quantity", 0) or 0
    den = d.get("quantity_hold", 0) or 0
    average_holding_time_month_saima = round(
        (d.get("sum_holding_time", 0) / den) if q_check else 0.0,
        1,
    )
    context.log.info(f"average_holding_time_month_saima: {average_holding_time_month_saima}")
    return average_holding_time_month_saima

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'source_dwh_prod', 'report_utils'})
def cea_data_saima_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    cea_data = get_pst_cea_data(conn=context.resources.source_dwh, period='day', position=ost_position_ids, start_date=start_date, end_date=end_date)
    context.log.info(f"cea_data: {cea_data}")
    return cea_data

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'source_dwh_prod', 'report_utils'})
def cea_data_month_saima_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    try:
        cea_data_month = get_pst_cea_data(
            conn=context.resources.source_dwh,
            period="month",
            position=ost_position_ids,
            start_date=start_date,
            end_date=end_date,
        )
        context.log.info(f"cea_data_month: {cea_data_month}")

        val = cea_data_month["items"][0]["critical_error_accuracy"]
        cea_data_month = round(float(val), 1) if val is not None else 0.0

    except Exception as e:
        context.log.exception(f"CEA month calc failed -> return 0.0. Error: {e}")
        cea_data_month = 0.0
    context.log.info(f"cea_data_month: {cea_data_month}")
    return cea_data_month

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'source_dwh_prod', 'report_utils'})
def csi_data_saima_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    csi_data = get_csi_data(conn=context.resources.source_dwh, period='day', group=groups, channels=[connection], start_date=start_date, end_date=end_date)
    context.log.info(f"csi_data: {csi_data}")
    return csi_data

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'source_dwh_prod', 'report_utils'})
def csi_data_month_saima_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    csi_data_month = get_csi_data(conn=context.resources.source_dwh, period='month', group=groups, channels=[connection], start_date=start_date, end_date=end_date)
    csi_data_month = aggregate_data(csi_data_month)
    context.log.info(f"csi_data_month: {csi_data_month}")
    return csi_data_month

@op
def csi_month_value_saima_op(context, csi_data_month):
    d = csi_data_month
    good = d.get("good_marks", 0) or 0
    bad = d.get("bad_marks", 0) or 0

    if good == 0 and bad == 0:
        csi_month_value = None
    elif good == 0:
        csi_month_value = 0.0
    elif bad == 0:
        csi_month_value = 100.0
    else:
        csi_month_value = round(good / (good + bad) * 100, 1)

    context.log.info(f"csi_month_value: {csi_month_value}")
    return csi_month_value

@op
def csi_conversion_month_value_saima_op(context, csi_data_month):
    d = csi_data_month
    all_contacts = d.get("all_contacts", 0) or 0
    good = d.get("good_marks", 0) or 0
    bad = d.get("bad_marks", 0) or 0
    neutral = d.get("neutral_marks", 0) or 0

    if all_contacts == 0:
        csi_conversion_month_value = None
    elif good == 0 and bad == 0 and neutral == 0:
        csi_conversion_month_value = 0.0
    else:
        csi_conversion_month_value = round((good + bad + neutral) / all_contacts * 100, 1)

    return csi_conversion_month_value

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'source_dwh_prod', 'report_utils'})
def ost_rc_data_saima_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    ost_rc_data = get_rc_data(conn=context.resources.source_dwh_prod, period='day', group=groups, start_date=start_date, end_date=end_date)
    context.log.info(f"ost_rc_data: {ost_rc_data}")
    return ost_rc_data

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'source_dwh_prod', 'report_utils'})
def ost_rc_data_month_saima_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    ost_rc_data_month = get_rc_data(conn=context.resources.source_dwh, period='month', group=groups, start_date=start_date, end_date=end_date)
    ost_rc_data_month = aggregate_data(ost_rc_data_month['items'])
    d = ost_rc_data_month
    resolved = d.get("resolved", 0) or 0
    not_resolved = d.get("not_resolved", 0) or 0

    ost_month_value = round(
        (not_resolved * 100 / (resolved + not_resolved)) if resolved != 0 else 0.0,
        1,
    )
    context.log.info(f"ost_month_value: {ost_month_value}")
    return ost_month_value

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'source_dwh_prod', 'source_cp', 'report_utils'})
def detailed_daily_data_saima_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    detailed_daily_data = get_daily_detailed_data(conn=context.resources.source_cp, bpm_conn=context.resources.source_dwh, naumen_conn_1=context.resources.source_naumen_1,
                                                  naumen_conn_3=context.resources.source_naumen_3, group=groups, views=ost_time_table_view_id,
                                                  positions=ost_position_ids, channels=[connection], start_date=start_date, end_date=end_date, report_date=report_date)
    context.log.info(f"detailed_daily_data: {detailed_daily_data}")
    return detailed_daily_data

@op
def max_ivr_total_saima_op(context, main_data_dynamic):
    vals = [d.get("ivr_total") for d in main_data_dynamic if d.get("ivr_total") is not None]
    max_ivr_total = max(vals) if vals else 0  # или None
    context.log.info(f"max_ivr_total = {max_ivr_total}")
    return max_ivr_total

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'source_dwh_prod', 'report_utils'})
def mp_data_messengers_aggregated_saima_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    mp_data_messengers_aggregated = get_mp_data(conn=context.resources.source_dwh, period='day', start_date=start_date, end_date=end_date)
    context.log.info(f"mp_data_messengers_aggregated: {mp_data_messengers_aggregated}")
    return mp_data_messengers_aggregated

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'source_dwh_prod', 'report_utils'})
def mp_month_data_saima_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    mp_month_data = get_mp_data(conn=context.resources.source_dwh, period='month', start_date=start_date, end_date=end_date)
    context.log.info(f"mp_data_messengers_aggregated: {mp_month_data}")
    return mp_month_data

@op
def month_average_reaction_time_saima_op(context, mp_month_data):
    month_average_reaction_time = round(mp_month_data[0]['average_reaction_time'] if mp_month_data else 0, 1)
    context.log.info(f"month_average_reaction_time = {month_average_reaction_time}")
    return month_average_reaction_time

@op
def month_average_speed_to_answer_saima_op(context, mp_month_data):
    month_average_speed_to_answer = round(mp_month_data[0]['average_speed_to_answer'] if mp_month_data else 0, 1)
    context.log.info(f"month_average_speed_to_answer = {month_average_speed_to_answer}")
    return month_average_speed_to_answer

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'source_dwh_prod', 'report_utils'})
def pst_cea_data_saima_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    pst_cea_data = get_pst_cea_data(conn=context.resources.source_dwh_prod, period='day', position=pst_position_ids, start_date=start_date, end_date=end_date)
    context.log.info(f"pst_cea_data = {pst_cea_data}")
    return pst_cea_data

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'source_dwh_prod', 'report_utils'})
def pst_cea_data_month_saima_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    try:
        pst_cea_data_month = get_pst_cea_data(
            conn=context.resources.source_dwh_prod,
            period="month",
            position=pst_position_ids,
            start_date=start_date,
            end_date=end_date,
        )

        val = pst_cea_data_month["items"][0]["critical_error_accuracy"]
        pst_cea_data_month = round(float(val), 1) if val is not None else 0.0

    except Exception as e:
        context.log.exception(f"pst_cea_data_month calc failed -> return 0.0. Error: {e}")
        pst_cea_data_month = 0.0

    context.log.info(f"pst_cea_data_month = {pst_cea_data_month}")
    return pst_cea_data_month

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'source_dwh_prod', 'report_utils'})
def pst_csi_data_saima_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    pst_csi_data = get_csi_data(conn=context.resources.source_dwh, period='day', group=groups, channels=channels, start_date=start_date, end_date=end_date)
    context.log.info(f"pst_csi_data: {pst_csi_data}")
    return pst_csi_data

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'source_dwh_prod', 'report_utils'})
def pst_csi_data_month_saima_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    pst_csi_data_month = get_csi_data(conn=context.resources.source_dwh, period='month', group=groups, channels=channels, start_date=start_date, end_date=end_date)
    context.log.info(f"pst_csi_data_month: {pst_csi_data_month}")
    return pst_csi_data_month

@op
def pst_csi_current_month_value_saima_op(context, pst_csi_month_data):
    if pst_csi_month_data[1]:
        pst_csi_current_month_value = round(pst_csi_month_data[1]['customer_satisfaction_index'], 1)
    else:
        pst_csi_current_month_value = 0

    context.log.info(f'pst_csi_current_month_value = {pst_csi_current_month_value}')
    return pst_csi_current_month_value

@op
def pst_csi_conversion_current_month_value_saima_op(context, pst_csi_month_data):
    if pst_csi_month_data:
        pst_csi_conversion_current_month_value = round(pst_csi_month_data[1]['conversion'], 1)
    else:
        pst_csi_conversion_current_month_value = 0

    context.log.info(f'pst_csi_conversion_current_month_value = {pst_csi_conversion_current_month_value}')
    return pst_csi_conversion_current_month_value

@op(required_resource_keys={'source_dwh_prod', 'report_utils'})
def jira_data_saima_ms_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    jira_data = JiraDataGetterNext(dwh_conn=context.resources.source_dwh_prod,
                                   jira_nur=context.resources.source_dwh_prod, # Поменять на context.resources.source_jira_nur
                                   jira_saima=context.resources.source_dwh_prod, # Поменять на context.resources.source_jira_saima
                                   start_date=start_date, end_date=end_date,
                                   jira="jirasd_saima")
    data_ms = jira_data.get_jira_subtask_time_series(["MS"])
    get_dagster_logger().info(f"data_ms: {data_ms}")

    data = json.loads(data_ms) if isinstance(data_ms, (str, bytes)) else data_ms
    get_dagster_logger().info(f"data: {data}")
    readable_ms = {}

    for item in data:
        if not isinstance(item, dict):
            continue
        result = item.get("result")
        if not result:
            continue

        decoded = json.loads(result)


        parent = decoded.get("parent_issue_type", "Unknown")
        subs = decoded.get("subtitles", []) or []

        try:
            subs_sorted = sorted(subs, key=lambda s: s.get("date", ""), reverse=True)
        except Exception:
            subs_sorted = subs

        readable_ms[parent] = subs_sorted

    context.log.info(f"readable_ms = {readable_ms}")
    return readable_ms

@op(required_resource_keys={'source_dwh_prod', 'report_utils'})
def issues_ma_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    jira_data = JiraDataGetterNext(dwh_conn=context.resources.source_dwh_prod,
                                   jira_nur=context.resources.source_dwh_prod,
                                   # Поменять на context.resources.source_jira_nur
                                   jira_saima=context.resources.source_dwh_prod,
                                   # Поменять на context.resources.source_jira_saima
                                   start_date=start_date, end_date=end_date,
                                   jira="jirasd_saima")
    issues_ma = jira_data.get_jira_issue_counts_by_date_and_type(["MA"])
    if isinstance(issues_ma, (str, bytes)):
        issues = json.loads(issues_ma)
    else:
        issues = issues_ma
    if not isinstance(issues, list):
        issues = list(issues or [])

    result_ma = {}
    for item in issues:
        if not isinstance(item, dict):
            continue
        t = item.get("parent_issue_type")
        if t is None:
            continue
        bucket = result_ma.setdefault(t, [])
        bucket.append({
            "date": item.get("issue_date"),
            "subtask_type": item.get("subtask_type"),
            "count": item.get("issues_count"),
        })

    context.log.info(f"result_ma = {result_ma}")
    return result_ma

@op(required_resource_keys={'source_dwh_prod', 'report_utils'})
def jira_data_saima_cs_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    jira_data = JiraDataGetterNext(dwh_conn=context.resources.source_dwh_prod,
                                   jira_nur=context.resources.source_dwh_prod,
                                   # Поменять на context.resources.source_jira_nur
                                   jira_saima=context.resources.source_dwh_prod,
                                   # Поменять на context.resources.source_jira_saima
                                   start_date=start_date, end_date=end_date,
                                   jira="jirasd_saima")

    data_cs = jira_data.get_jira_subtask_time_series(["CS"])
    data = json.loads(data_cs) if isinstance(data_cs, (str, bytes)) else data_cs
    excluded_types = {
        "ТП Сотрудник 550 + О!TV от Сайма Телеком",
        "Task",
        "На включение xDSL",
        "Перерасчет в биллинге",
        "Тех лист: На исправление",
    }

    filtered= []
    for item in data:
        if not isinstance(item, dict):
            continue
        inner_raw = item.get("result")
        try:
            inner = json.loads(inner_raw) if inner_raw else None
            if inner and inner.get("parent_issue_type") in excluded_types:
                continue
        except json.JSONDecodeError:
            pass
        filtered.append(item)

    data_cs_list = list(filtered)

    readable_cs = {}
    for item in data_cs_list:
        result = item.get("result")
        if not result:
            continue
        decoded = json.loads(result)


        parent = decoded.get("parent_issue_type", "Unknown")
        subs = decoded.get("subtitles", []) or []

        try:
            subs_sorted = sorted(subs, key=lambda s: s.get("date", ""), reverse=True)
        except Exception:
            subs_sorted = subs

        readable_cs[parent] = subs_sorted

    context.log.info(f"readable_cs = {readable_cs}")
    return readable_cs

@op(required_resource_keys={'source_dwh_prod', 'report_utils'})
def jira_data_saima_unique_ms_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    jira_data = JiraDataGetterNext(dwh_conn=context.resources.source_dwh_prod,
                                   jira_nur=context.resources.source_dwh_prod,
                                   # Поменять на context.resources.source_jira_nur
                                   jira_saima=context.resources.source_dwh_prod,
                                   # Поменять на context.resources.source_jira_saima
                                   start_date=start_date, end_date=end_date,
                                   jira="jirasd_saima")

    data = jira_data.get_open_issues_subtasks_by_region_month(["MS"])

    get_dagster_logger().info(f"get_open_issues = {data}")
    data = json.loads(data) if isinstance(data, (str, bytes)) else (data or [])
    if isinstance(data, dict):
        data = list(data.values())
    data = [it for it in data if isinstance(it, dict)]

    exclude_titles = {
        "MAIN SERVICES Аварийные работы",
        "MAIN SERVICES Не работает беспроводной интернет",
        "MAIN SERVICES Обращения с сайта: Поддержка",
        "MAIN SERVICES Ак-Ордо / Ала-Тоо 3: Проблема",
    }
    data = [it for it in data if it.get("title") not in exclude_titles]

    unique_issues_array = jira_data.get_unique_unresolved_issues_by_month_region_title(["MS"])
    unique_issues_array = json.loads(unique_issues_array) if isinstance(unique_issues_array, (str, bytes)) else (
                unique_issues_array or [])
    if isinstance(unique_issues_array, dict):
        unique_issues_array = list(unique_issues_array.values())
    unique_issues_array = [it for it in unique_issues_array if
                           isinstance(it, dict) and it.get("title") not in exclude_titles]

    grouped: dict[str, dict] = {}
    for item in data:
        title = item.get("title")
        region = item.get("область")
        key = f"{title}|{region}"
        g = grouped.setdefault(key, {"title": title, "область": region, "entries": []})

        subs_raw = item.get("subtasks")
        subtasks = None
        if isinstance(subs_raw, (str, bytes)):
            try:
                subtasks = json.loads(subs_raw)
            except json.JSONDecodeError:
                subtasks = None
        else:
            subtasks = subs_raw

        g["entries"].append({
            "month": item.get("month"),
            "subtasks": subtasks,
            "count": 0,
        })

    region_order = {
        "Чуйская область": 1,
        "Ошская область": 2,
        "Иссык-Кульская область": 3,
        "Джалал-Абадская область": 4,
        "Нарынская область": 5,
        "Баткенская область": 6,
        "Таласская область": 7,
    }

    result = list(grouped.values())

    for ui in unique_issues_array:
        for grp in result:
            if grp["title"] == ui.get("title") and grp["область"] == ui.get("область"):
                for entry in grp["entries"]:
                    if entry["month"] == ui.get("month"):
                        entry["count"] = int(ui.get("count", 0))

    def _prio(region: str) -> int:
        return region_order.get(region, 2 ** 31 - 1)

    def _sum_counts(grp: dict) -> int:
        return sum(int(e.get("count") or 0) for e in grp.get("entries", []))

    result.sort(key=lambda g: (_prio(g["область"]), -_sum_counts(g)))

    get_dagster_logger().info(f"result = {result}")
    return result

@op(required_resource_keys={'source_dwh_prod', 'report_utils'})
def jira_data_saima_unique_ma_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    jira_data = JiraDataGetterNext(dwh_conn=context.resources.source_dwh_prod,
                                   jira_nur=context.resources.source_dwh_prod,
                                   # Поменять на context.resources.source_jira_nur
                                   jira_saima=context.resources.source_dwh_prod,
                                   # Поменять на context.resources.source_jira_saima
                                   start_date=start_date, end_date=end_date,
                                   jira="jirasd_saima")

    data_ma = jira_data.get_open_issues_subtasks_by_region_month(["MA"])
    data_ma = json.loads(data_ma) if isinstance(data_ma, (str, bytes)) else (data_ma or [])
    if isinstance(data_ma, dict):
        data_ma = list(data_ma.values())
    data_ma = [it for it in data_ma if isinstance(it, dict)]

    unique_issues_array_ma = jira_data.get_unique_unresolved_issues_by_month_region_title(["MA"])
    unique_issues_array_ma = json.loads(unique_issues_array_ma) if isinstance(unique_issues_array_ma,
                                                                              (str, bytes)) else (
                unique_issues_array_ma or [])
    if isinstance(unique_issues_array_ma, dict):
        unique_issues_array_ma = list(unique_issues_array_ma.values())
    unique_issues_array_ma = [it for it in unique_issues_array_ma if isinstance(it, dict)]

    grouped_ma = {}
    for item in data_ma:
        title = item.get("title")
        region = item.get("область")
        key = f"{title}|{region}"
        g = grouped_ma.setdefault(key, {"title": title, "область": region, "entries": []})

        subs_raw = item.get("subtasks")
        if isinstance(subs_raw, (str, bytes)):
            try:
                subtasks = json.loads(subs_raw)
            except json.JSONDecodeError:
                subtasks = None
        else:
            subtasks = subs_raw

        g["entries"].append({
            "month": item.get("month"),
            "subtasks": subtasks,
            "count": 0,
        })

    region_order = {
        "Чуйская область": 1,
        "Ошская область": 2,
        "Иссык-Кульская область": 3,
        "Джалал-Абадская область": 4,
        "Нарынская область": 5,
        "Баткенская область": 6,
        "Таласская область": 7,
    }
    result = list(grouped_ma.values())

    for ui in unique_issues_array_ma:
        for grp in result:
            if grp["title"] == ui.get("title") and grp["область"] == ui.get("область"):
                for entry in grp["entries"]:
                    if entry["month"] == ui.get("month"):
                        entry["count"] = int(ui.get("count", 0))

    PHP_INT_MAX = 2 ** 63 - 1

    def _prio(region: str) -> int:
        return region_order.get(region, PHP_INT_MAX)

    def _sum_counts(grp) -> int:
        return sum(int(e.get("count") or 0) for e in grp.get("entries", []))

    result.sort(key=lambda g: (_prio(g["область"]), -_sum_counts(g)))

    all_entries = []
    for raw_group in result:
        for entry in raw_group["entries"]:
            all_entries.append({
                "title": raw_group["title"],
                "month": entry["month"],
                "subtasks": entry["subtasks"],
                "count": entry["count"],
            })

    combined_group = {"entries": all_entries}

    return combined_group

@op(required_resource_keys={'source_dwh_prod', 'report_utils'})
def jira_data_saima_unique_cs_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    jira_data = JiraDataGetterNext(dwh_conn=context.resources.source_dwh_prod,
                                   jira_nur=context.resources.source_dwh_prod,
                                   # Поменять на context.resources.source_jira_nur
                                   jira_saima=context.resources.source_dwh_prod,
                                   # Поменять на context.resources.source_jira_saima
                                   start_date=start_date, end_date=end_date,
                                   jira="jirasd_saima")
    data_cs = jira_data.get_open_issues_subtasks_by_region_month(["CS"])
    data_cs = json.loads(data_cs) if isinstance(data_cs, (str, bytes)) else (data_cs or [])
    if isinstance(data_cs, dict):
        data_cs = list(data_cs.values())
    data_cs = [it for it in data_cs if isinstance(it, dict)]

    exclude_titles = {
        "CUSTOMER SERVICES [SUB] Новый абонент - ОАП: Монтаж",
        "CUSTOMER SERVICES Нет покрытия",
        "CUSTOMER SERVICES ТП Сотрудник 550 + О!TV от Сайма Телеком",
        "CUSTOMER SERVICES Исправление данных в договоре",
    }
    data_cs = [it for it in data_cs if it.get("title") not in exclude_titles]

    unique_issues_array_cs = jira_data.get_unique_unresolved_issues_by_month_region_title(["CS"])
    unique_issues_array_cs = json.loads(unique_issues_array_cs) if isinstance(unique_issues_array_cs,
                                                                              (str, bytes)) else (
                unique_issues_array_cs or [])
    if isinstance(unique_issues_array_cs, dict):
        unique_issues_array_cs = list(unique_issues_array_cs.values())
    unique_issues_array_cs = [it for it in unique_issues_array_cs if
                              isinstance(it, dict) and it.get("title") not in exclude_titles]

    SPECIAL_TITLE = "CUSTOMER SERVICES Подключение нового корпоративного абонента"
    for it in data_cs:
        if it.get("title") == SPECIAL_TITLE:
            it["область"] = "Все области КР"
    for it in unique_issues_array_cs:
        if it.get("title") == SPECIAL_TITLE:
            it["область"] = "Все области КР"

    grouped_cs = {}
    for item in data_cs:
        title = item.get("title")
        region = item.get("область")
        if title is None or region is None:
            continue
        key = f"{title}|{region}"
        g = grouped_cs.setdefault(key, {"title": title, "область": region, "entries": []})

        subs_raw = item.get("subtasks")
        if isinstance(subs_raw, (str, bytes)):
            try:
                subtasks = json.loads(subs_raw)
            except json.JSONDecodeError:
                subtasks = None  # как PHP null при битом JSON
        else:
            subtasks = subs_raw

        g["entries"].append({
            "month": item.get("month"),
            "subtasks": subtasks,
            "count": 0,  # инициализация
        })

    # 4) Приоритет областей и преобразование в список
    region_order = {
        "Чуйская область": 1,
        "Ошская область": 2,
        "Иссык-Кульская область": 3,
        "Джалал-Абадская область": 4,
        "Нарынская область": 5,
        "Баткенская область": 6,
        "Таласская область": 7,
    }
    result = list(grouped_cs.values())

    # 5) Подстановка реальных count из uniqueIssuesArrayCS
    for ui in unique_issues_array_cs:
        for grp in result:
            if grp["title"] == ui.get("title") and grp["область"] == ui.get("область"):
                for entry in grp["entries"]:
                    if str(entry["month"]) == str(ui.get("month")):
                        entry["count"] = int(ui.get("count", 0))

    # 6) Сортировка: по приоритету области, затем по сумме count (DESC)
    PHP_INT_MAX = 2 ** 63 - 1

    def _prio(region: str) -> int:
        return region_order.get(region, PHP_INT_MAX)

    def _sum_counts(grp) -> int:
        return sum(int(e.get("count") or 0) for e in grp.get("entries", []))

    result.sort(key=lambda g: (_prio(g["область"]), -_sum_counts(g)))
    context.log.info(f"result = {result}")
    return result

@op(required_resource_keys={'source_dwh_prod', 'source_naumen_3', 'source_naumen_1', 'source_dwh', 'report_utils'})
def repeat_calls_by_ivr_saima_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    rows = get_rc_by_ivr(
        conn=context.resources.source_naumen_1,
        bpm_conn=context.resources.source_dwh_prod,
        start_date=date_list[0], end_date=end_date,
    )
    context.log.info(f'rows = {rows}')
    for r in rows:
        day = r.get("day") if isinstance(r, dict) else getattr(r, "day", None)
        if day is not None:
            (r.__setitem__("date", day) if isinstance(r, dict) else setattr(r, "date", day))
    return rows

@op
def average_repeat_ivr_saima_op(context, repeat_calls_by_ivr):
    total = cnt = 0.0
    for r in repeat_calls_by_ivr:
        val = r.get("repeat_calls") if isinstance(r, dict) else getattr(r, "repeat_calls", None)
        if val is not None:
            try:
                total += float(val);
                cnt += 1
            except (TypeError, ValueError):
                pass
    avg = round(total / cnt, 1) if cnt > 0 else 0.0
    context.log.info(f"average_repeat_ivr_pct={avg}")
    return avg

@op(required_resource_keys={'source_dwh_prod', 'source_dwh', 'source_cp', 'report_utils'})
def data_omni_saima_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    omni_data_getter = OmniDataGetter(conn=context.resources.source_cp,
                                      start_date=start_date, end_date=end_date)
    omni_data_getter.set_days_trunked_data()
    data_omni_saima = omni_data_getter.get_all_aggregated_requests()

    # ----- helpers: доступ к полям и безопасное деление -----
    def get(item, key, default=0.0):
        if isinstance(item, dict):
            return item.get(key, default)
        return getattr(item, key, default)

    def set_(item, key, value):
        if isinstance(item, dict):
            item[key] = value
        else:
            setattr(item, key, value)

    def div(a, b):
        return (float(a) / float(b)) if b else 0.0

    total_pct, count = 0.0, 0
    for row in data_omni_saima:
        if get(row, "closed_by_first_line", None) is not None:
            total_pct += float(get(row, "closed_by_first_line", 0))
            count += 1
    average_closed_by_first_line = round(div(total_pct, count), 1)

    for item in data_omni_saima:
        created_other = float(get(item, "created_other_jira_issue", 0))
        linked       = float(get(item, "linked_jira_issue", 0))

        closed_sl = float(get(item, "closed_by_second_line", 0))

        total        = float(get(item, "total", 0))
        closed_fls   = float(get(item, "closed_by_first_line", 0))

        jira_issue = (created_other + closed_sl) + linked
        portion = round(div(closed_fls, total) * 100, 1)
        proportion_of_escalated = round(div(jira_issue, total) * 100, 1)

        set_(item, "jira_issue", jira_issue)
        set_(item, "portion", portion)
        set_(item, "proportion_of_escalated", proportion_of_escalated)

    def avg_of(field):
        tot, cnt = 0.0, 0
        for row in data_omni_saima:
            val = get(row, field, None)
            if val is not None:
                tot += float(val)
                cnt += 1
        return round(div(tot, cnt), 1)

    average_jira_issue = avg_of("jira_issue")
    average_portion = avg_of("portion")
    average_proportion_of_escalated = avg_of("proportion_of_escalated")

    return {
        "items": data_omni_saima,
        "avg_closed_by_first_line": average_closed_by_first_line,
        "avg_jira_issue": average_jira_issue,
        "avg_portion": average_portion,
        "avg_proportion_of_escalated": average_proportion_of_escalated,
    }

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'source_cp', 'report_utils'})
def pst_detailed_daily_data_saima_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    pst_detailed_daily_data = render_pst_daily_profile(conn=context.resources.source_cp, bpm_conn=context.resources.source_dwh, naumen_conn_1=context.resources.source_naumen_1, naumen_conn_3=context.resources.source_naumen_3, project_ids=mp_project_ids,
                                                       groups=groups, channels=channels, period='hour',
                                                       timetable_views=pst_time_table_view_id, position_ids=pst_position_ids, start_date=start_date, end_date=end_date, report_date=report_date)

    context.log.info(f"pst_detailed_daily = {pst_detailed_daily_data}")
    return pst_detailed_daily_data

@op(required_resource_keys={'source_dwh', 'source_nod_dinosaur', 'report_utils'})
def ivr_transitions_data_saima_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    items = get_saima_ivr_transitions_data(nod_dinosaur_conn=context.resources.source_nod_dinosaur, callers=[], parents=prompt_ids, period='week', start_date=start_date, end_date=end_date, report_date=report_date, date_list=date_list)
    return items

@op(
    required_resource_keys={"report_utils"},
    ins={"file_path": In(str)},
    out=Out(dict),
)
def upload_report_instance_saima_op(context, file_path: str) -> Dict[str, Any]:
    cfg = context.op_config or {}

    dates = context.resources.report_utils.get_utils()
    rd = dates.ReportDate
    if isinstance(rd, str):
        report_day = datetime.strptime(rd[:10], "%Y-%m-%d").date()
    elif isinstance(rd, datetime):
        report_day = rd.date()
    else:
        report_day = rd  # date

    day_str = report_day.strftime("%Y.%m.%d")

    tz_name = cfg.get("timezone") or os.getenv("REPORTS_TZ") or "Asia/Bishkek"
    context.log.info(f"День отчёта: {day_str} (from report_utils.ReportDate, TZ={tz_name})")

    base_url = cfg.get("base_url") or os.getenv("REPORTS_API_BASE", "https://rv.o.kg")
    report_id = int(cfg.get("report_id", 5))
    api_key = cfg.get("api_key") or os.getenv("REPORTS_API_KEY") or os.getenv("RV_REPORTS_API_KEY")
    if not api_key:
        raise Failure("Не задан API ключ (REPORTS_API_KEY / RV_REPORTS_API_KEY или op_config.api_key).")

    timeout_connect = int(cfg.get("timeout_connect", 10))
    timeout_read = int(cfg.get("timeout_read", 120))
    verify_ssl = bool(cfg.get("verify_ssl", True))

    title = cfg.get("title") or "SAIMA | Общая ежедневная статистика"

    url = f"{base_url.rstrip('/')}/api/report-instances/upload/{report_id}"
    headers = {"x-api-key": api_key}
    data = {"title": title, "day": day_str}

    try:
        with open(file_path, "rb") as f:
            files = {"file": (os.path.basename(file_path), f, "application/pdf")}
            resp = requests.post(url, headers=headers, data=data, files=files,
                                 timeout=(timeout_connect, timeout_read), verify=verify_ssl)
    except Exception as e:
        return {"status_code": -1, "text": str(e)}

    try:
        return resp.json()
    except Exception:
        return {"status_code": resp.status_code, "text": resp.text}

@op(
    required_resource_keys={"report_utils"},
    ins={"file_path": In(str)},
    out=Out(dict),
)
def upload_report_instance_saima_short_op(context, file_path: str) -> Dict[str, Any]:
    cfg = context.op_config or {}

    dates = context.resources.report_utils.get_utils()
    rd = dates.ReportDate
    if isinstance(rd, str):
        report_day = datetime.strptime(rd[:10], "%Y-%m-%d").date()
    elif isinstance(rd, datetime):
        report_day = rd.date()
    else:
        report_day = rd  # date

    day_str = report_day.strftime("%Y.%m.%d")

    tz_name = cfg.get("timezone") or os.getenv("REPORTS_TZ") or "Asia/Bishkek"
    context.log.info(f"День отчёта: {day_str} (from report_utils.ReportDate, TZ={tz_name})")

    base_url = cfg.get("base_url") or os.getenv("REPORTS_API_BASE", "https://rv.o.kg")
    report_id = int(cfg.get("report_id", 15))
    api_key = cfg.get("api_key") or os.getenv("REPORTS_API_KEY") or os.getenv("RV_REPORTS_API_KEY")
    if not api_key:
        raise Failure("Не задан API ключ (REPORTS_API_KEY / RV_REPORTS_API_KEY или op_config.api_key).")

    timeout_connect = int(cfg.get("timeout_connect", 10))
    timeout_read = int(cfg.get("timeout_read", 120))
    verify_ssl = bool(cfg.get("verify_ssl", True))

    title = cfg.get("title") or "SAIMA | Сокращенная общая ежедневная статистика"

    url = f"{base_url.rstrip('/')}/api/report-instances/upload/{report_id}"
    headers = {"x-api-key": api_key}
    data = {"title": title, "day": day_str}

    try:
        with open(file_path, "rb") as f:
            files = {"file": (os.path.basename(file_path), f, "application/pdf")}
            resp = requests.post(url, headers=headers, data=data, files=files,
                                 timeout=(timeout_connect, timeout_read), verify=verify_ssl)
    except Exception as e:
        return {"status_code": -1, "text": str(e)}

    try:
        return resp.json()
    except Exception:
        return {"status_code": resp.status_code, "text": resp.text}