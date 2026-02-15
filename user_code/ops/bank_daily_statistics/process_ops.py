import requests
from dagster import op, In, Out, Failure

from resources import ReportUtils
from utils.report_data_collector import get_main_aggregation_data, get_main_aggregation_data_filtered, \
    get_average_handling_time_data, get_pst_cea_data, get_csi_data, get_occupancy_data_filtered, get_ur_data, \
    get_rc_data, get_daily_detailed_data, get_average_handling_time_data_filtered_group, get_mp_data, \
    render_pst_daily_profile, get_rc_data_filtered, get_ivr_transitions

import os

from typing import Any, Dict

from datetime import date, timedelta, datetime

from zoneinfo import ZoneInfo

from utils.mail.mail_sender import ReportsApiConfig, ReportInstanceUploader

connection = 'naumen3'
ost_position_ids = [36]
pst_position_ids = [46]
mp_project_ids = [5, 11, 15]
omni_project_ids = [5, 11, 15]
ost_time_table_view_id = [9]
pst_time_table_view_id = [18]
promt_ids = [148, 147, 146, 145, 144, 143]
groups = ["Money", "Bank", "O!Bank"]
channels = ['cp']
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

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def main_data_dynamic_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    main_data_dynamic = get_main_aggregation_data(context.resources.source_dwh, groups, 'day', start_date, end_date, report_date)
    context.log.info(f"main_data_dynamic = {main_data_dynamic}")
    return main_data_dynamic

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def main_data_dynamic_filtered_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    main_data_dynamic_filtered = get_main_aggregation_data_filtered(context.resources.source_dwh, groups, 'day', start_date, end_date)
    context.log.info(f'main_data_dynamic_filtered = {main_data_dynamic_filtered}')
    return main_data_dynamic_filtered

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def main_data_month_general_op1(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    main_data_month_general = get_main_aggregation_data(context.resources.source_dwh, groups, 'month', start_date, end_date, report_date)
    context.log.info(f'main_data_month_general = {main_data_month_general}')
    return main_data_month_general

@op
def service_level_current_month_general_op(context, main_data_month_general):
    service_level_current_month_general = round(main_data_month_general[0]['SL'], 1)
    context.log.info(f"service_level_current_month_general = {service_level_current_month_general}")
    return service_level_current_month_general

@op
def handled_calls_current_month_general_op(context, main_data_month_general):
    handled_calls_current_month_general = round(main_data_month_general[0]['ACD'], 1)
    context.log.info(f"handled_calls_current_month_general = {handled_calls_current_month_general}")
    return handled_calls_current_month_general

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def handling_times_data_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    handling_times_data = get_average_handling_time_data(conn=context.resources.source_dwh, project=groups, period='day', start_date=start_date, end_date=end_date)
    context.log.info(f"handling_times_data = {handling_times_data}")
    return handling_times_data

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def handling_times_month_data_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    handling_times_month_data = get_average_handling_time_data(conn=context.resources.source_dwh, project=groups, period='month', start_date=start_date, end_date=end_date)
    context.log.info(f"handling_times_month_data = {handling_times_month_data}")
    return handling_times_month_data

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def average_handling_time_current_month_rounded_value_op(context, handling_times_month_data):
    average_handling_time_current_month_rounded_value = round(handling_times_month_data['items'][0]['average_handling_time'], 1)
    context.log.info(f"average_handling_time_current_month_rounded_value = {average_handling_time_current_month_rounded_value}")
    return average_handling_time_current_month_rounded_value

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def average_ringing_time_current_month_rounded_value_op(context, handling_times_month_data):
    average_ringing_time_current_month_rounded_value = round(handling_times_month_data['items'][0]['average_ringing_time'], 1)
    context.log.info(f"average_ringing_time_current_month_rounded_value = {average_ringing_time_current_month_rounded_value}")
    return average_ringing_time_current_month_rounded_value

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def average_holding_time_current_month_rounded_value_op(context, handling_times_month_data):
    average_holding_time_current_month_rounded_value = round(handling_times_month_data['items'][0]['average_holding_time'], 1)
    context.log.info(f"average_holding_time_current_month_rounded_value = {average_holding_time_current_month_rounded_value}")
    return average_holding_time_current_month_rounded_value

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def cea_data_op1(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    cea_data = get_pst_cea_data(conn=context.resources.source_dwh, period='day', position=ost_position_ids, start_date=start_date, end_date=end_date)
    context.log.info(f"cea_data = {cea_data}")
    return cea_data

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def cea_current_month_value_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    cea_data_month = get_pst_cea_data(conn=context.resources.source_dwh, period='month', position=ost_position_ids, start_date=start_date, end_date=end_date)
    cea_current_month_value = round(cea_data_month['items'][0]['critical_error_accuracy'], 1)
    context.log.info(f"cea_current_month_value = {cea_current_month_value}")
    return cea_current_month_value

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def csi_data_op1(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    csi_data = get_csi_data(conn=context.resources.source_dwh, period='day', group=groups, channels=[connection], start_date=start_date, end_date=end_date)
    context.log.info(f"csi_data = {csi_data}")
    return csi_data

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def csi_month_data_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    csi_month_data = get_csi_data(conn=context.resources.source_dwh, period='month', group=groups, channels=[connection], start_date=start_date, end_date=end_date)
    context.log.info(f"csi_month_data = {csi_month_data}")
    return csi_month_data

@op
def csi_current_month_value_op(context, csi_month_data):
    if csi_month_data[1]:
        csi_current_month_value = round(csi_month_data[1]['customer_satisfaction_index'], 1)
    else:
        csi_current_month_value = 0

    context.log.info(f"csi_current_month_value = {csi_current_month_value}")
    return csi_current_month_value

@op
def csi_conversion_current_month_value_op(context, csi_month_data):
    if csi_month_data[1]:
        csi_conversion_current_month_value = round(csi_month_data[1]['conversion'], 1)
    else:
        csi_conversion_current_month_value = 0

    context.log.info(f"csi_conversion_current_month_value = {csi_conversion_current_month_value}")
    return csi_conversion_current_month_value

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def ost_ur_data_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    ost_ur_data = get_ur_data(conn=context.resources.source_dwh, naumen_conn_1=context.resources.source_naumen_1, naumen_conn_3=context.resources.source_naumen_3, group=groups, period='day', position=ost_position_ids, view_ids=ost_time_table_view_id, start_date=start_date, end_date=end_date)
    context.log.info(f'ost_ur_data = {ost_ur_data}')
    return ost_ur_data

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def ost_ur_month_data_rounded_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    ost_ur_data = get_ur_data(conn=context.resources.source_dwh, naumen_conn_1=context.resources.source_naumen_1, naumen_conn_3=context.resources.source_naumen_3,  group=groups, period='month', position=ost_position_ids, view_ids=ost_time_table_view_id, start_date=start_date, end_date=end_date)
    context.log.info(f"ost_ur_data (month) = {ost_ur_data}")
    ost_ur_month_data_rounded = round(ost_ur_data['utilization_rate'], 1)
    context.log.info(f"ost_ur_month_data_rounded = {ost_ur_month_data_rounded}")
    return ost_ur_month_data_rounded

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'source_cp', 'report_utils'})
def ost_occupancy_data_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    ost_occupancy_data = get_occupancy_data_filtered(conn=context.resources.source_cp, bpm_conn=context.resources.source_dwh, naumen_conn_1=context.resources.source_naumen_1, naumen_conn_3=context.resources.source_naumen_3,  group=groups, period='day', positions=ost_position_ids, view_ids=ost_time_table_view_id)
    context.log.info(f"ost_occupancy_data = {ost_occupancy_data}")
    return ost_occupancy_data

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'source_cp', 'report_utils'})
def ost_occupancy_month_data_rounded_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    ost_occupancy_month = get_occupancy_data_filtered(conn=context.resources.source_cp, bpm_conn=context.resources.source_dwh, naumen_conn_1=context.resources.source_naumen_1, naumen_conn_3=context.resources.source_naumen_3, group=groups, period='month', positions=ost_position_ids, view_ids=ost_time_table_view_id, start_date=start_date, end_date=end_date)
    ost_occupancy_month_data_rounded = round(ost_occupancy_month['items'][0]['occupancy'], 1)
    context.log.info(f"ost_occupancy_month_data_rounded = {ost_occupancy_month_data_rounded}")
    return ost_occupancy_month_data_rounded

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def ost_rc_data_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    ost_rc_data = get_rc_data(conn=context.resources.source_dwh, period='day', group=groups, start_date=start_date, end_date=end_date)
    context.log.info(f"ost_rc_data = {ost_rc_data}")
    return ost_rc_data

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def ost_rc_current_month_value_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    ost_rc_month_data = get_rc_data(conn=context.resources.source_dwh, period='month', group=groups, start_date=start_date, end_date=end_date)
    ost_rc_current_month_value = round(ost_rc_month_data['items'][0]['repeat_calls'], 1)
    context.log.info(f"ost_rc_current_month_value = {ost_rc_current_month_value}")
    return ost_rc_current_month_value

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'source_cp', 'report_utils'})
def detailed_daily_data_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    detailed_daily_data = get_daily_detailed_data(conn=context.resources.source_cp, bpm_conn=context.resources.source_dwh, naumen_conn_1=context.resources.source_naumen_1, naumen_conn_3 = context.resources.source_naumen_3,
                                                  group=groups, views=ost_time_table_view_id, positions=ost_position_ids, channels=[connection], start_date=start_date, end_date=end_date, report_date=report_date)
    context.log.info(f"detailed_daily_data = {detailed_daily_data}")
    return detailed_daily_data

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def merged_main_data_month_general_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    merged_main_data_month_general = get_main_aggregation_data(conn=context.resources.source_dwh, group=groups, period='month', start_date=start_date, end_date=end_date, report_date=report_date)
    context.log.info(f"merged_main_data_month_general = {merged_main_data_month_general}")
    return merged_main_data_month_general

@op
def sl_current_month_joined_op(context, merged_main_data_month):
    sl_current_month_joined = round(merged_main_data_month[0]["SL"], 1)
    context.log.info(f"sl_current_month_joined = {sl_current_month_joined}")
    return sl_current_month_joined

@op
def hcr_current_month_joined_op(context, merged_main_data_month):
    hcr_current_month_joined = round(merged_main_data_month[0]["ACD"], 1)
    context.log.info(f"hcr_current_month_joined = {hcr_current_month_joined}")
    return hcr_current_month_joined

@op
def ssr_current_month_joined_op(context, merged_main_data_month):
    ssr_current_month_joined = round(merged_main_data_month[0]["SSR"], 1)
    context.log.info(f"ssr_current_month_joined = {ssr_current_month_joined}")
    return ssr_current_month_joined

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def merged_aht_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    merged_aht = get_average_handling_time_data(conn=context.resources.source_dwh, project=groups, period='day', start_date=start_date, end_date=end_date)
    context.log.info(f"merged_aht={merged_aht}")
    return merged_aht

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def aht_by_project_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    aht_by_project = get_average_handling_time_data_filtered_group(conn=context.resources.source_dwh, project=groups, period='day', start_date=start_date, end_date=end_date)
    context.log.info(f"aht_by_project = {aht_by_project}")
    return aht_by_project

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def merged_aht_month_rounded_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    merged_aht = get_average_handling_time_data(conn=context.resources.source_dwh, project=groups, period='month', start_date=start_date, end_date=end_date)
    merged_aht_month_rounded = round(merged_aht['items'][0]['average_handling_time'], 1)
    context.log.info(f"merged_aht_month_rounded = {merged_aht_month_rounded}")
    return merged_aht_month_rounded

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def merged_ost_rc_data_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    merged_ost_rc_data = get_rc_data(conn=context.resources.source_dwh, period='day', group=groups, start_date=start_date, end_date=end_date)
    context.log.info(f"merged_ost_rc_data = {merged_ost_rc_data}")
    return merged_ost_rc_data

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def merged_ost_rc_data_filtered_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    merged_ost_rc_data_filtered = get_rc_data_filtered(conn=context.resources.source_dwh, period='day', group=groups, start_date=start_date, end_date=end_date)
    context.log.info(f"merged_ost_rc_data = {merged_ost_rc_data_filtered}")
    return merged_ost_rc_data_filtered

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def merged_ost_rc_current_month_data_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    merged_ost_month_rc_data = get_rc_data(conn=context.resources.source_dwh, period='month', group=groups, start_date=start_date, end_date=end_date)
    context.log.info(f"merged_ost_month_rc_data={merged_ost_month_rc_data}")
    merged_ost_rc_current_month_data = round(merged_ost_month_rc_data['items'][0]['repeat_calls'], 1)
    context.log.info(f"merged_ost_rc_current_month_data = {merged_ost_rc_current_month_data}")
    return merged_ost_rc_current_month_data

@op
def max_ivr_total_op1(context, main_data_dynamic):
    max_ivr_total = max(d["ivr_total"] for d in main_data_dynamic)
    context.log.info(f"max_ivr_total = {max_ivr_total}")
    return max_ivr_total

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def mp_data_messengers_aggregated_op1(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    mp_data_messengers_aggregated = get_mp_data(conn=context.resources.source_dwh, period='day', start_date=start_date, end_date=end_date)
    context.log.info(f'mp_data_messengers_aggregated = {mp_data_messengers_aggregated}')
    return mp_data_messengers_aggregated

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def mp_month_data_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    mp_month_data = get_mp_data(conn=context.resources.source_dwh, period='month', start_date=start_date, end_date=end_date)
    context.log.info(f'mp_month_data = {mp_month_data}')
    return mp_month_data

@op
def month_average_reaction_time_op1(context, mp_month_data):
    month_average_reaction_time = round(mp_month_data[0]['average_reaction_time'] if mp_month_data else 0, 1)
    context.log.info(f"month_average_reaction_time = {month_average_reaction_time}")
    return month_average_reaction_time

@op
def month_average_speed_to_answer_op1(context, mp_month_data):
    month_average_speed_to_answer = round(mp_month_data[0]['average_speed_to_answer'] if mp_month_data else 0, 1)
    context.log.info(f"month_average_speed_to_answer = {month_average_speed_to_answer}")
    return month_average_speed_to_answer

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def pst_ur_data_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    pst_ur_data = get_ur_data(conn=context.resources.source_dwh, naumen_conn_1=context.resources.source_naumen_1, naumen_conn_3=context.resources.source_naumen_3,  group=groups, period='day', position=pst_position_ids,
                              view_ids=pst_time_table_view_id, start_date=start_date, end_date=end_date)
    context.log.info(f"pst_ur_data = {pst_ur_data}")
    return pst_ur_data

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def pst_ur_month_data_rounded_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    pst_ur_month_data = get_ur_data(group=groups, naumen_conn_1=context.resources.source_naumen_1, naumen_conn_3=context.resources.source_naumen_3,  period='month', view_ids=pst_time_table_view_id, position=pst_position_ids, conn=context.resources.source_dwh, start_date=start_date, end_date=end_date)
    pst_ur_month_data_rounded = round(pst_ur_month_data['utilization_rate'] if pst_ur_month_data else 0, 1)
    context.log.info(f"pst_ur_month_data_rounded = {pst_ur_month_data_rounded}")
    return pst_ur_month_data_rounded

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def pst_cea_data_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    pst_cea_data = get_pst_cea_data(conn=context.resources.source_dwh, period='day', position=pst_position_ids, start_date=start_date, end_date=end_date)
    context.log.info(f"pst_cea_data = {pst_cea_data}")
    return pst_cea_data

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def pst_cea_current_month_value_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    pst_cea_data_month = get_pst_cea_data(conn=context.resources.source_dwh, period='month', position=pst_position_ids, start_date=start_date, end_date=end_date)
    pst_cea_current_month_value = round(pst_cea_data_month['items'][0]['critical_error_accuracy'], 1)
    context.log.info(f'pst_cea_current_month_value = {pst_cea_current_month_value}')
    return pst_cea_current_month_value

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def pst_csi_data_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    pst_csi_data = get_csi_data(conn=context.resources.source_dwh, period='day', group=groups, channels=channels, start_date=start_date, end_date=end_date)
    context.log.info(f"pst_csi_data = {pst_csi_data}")
    return pst_csi_data

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'report_utils'})
def pst_csi_month_data_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    pst_csi_month_data = get_csi_data(conn=context.resources.source_dwh, period='month', group=groups, channels=channels, start_date=start_date, end_date=end_date)
    return pst_csi_month_data

@op
def pst_csi_current_month_value_op(context, pst_csi_month_data):
    if pst_csi_month_data:
        pst_csi_current_month_value = round(pst_csi_month_data[1]['customer_satisfaction_index'], 1)
    else:
        pst_csi_current_month_value = 0

    context.log.info(f'pst_csi_current_month_value = {pst_csi_current_month_value}')
    return pst_csi_current_month_value

@op
def pst_csi_conversion_current_month_value_op(context, pst_csi_month_data):
    if pst_csi_month_data:
        pst_csi_conversion_current_month_value = round(pst_csi_month_data[1]['conversion'], 1)
    else:
        pst_csi_conversion_current_month_value = 0

    context.log.info(f'pst_csi_conversion_current_month_value = {pst_csi_conversion_current_month_value}')
    return pst_csi_conversion_current_month_value

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1', 'source_cp', 'report_utils'})
def pst_detailed_daily_data_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    return render_pst_daily_profile(conn=context.resources.source_cp, bpm_conn=context.resources.source_dwh, naumen_conn_1=context.resources.source_naumen_1, naumen_conn_3=context.resources.source_naumen_3, project_ids=mp_project_ids,
                                    groups=groups, channels=channels, period='hour', timetable_views=pst_time_table_view_id, position_ids=pst_position_ids, start_date=start_date, end_date=end_date, report_date=report_date)


@op(required_resource_keys={'source_nod_dinosaur', 'report_utils'})
def ivr_transitions_bank_op(context):
    dates = context.resources.report_utils.get_utils()
    start_date = dates.StartDate
    end_date = dates.EndDate
    date_list = dates.Dates
    report_date = dates.ReportDate
    month_start = dates.MonthStart
    month_end = dates.MonthEnd
    formatted_dates = dates.FormattedDates

    result = get_ivr_transitions(nod_dinosaur_conn=context.resources.source_nod_dinosaur, callers=callers, parents=promt_ids, period='week', start_date=start_date, end_date=end_date, report_date=report_date, date_list=date_list)
    context.log.info(f'ivr_transitions = {result}')
    return result

@op(
    required_resource_keys={"report_utils"},
    ins={"file_path": In(str, description="Абсолютный путь к PDF файлу.")},
    out=Out(dict, description="Ответ API как dict (JSON) либо {'status_code', 'text'}"),
    description=(
        "Загружает PDF-отчёт на /api/report-instances/upload/{report_id} (RV) по схеме как в curl: "
        "заголовок x-api-key и форма file/title/day. "
        "Дата 'day' по умолчанию = (сегодня в локальной TZ) − 1, формат YYYY.MM.DD."
    ),
)
def upload_report_instance_bank_op(context, file_path: str) -> Dict[str, Any]:
    cfg = context.op_config or {}

    tz_name = cfg.get("timezone") or os.getenv("REPORTS_TZ") or "Asia/Bishkek"
    dates = context.resources.report_utils.get_utils()
    rd = dates.ReportDate
    if isinstance(rd, str):
        report_day = datetime.strptime(rd[:10], "%Y-%m-%d").date()
    elif isinstance(rd, datetime):
        report_day = rd.date()
    else:
        report_day = rd  # date

    day_str = report_day.strftime("%Y.%m.%d")
    context.log.info(f"День отчёта: {day_str} (TZ={tz_name})")

    base_url = cfg.get("base_url") or os.getenv("REPORTS_API_BASE", "https://rv.o.kg")
    report_id = int(cfg.get("report_id", 8))
    api_key = cfg.get("api_key") or os.getenv("REPORTS_API_KEY") or os.getenv("RV_REPORTS_API_KEY")
    if not api_key:
        raise Failure(description="Не задан API ключ. Укажите op_config.api_key или переменную окружения REPORTS_API_KEY / RV_REPORTS_API_KEY.")

    timeout_connect = int(cfg.get("timeout_connect", 10))
    timeout_read = int(cfg.get("timeout_read", 120))
    verify_ssl = bool(cfg.get("verify_ssl", True))

    title = cfg.get("title") or "O!BANK | Общая ежедневная статистика "

    url = f"{base_url.rstrip('/')}/api/report-instances/upload/{report_id}"
    headers = {"x-api-key": api_key}
    data = {"title": title, "day": day_str}

    context.log.info(f"POST {url} (verify_ssl={verify_ssl})")

    try:
        with open(file_path, "rb") as f:
            files = {"file": (os.path.basename(file_path), f, "application/pdf")}
            resp = requests.post(
                url,
                headers=headers,
                data=data,
                files=files,
                timeout=(timeout_connect, timeout_read),
                verify=verify_ssl,
            )
    except Exception as e:
        context.log.error(f"Ошибка при загрузке: {e}")
        return {"status_code": -1, "text": str(e)}

    context.log.info(f"Статус ответа: {resp.status_code}")
    try:
        return resp.json()
    except Exception:
        return {"status_code": resp.status_code, "text": resp.text}