from dagster import op, get_dagster_logger, In, Out, Failure
from datetime import datetime, date, timedelta
from collections import defaultdict
from resources import ReportUtils
from utils.array_operations import left_join_array
from utils.getters.average_handling_time_data_getter import AverageHandlingTimeDataGetter
from utils.getters.customer_satisfaction_index_data_getter import CustomerSatisfactionIndexDataGetter
from utils.getters.naumen_service_level_data_getter import NaumenServiceLevelDataGetter
from utils.getters.price_plans_data_getter import PricePlansDataGetter
from utils.getters.repeat_calls_data_getter import RepeatCallsDataGetter
from utils.getters.topics_data_getter import TopicsDataGetter

from typing import Dict, Any

from zoneinfo import ZoneInfo

import os

import requests

callers = [
        "*707",
        "707",
        "0705700700",
        "0312909000",
        "0706909000",
        "0770909101",
        "0554909900",
        "999",
        "0700000999",
        "5858",
        "*585",
        "0505585558",
        "799",
        "*799",
        "0702000799",
        "7878",
        "0501780078",
        "405",
        "0702000405",
        "0706988330",
        "555988330",
        "996770988330",
        "9999",
        "8008",
        "8833"
    ]
projects = [
            'General',
            'Agent',
            'Entrepreneurs',
            'Money',
            'Bank',
            'O!Bank',
            'TechSup',
            'Cheaper Together',
            'Terminals',
            'Saima',
            'AkchaBulak',
        ]

projects_names = "Все проекты"
dates = ReportUtils()
dates = dates.get_utils()
start_date = dates.StartDate
end_date = dates.EndDate
date_list = dates.Dates
report_date = dates.ReportDate
month_start = dates.MonthStart
month_end = dates.MonthEnd
start_date_year = dates.StartDateYear

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1'})
def main_data_dynamic_general_joined_monthly_op(context):
    get_main_data_dynamic = NaumenServiceLevelDataGetter(start_date=month_start, end_date=month_end, conn=context.resources.source_dwh)
    get_main_data_dynamic.set_days_trunked_data()
    get_main_data_dynamic.filter_projects_by_group_array(projects)
    main_data_dynamic_general = get_main_data_dynamic.get_aggregated_data()
    get_main_data_dynamic.reload_projects()
    get_main_data_dynamic.filter_projects_by_group_array(projects, 'IVR')
    main_data_dynamic_general_ivr = get_main_data_dynamic.get_aggregated_data_ivr()
    main_data_dynamic_general_joined = left_join_array(main_data_dynamic_general, main_data_dynamic_general_ivr, 'date')

    context.log.info(f"main_data_dynamic_general_joined = {main_data_dynamic_general_joined}")
    return main_data_dynamic_general_joined

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1'})
def main_data_month_dynamic_general_op(context):
    get_main_data_dynamic = NaumenServiceLevelDataGetter(start_date=start_date_year, end_date=month_end, conn=context.resources.source_dwh)
    get_main_data_dynamic.set_months_trunked_data()
    get_main_data_dynamic.filter_projects_by_group_array(projects)
    main_data_month_dynamic_general = get_main_data_dynamic.get_aggregated_data()
    context.log.info(f"main_data_month_dynamic_general = {main_data_month_dynamic_general}")
    return main_data_month_dynamic_general

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1'})
def main_data_dynamic_general_arpu_op(context):
    get_main_data_dynamic = NaumenServiceLevelDataGetter(start_date=start_date_year, end_date=month_end,
                                                       conn=context.resources.source_dwh)
    get_main_data_dynamic.set_months_trunked_data()
    get_main_data_dynamic.filter_projects_by_group_array(projects)
    main_data_month_dynamic_general = get_main_data_dynamic.get_arpu_trunked_data()
    context.log.info(f"main_data_month_dynamic_general = {main_data_month_dynamic_general}")
    return main_data_month_dynamic_general

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1'})
def main_data_month_dynamic_general_ivr_op(context):
    get_main_data_dynamic = NaumenServiceLevelDataGetter(start_date=start_date_year, end_date=month_end,
                                                       conn=context.resources.source_dwh)
    get_main_data_dynamic.set_months_trunked_data()
    get_main_data_dynamic.filter_projects_by_group_array(projects, 'IVR')
    main_data_month_dynamic_general_ivr = get_main_data_dynamic.get_aggregated_data_ivr()
    context.log.info(f"main_data_month_dynamic_general_ivr = {main_data_month_dynamic_general_ivr}")
    return main_data_month_dynamic_general_ivr

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1'})
def main_data_month_dynamic_general_joined_op(context, main_data_month_dynamic_general, main_data_month_dynamic_general_ivr):
    main_data_month_dynamic_general_joined = left_join_array(main_data_month_dynamic_general['items'], main_data_month_dynamic_general_ivr['items'], 'date')
    context.log.info(f"main_data_month_dynamic_general_joined = {main_data_month_dynamic_general_joined}")
    return main_data_month_dynamic_general_joined

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1'})
def topics_data_op(context):
    topics_data = TopicsDataGetter(start_date=month_start, end_date=month_end, conn=context.resources.source_dwh)
    topics_data.set_months_trunked_data()
    main_topics = topics_data.get_main_level_data(view_id=8)
    context.log.info(f"main_topics = {main_topics}")
    return main_topics

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1'})
def handling_times_data_month_general_op(context):
    get_handling_times_data = AverageHandlingTimeDataGetter(start_date=start_date_year, end_date=month_end, dwh_conn=context.resources.source_dwh)
    get_handling_times_data.set_months_trunked_data()
    get_handling_times_data.filter_projects_by_group_array(projects)
    handling_times_data_month_general = get_handling_times_data.get_aggregated_data()
    context.log.info(f"handling_times_data_month_general = {handling_times_data_month_general}")
    return handling_times_data_month_general

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1'})
def handling_times_data_month_general_arpu_op(context):
    get_handling_times_data = AverageHandlingTimeDataGetter(start_date=start_date_year, end_date=month_end,
                                                            dwh_conn=context.resources.source_dwh)
    get_handling_times_data.set_months_trunked_data()
    get_handling_times_data.filter_projects_by_group_array(projects)
    handling_times_data_month_general_arpu = get_handling_times_data.get_arpu_trunked_data()
    context.log.info(f"handling_times_data_month_general_arpu = {handling_times_data_month_general_arpu}")
    return handling_times_data_month_general_arpu

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1'})
def rc_data_dynamic_main_op(context):
    get_rc_data_dynamic_main = RepeatCallsDataGetter(start_date=start_date_year, end_date=month_end, dwh_conn=context.resources.source_dwh)
    get_rc_data_dynamic_main.set_months_trunked_data()
    rc_data_dynamic_main = get_rc_data_dynamic_main.get_aggregated_data(table='00:00:00', groups=projects)
    context.log.info(f"rc_data_dynamic_main = {rc_data_dynamic_main}")
    return rc_data_dynamic_main

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1'})
def csi_data_month_main_op(context):
    get_csi_data_month = CustomerSatisfactionIndexDataGetter(start_date=start_date_year, end_date=month_end, bpm_conn=context.resources.source_dwh)
    get_csi_data_month.set_months_trunked_data()
    get_csi_data_month.filter_projects_by_group_array(projects)
    csi_data_month = get_csi_data_month.get_aggregated_data()
    context.log.info(f"csi_data_month = {csi_data_month}")
    return csi_data_month


@op
def main_index_op(context, main_data_month_dynamic_general, handling_times_data_month_general, rc_data_dynamic_main,
                  csi_data_month_main):
    MainIndex = defaultdict(dict)

    for item in main_data_month_dynamic_general.get('items', []):
        d = item.get('date')
        ts = int(d.timestamp()) if isinstance(d, datetime) else int(datetime.fromisoformat(str(d)).timestamp())

        sl = item.get('SL')
        acd = item.get('ACD')

        if sl is not None:
            MainIndex[ts]['service_level'] = round(float(sl), 2)
        if acd is not None:
            MainIndex[ts]['handled_calls_rate'] = round(float(acd), 2)

    for item in handling_times_data_month_general.get('items', []):
        d = item.get('date')
        ts = int(d.timestamp()) if isinstance(d, datetime) else int(datetime.fromisoformat(str(d)).timestamp())
        aht = item.get('average_handling_time')
        if aht is not None:
            MainIndex[ts]['average_handling_time'] = round(float(aht), 2)

    for item in rc_data_dynamic_main.get('items', []):
        d = item.get('date')
        ts = int(d.timestamp()) if isinstance(d, datetime) else int(datetime.fromisoformat(str(d)).timestamp())
        rc = item.get('repeat_calls')
        if rc is not None:
            MainIndex[ts]['repeat_calls'] = round(float(rc), 2)

    for item in csi_data_month_main.get('items', []):
        d = item.get('date')
        ts = int(d.timestamp()) if isinstance(d, datetime) else int(datetime.fromisoformat(str(d)).timestamp())
        csi = item.get('customer_satisfaction_index')
        if csi is not None:
            MainIndex[ts]['customer_satisfaction_index'] = round(float(csi), 2)

    result = []
    for ts, vals in sorted(MainIndex.items()):
        v = dict(vals)
        result.append({
            'date': datetime.fromtimestamp(int(ts)).strftime('%Y-%m-%d'),
            'service_level': float(v.get('service_level', 0)),
            'handled_calls_rate': float(v.get('handled_calls_rate', 0)),
            'average_handling_time': float(v.get('average_handling_time', 0)),
            'repeat_calls': float(v.get('repeat_calls', 0)),
            'customer_satisfaction_index': float(v.get('customer_satisfaction_index', 0)),
        })

    context.log.info(f"Main Index: {result}")
    return result


@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1'})
def price_plans_with_bad_marks_op(context):
    price_plans = PricePlansDataGetter(start_date=month_start, end_date=month_end, bpm_conn=context.resources.source_dwh)
    price_plans.set_days_trunked_data()
    price_plans_with_bad_marks = price_plans.get_data_with_bad_marks()
    context.log.info(f"price_plans_with_bad_marks = {price_plans_with_bad_marks}")
    return price_plans_with_bad_marks

@op(
    ins={"file_path": In(str, description="Абсолютный путь к PDF файлу.")},
    out=Out(dict, description="Ответ API как dict (JSON) либо {'status_code', 'text'}"),
    description=(
        "Загружает PDF-отчёт на /api/report-instances/upload/{report_id} (RV) по схеме как в curl: "
        "заголовок x-api-key и форма file/title/day. "
        "Дата 'day' по умолчанию = (сегодня в локальной TZ) − 1, формат YYYY.MM.DD."
    ),
)
def upload_report_instance_general_monthly_op(context, file_path: str) -> Dict[str, Any]:
    cfg = context.op_config or {}

    tz_name = cfg.get("timezone") or os.getenv("REPORTS_TZ") or "Asia/Bishkek"
    tz = ZoneInfo(tz_name)
    if cfg.get("day"):
        day_str = str(cfg["day"])
    else:
        report_day: date = datetime.now(tz).date() - timedelta(days=1)
        day_str = report_day.strftime("%Y.%m.%d")
    context.log.info(f"День отчёта: {day_str} (TZ={tz_name})")

    base_url = cfg.get("base_url") or os.getenv("REPORTS_API_BASE", "https://rv.o.kg")
    report_id = int(cfg.get("report_id", 9))
    api_key = cfg.get("api_key") or os.getenv("REPORTS_API_KEY") or os.getenv("RV_REPORTS_API_KEY")
    if not api_key:
        raise Failure(description="Не задан API ключ. Укажите op_config.api_key или переменную окружения REPORTS_API_KEY / RV_REPORTS_API_KEY.")

    timeout_connect = int(cfg.get("timeout_connect", 10))
    timeout_read = int(cfg.get("timeout_read", 120))
    verify_ssl = bool(cfg.get("verify_ssl", True))

    title = cfg.get("title") or "ОТЧЕТ ПО GENERAL MONTHLY"

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