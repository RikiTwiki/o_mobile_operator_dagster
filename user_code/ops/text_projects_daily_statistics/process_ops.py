import pandas as pd
import requests
from dagster import op, In, Out, AssetKey, Output, Float, Failure
from typing import List, Any, Tuple, Dict, OrderedDict

from pathlib import Path

import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta, date
from datetime import datetime as dt

from utils.path import BASE_SQL_PATH

from resources import resources

from utils.getters.messaging_platform_data_getter import MessagingPlatformDataGetter

from utils.statistic_dates import StatisticDates

from utils.array_operations import left_join_array

from datetime import datetime

import os
import pickle

from utils.transformers.mp_report import _to_dt

from utils.transformers.mp_report import to_hms

from utils.getters.mp_occupancy_data_getter import MPOccupancyDataGetter

from utils.mail.mail_sender import ReportsApiConfig, ReportInstanceUploader

from zoneinfo import ZoneInfo

from typing import Union

@op(required_resource_keys={'source_dwh', 'report_utils', 'source_cp'})
def process_aggregation_project_data_op(
    context,
    start_date: datetime,
    end_date: datetime,
    report_date: datetime,
    month_start: datetime,
    month_end: datetime,
    project_ids: List[int],
) -> Dict[str, Any]:
    """
    Собирает данные по дням и месяцам используя MessagingPlatformDataGetter и left_join_array из report_utils.
    """
    # 1) Инициализируем getter и устанавливаем проекты
    mp_getter = MessagingPlatformDataGetter.prepare_mp_data_getter(
        trunc='day',
        start_date=start_date,
        end_date=end_date,
        report_date=report_date,
        conn=context.resources.source_dwh,
    )
    mp_getter.project_ids = project_ids

    context.log.info(f"project_ids = {project_ids}")

    mp_getter.context = context

    # 2) День: агрегированные данные
    day_data = mp_getter.get_aggregated()

    context.log.info(f"day_data = {day_data}")

    mp_getter = MessagingPlatformDataGetter.prepare_mp_data_getter(
        trunc='day',
        start_date=start_date,
        end_date=end_date,
        report_date=report_date,
        conn=context.resources.source_cp,
        project_ids=project_ids
    )

    # 3) День: вошедшие чаты
    entered = mp_getter.get_entered_chats()
    day_data = left_join_array(day_data, entered, 'date')
    context.log.info(f"start_date: {mp_getter.start_date}, end_date {mp_getter.end_date}, date_format {mp_getter.date_format}, project_ids {mp_getter.project_ids}, trunc {mp_getter.trunc}")

    context.log.info(f"entered = {entered}")

    mp_getter = MessagingPlatformDataGetter.prepare_mp_data_getter(
        trunc='day',
        report_date=report_date,
        start_date=start_date,
        end_date=end_date,
        conn=context.resources.source_dwh,
        project_ids=project_ids
    )

    # 4) День: обработанные чаты
    handled = mp_getter.get_all_handled_chats()
    day_data = left_join_array(day_data, handled, 'date')

    context.log.info(f"handled = {handled}")

    # 5) День: чаты, обработанные ботом
    bot_handled = mp_getter.get_bot_handled_chats('day', start_date, end_date, project_ids)
    day_data = left_join_array(day_data, bot_handled, 'date')

    context.log.info(f"bot_handled = {bot_handled}")

    # 6) Месяц: агрегированные данные
    mp_getter_month = MessagingPlatformDataGetter.prepare_mp_data_getter(
        trunc='month',
        report_date=report_date,
        start_date=month_start,
        end_date=month_end,
        conn=context.resources.source_dwh,
        project_ids=project_ids
    )

    mp_getter_month.context = context


    month_data = mp_getter_month.get_mp_aggregated()

    # 7) Вычисляем средние показатели
    if month_data:
        avg_reaction = round(month_data[0].get('average_reaction_time', 0))
        avg_speed = round(month_data[0].get('average_speed_to_answer', 0))
    else:
        avg_reaction = avg_speed = 0


    result = {
        'dayData': day_data,
        'monthAverageReactionTime': avg_reaction,
        'monthAverageSpeedToAnswer': avg_speed, }


    context.log.info(f"return: {result}")

    return {
        'dayData': day_data,
        'monthAverageReactionTime': avg_reaction,
        'monthAverageSpeedToAnswer': avg_speed,
    }

@op
def get_month_avg_reaction_time(data):
    return data["monthAverageReactionTime"]

@op
def get_month_avg_speed_to_answer(data):
    return data["monthAverageSpeedToAnswer"]

@op(required_resource_keys={'source_dwh', 'report_utils'})
def get_service_rate_by_day(context,
    start_date: datetime,
    end_date: datetime,
    project_ids: list[int]):
    mp_getter = MessagingPlatformDataGetter.prepare_mp_data_getter(
        trunc='day',
        start_date=start_date,
        end_date=end_date,
        conn=context.resources.source_dwh,
        project_ids=project_ids
    )
    result = mp_getter.get_self_service_rate()
    context.log.info(f"result = {result}")
    return result

@op(required_resource_keys={'source_dwh', 'report_utils'})
def get_service_rate_by_month(context,
    start_date: datetime,
    end_date: datetime,
    project_ids: list[int]):
    mp_getter = MessagingPlatformDataGetter.prepare_mp_data_getter(
        trunc='month',
        start_date=start_date,
        end_date=end_date,
        conn=context.resources.source_dwh,
        project_ids=project_ids
    )
    result = mp_getter.get_self_service_rate()
    context.log.info(f"result = {result}")
    return result

@op(out=Out(List[Dict[str, Any]]))
def load_self_service_rate(context) -> List[Dict[str, Any]]:
    # Укажите точное имя файла с pickle-данными
    path = "/opt/dagster/mp/all_service_rate_by_day"
    if not os.path.isfile(path):
        context.log.error(f"Файл pickle не найден: {path}")
        return []

    try:
        with open(path, "rb") as f:
            data = pickle.load(f)
    except Exception as e:
        context.log.error(f"Не удалось прочитать pickle-файл {path}: {e}")
        return []

    # Предполагаем, что внутри — список словарей
    if not isinstance(data, list):
        context.log.warning(f"Ожидался список, получили {type(data)}")
    else:
        context.log.info(f"Загружено {len(data)} записей self_service_rate")

    return data

@op(out=Out(float))
def load_month_self_service_rate(context) -> float:
    path = "/opt/dagster/mp/service_rate_all_by_month"
    if not os.path.isfile(path):
        context.log.error(f"Файл pickle не найден: {path}, возвращаю 0.0")
        return 0.0

    try:
        with open(path, "rb") as f:
            data = pickle.load(f)
    except Exception as e:
        context.log.error(f"Ошибка при чтении pickle-файла {path}: {e}")
        return 0.0

    # Ожидаем список словарей
    if not isinstance(data, list) or not data:
        context.log.warning(
            f"Ожидался непустой список, получили {type(data).__name__}, возвращаю 0.0"
        )
        return 0.0

    first = data[0]
    rate = first.get("self_service_rate")
    if rate is None:
        context.log.warning(
            "В первом словаре нет ключа 'self_service_rate', возвращаю 0.0"
        )
        return 0.0

    try:
        rate_f = float(rate)
    except Exception:
        context.log.warning(f"Не удалось привести {rate!r} к float, возвращаю 0.0")
        return 0.0

    context.log.info(f"Значение месячного self-service rate: {rate_f}")
    return rate_f

@op(required_resource_keys={'source_dwh', 'report_utils', 'source_cp'})
def hour_mp_data_messenger_detailed_op(
    context,
    report_date: datetime,
    start_date: datetime,
    end_date: datetime,
    project_ids: list[int],
) -> List[Dict[str, Any]]:
    mp_getter = MessagingPlatformDataGetter.prepare_mp_data_getter(
        trunc='hour',
        start_date=report_date,
        end_date=end_date,
        report_date=report_date,
        conn=context.resources.source_dwh,
        project_ids=project_ids
    )
    mp_getter.context = context

    context.log.info(f'Dates: {start_date, mp_getter.start_date, report_date, end_date}')

    context.log.info(f'PARAMS: {mp_getter.trunc, mp_getter.date_field, mp_getter.date_format}')

    # 1) агрегаты
    hour_mp_data_messengers_detailed = mp_getter.get_full_aggregated()
    context.log.info(f"hour_mp_data_messengers_detailed = {hour_mp_data_messengers_detailed}")

    mp_getter = MessagingPlatformDataGetter.prepare_mp_data_getter(
        trunc='hour',
        start_date=report_date,
        end_date=end_date,
        report_date=report_date,
        conn=context.resources.source_cp,
        project_ids=project_ids

    )
    # 2) вошедшие чаты
    hour_data_messengers_entered_chats = mp_getter.get_entered_chats()
    context.log.info(f"hour_data_messengers_entered_chats = {hour_data_messengers_entered_chats}")

    # 3) join по 'date'
    hour_mp_data_messengers_detailed = left_join_array(
        hour_mp_data_messengers_detailed,
        hour_data_messengers_entered_chats,
        'date'
    )
    context.log.info(f"Report date = {report_date}")
    for row in hour_mp_data_messengers_detailed:
        row['date'] = to_hms(row.get('date'))

    context.log.info(f"return: {hour_mp_data_messengers_detailed}")
    return hour_mp_data_messengers_detailed

@op(required_resource_keys={'source_dwh', 'report_utils'})
def day_data_messengers_detailed_op(context, start_date, end_date, project_ids):
    mp_getter = MessagingPlatformDataGetter.prepare_mp_data_getter(
        trunc='day',
        start_date=start_date,
        end_date=end_date,
        conn=context.resources.source_dwh,
        project_ids=project_ids
    )

    result = mp_getter.get_detailed_with_bot()
    context.log.info(f"return: {result}")
    return result

@op(required_resource_keys={'source_dwh', 'report_utils', 'source_cp'})
def day_data_messengers_entered_chats_op(context, start_date, end_date, project_ids):
    mp_getter = MessagingPlatformDataGetter.prepare_mp_data_getter(
        trunc='day',
        start_date=start_date,
        end_date=end_date,
        conn=context.resources.source_dwh,
        project_ids=project_ids
    )

    result = mp_getter.get_project_entered_chats()
    context.log.info(f"return: {result}")
    return result

@op(required_resource_keys={'source_dwh', 'report_utils'})
def day_data_get_users_detailed_op(context, start_date, end_date, project_ids):
    mp_getter = MessagingPlatformDataGetter.prepare_mp_data_getter(
        trunc="day",
        start_date=start_date,
        end_date=end_date,
        conn = context.resources.source_dwh,
        project_ids=project_ids
    )
    result = mp_getter.get_users_detailed()
    context.log.info(f'return: {result}')
    return result

@op(required_resource_keys={'source_dwh', 'report_utils'})
def day_mp_data_get_users_detailed_by_view_op(context, start_date, end_date, project_ids):
    end_date = end_date - timedelta(days=1)
    mp_getter = MessagingPlatformDataGetter.prepare_mp_data_getter(
        trunc="day",
        start_date=start_date,
        end_date=end_date,
        conn=context.resources.source_dwh,
        project_ids=project_ids
    )
    result = mp_getter.get_users_detailed_by_view()
    context.log.info(f'return: {result}')
    return result

@op(required_resource_keys={'source_dwh', 'report_utils', 'source_cp'})
def get_mp_statuses_op(context, start_date, end_date, month_start):
    mp_occupancy = MPOccupancyDataGetter(
        trunc="day",
        start_date=start_date,
        end_date=end_date,
        conn=context.resources.source_dwh,
        month_start=month_start,
        pst_positions=[10, 41, 46], )
    mp_occupancy.set_days_trunked_data()

    cp_occupancy = MPOccupancyDataGetter(
        trunc="day",
        start_date=start_date,
        end_date=end_date,
        conn=context.resources.source_cp,
        month_start=month_start,
        pst_positions=[10, 41, 46], )
    cp_occupancy.set_days_trunked_data()

    # cross-day логины (исключаемые смены)
    cross_day_users = mp_occupancy.get_active_user_logins([9, 10, 11, 12, 13, 19])
    all_users       = mp_occupancy.get_active_user_logins()

    cross_logins = {row.get("login") for row in cross_day_users if row.get("login")}
    all_logins   = [row.get("login") for row in all_users if row.get("login")]

    # array_diff($allLogins, $crossDayLogins)
    normal_shift_logins = [lg for lg in all_logins if lg not in cross_logins]
    context.log.info(f"normal_shift_logins = {normal_shift_logins}")

    normal_shift_statuses = cp_occupancy.get_user_statuses_by_positions(normal_shift_logins)

    cross_day_shift_sheets    = mp_occupancy.get_active_user_sheets()
    cross_day_shift_statuses  = cp_occupancy.get_sheet_statuses_by_positions(cross_day_shift_sheets)

    # группировка cross-day, как collect()->groupBy()->map()->values()->toArray()
    grouped_cross_day_shift_statuses = []
    if cross_day_shift_statuses:
        df = pd.DataFrame(cross_day_shift_statuses)

        need_cols = {"date", "login", "code"}
        if need_cols.issubset(df.columns):
            df = df[df["date"].notna() & df["login"].notna() & df["code"].notna()]

            df["seconds"] = pd.to_numeric(df.get("seconds"), errors="coerce").fillna(0).astype("Int64")
            df["minutes"] = pd.to_numeric(df.get("minutes"), errors="coerce").fillna(0.0).astype("float64")
            df["hours"]   = pd.to_numeric(df.get("hours"),   errors="coerce").fillna(0.0).astype("float64")

            out = (
                df.groupby(["date", "login", "code"], as_index=False, sort=False)
                  .agg({"seconds": "sum", "minutes": "sum", "hours": "sum"})
            )
            out["minutes"] = out["minutes"].round(2)
            out["hours"]   = out["hours"].round(3)

            grouped_cross_day_shift_statuses = out.to_dict(orient="records")

    # array_merge
    statuses = normal_shift_statuses + grouped_cross_day_shift_statuses

    # leftJoinArray($statuses, $allUsers, 'login')
    df_statuses = pd.DataFrame(statuses)
    df_all      = pd.DataFrame(all_users)

    if not df_all.empty:
        df_all = (
            df_all.dropna(subset=["login"])
                 .drop_duplicates(subset=["login"], keep="first")
                 [["login", "supervisor", "abbreviation"]]
        )

    df_statuses = df_statuses.merge(df_all, how="left", on="login")
    statuses = df_statuses.to_dict(orient="records")

    # как array_keys($this->Statuses)
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
    ordered_codes = list(STATUSES.keys())

    # $statuses->where('code', $status)->values()->all()
    status_groups = {
        code: [row for row in statuses if row.get("code") == code]
        for code in ordered_codes
    }

    context.log.info(f"status_groups keys = {status_groups}")
    return status_groups

@op(
    out={
        "dates": Out(List[str], description="Сырые даты для рядов"),
        "formatted_dates": Out(List[str], description="Отформатированные даты для заголовков"),
    }
)
def compute_statistic_dates_op(context, start_date: datetime, end_date: datetime):
    stats = StatisticDates(start_date, end_date)
    yield Output(stats.dates, "dates")

    yield Output(stats.formatted_dates, "formatted_dates")

@op(
    ins={"month_start": In(datetime), "month_end": In(datetime)},
    out={
        "all":     Out(List[int]),
        "saima":   Out(List[int]),
        "general": Out(List[int]),
        "other":   Out(List[int]),
        "all_services": Out(List[int]),
        "bank_services": Out(List[int]),
    },
    description="Возвращает четыре набора project_ids"
)
def get_project_ids_op(context, month_start, month_end):

    all_project_ids = [1,4,5,8,9,10,11,12,15,20]

    saima_project_ids = [1]

    general_store_project_ids = [4,12]

    other_project_ids = [5,8,10,11,15,20,21]

    all_service_ids = [1, 8, 9]
    bank_service_ids = [8,9,10,15,20]

    return (
        all_project_ids,
        saima_project_ids,
        general_store_project_ids,
        other_project_ids,
        all_service_ids,
        bank_service_ids
    )

@op(
    out={
        "sd":  Out(dt),
        "ed":  Out(dt),
        "msd": Out(dt),
        "med": Out(dt),
        "rp": Out(dt)
    },
    required_resource_keys={"report_utils"},
)
def parse_dates(context):
    d = context.resources.report_utils.get_utils()
    sd  = _to_dt(getattr(d, "StartDate"))
    ed  = _to_dt(getattr(d, "EndDate"))
    msd = _to_dt(getattr(d, "MonthStart"))
    med = _to_dt(getattr(d, "MonthEnd"))
    rp = _to_dt(getattr(d,"ReportDate"))
    context.log.info(f"sd={sd} ed={ed} msd={msd} med={med} rp={rp}")
    yield Output(sd,  "sd")
    yield Output(ed,  "ed")
    yield Output(msd, "msd")
    yield Output(med, "med")
    yield Output(rp, "rp")

@op(
    required_resource_keys={"report_utils"},
    ins={"file_path": In(str)},
    out=Out(dict),
)
def upload_report_instance_op(context, file_path: str) -> Dict[str, Any]:
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
    report_id = int(cfg.get("report_id", 6))
    api_key = cfg.get("api_key") or os.getenv("REPORTS_API_KEY") or os.getenv("RV_REPORTS_API_KEY")
    if not api_key:
        raise Failure("Не задан API ключ (REPORTS_API_KEY / RV_REPORTS_API_KEY или op_config.api_key).")

    timeout_connect = int(cfg.get("timeout_connect", 10))
    timeout_read = int(cfg.get("timeout_read", 120))
    verify_ssl = bool(cfg.get("verify_ssl", True))

    title = cfg.get("title") or "ГПО | Общая ежедневная статистика"

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