import base64
import math
import os
import re
from datetime import datetime, date
from decimal import Decimal
from io import BytesIO
from typing import Dict, Any, List
from datetime import datetime, date as _date

import numpy as np
import pandas as pd
from dagster import get_dagster_logger, Output, MetadataValue
from datetime import datetime as dt

from utils.templates.tables.MatplotlibTableConstructor2 import MatplotlibTableConstructor2, get_data_in_dates

from utils.path import BASE_SQL_PATH

def hourly_params(context) -> Dict:
    """
    Формирование SQL-параметров для почасовой агрегации
    """
    dates = context.resources.report_utils.get_utils()
    projects = context.resources.mp_daily_config.get_project_ids()
    positions = context.resources.mp_daily_config.get_position_ids()

    return {
        "trunc": "hour",
        "date_format": "HH24:MI:SS",
        "start_date": dates.StartDate,
        "end_date": dates.EndDate,
        "project_ids": projects,
        "timezone_adjustment_date": "2024-08-21",
        "position_ids": positions,
    }

def daily_params(context) -> Dict:
    """
    Формирование SQL-параметров для дневной агрегации
    """
    dates = context.resources.report_utils.get_utils()
    projects = context.resources.mp_daily_config.get_project_ids()
    positions = context.resources.mp_daily_config.get_position_ids()


    return {
        "trunc": "day",
        "date_format": "YYYY-MM-DD",
        "start_date": dates.StartDate,
        "end_date": dates.EndDate,
        "project_ids": projects,
        "position_ids": positions,
        "timezone_adjustment_date": "2024-08-21",
        "month_start": dates.MonthStart,
    }

def monthly_params(context) -> Dict:
    """
    Формирование SQL-параметров для месячной агрегации
    """
    dates = context.resources.report_utils.get_utils()
    projects = context.resources.mp_daily_config.get_project_ids()
    positions = context.resources.mp_daily_config.get_position_ids()

    return {
        "trunc": "month",
        "date_format": "YYYY-MM-DD",
        "start_date": dates.MonthStart,
        "end_date": dates.MonthEnd,
        "project_ids": projects,
        "position_ids" : positions,
        "timezone_adjustment_date": "2024-08-21",

    }

def process_aggregation_project_data(
    context,
    aggregated_by_day,
    entered_by_day,
    all_handled_by_day,
    bot_handled_by_day,
    aggregated_by_month,
) -> Dict:
    sources = {
        "entered_by_day": entered_by_day,
        "all_handled_by_day": all_handled_by_day,
        "bot_handled_by_day": bot_handled_by_day,
    }

    df = pd.DataFrame(aggregated_by_day)

    for name, src in sources.items():
        df_src = pd.DataFrame(src)
        context.log.info(f"{name} columns: {df_src.columns.tolist()}")
        if "date" in df_src.columns:
            df = df.merge(df_src, on="date", how="left")
        else:
            context.log.warning(
                f"Источник `{name}` не содержит колонки 'date', merge пропущен."
            )
    df = df.fillna(0)
    day_data = df.to_dict(orient="records")

    if aggregated_by_month:
        m0 = aggregated_by_month[0]
        month_avg_rt = round(m0.get("average_reaction_time", 0))
        month_avg_sta = round(m0.get("average_speed_to_answer", 0))
    else:
        month_avg_rt = month_avg_sta = 0

    return {
        "dayData": day_data,
        "monthAverageReactionTime": month_avg_rt,
        "monthAverageSpeedToAnswer": month_avg_sta,
    }


def render_aggregation_project_data_op(
    context,
    project_data: Dict[str, Any],
    self_service_rate: List[Dict[str, Any]] = None,
    month_self_service_rate: float = None,
):
    logger = get_dagster_logger()

    # 1) Собираем и логируем данные
    full_day_data = project_data["dayData"]
    logger.info(f"Received day_data: {len(full_day_data)} rows")

    # 2) Берём последние 8 дней (включая сегодня)
    last8 = sorted(full_day_data, key=lambda x: x["date"], reverse=True)[:8][::-1]

    # 3) Форматируем даты для заголовков
    formatted_dates = []
    for rec in last8:
        d_str = rec["date"].split(" ")[0]
        dt = datetime.strptime(d_str, "%Y-%m-%d")
        formatted_dates.append(dt.strftime("%d.%m"))

    logger.info(f"Using dates: {formatted_dates}")

    # 4) Годовые/месячные метрики
    month_rt = project_data.get("monthAverageReactionTime", 0)
    month_sta = project_data.get("monthAverageSpeedToAnswer",  0)
    logger.info(f"Month avg — RT: {month_rt}, STA: {month_sta}, BSR: {month_self_service_rate}")

    # 5) Хелпер для строк
    def get_metric_list(key: str) -> List[float]:
        vals = []
        for rec in last8:
            v = rec.get(key, 0) or 0
            try:
                fv = float(v)
            except Exception:
                fv = 0.0
            vals.append(round(fv, 1))
        return vals

    # 6) Заголовки и строки таблицы
    headers = ["Показатель", "План"] + formatted_dates + ["Среднее за текущий месяц"]
    table_data = []

    # 6.1 Reaction Time
    rt_vals = get_metric_list("average_reaction_time")
    table_data.append(
        ["Average Reaction Time", "≤ 60 сек"] + rt_vals + [month_rt]
    )

    # 6.2 Speed To Answer
    sta_vals = get_metric_list("average_speed_to_answer")
    table_data.append(
        ["Average Speed To Answer", "≤ 60 сек"] + sta_vals + [month_sta]
    )

    iso_dates = [rec["date"].split(" ")[0] for rec in last8]
    if self_service_rate:
        bsr_values = get_data_in_dates(
            self_service_rate,
            iso_dates,
            date_key="date",
            value_key="self_service_rate",
            precision=1,
        )
        # сразу добавляем готовую строку в table_data
        table_data.append([
            "Bot Self Service Rate (чат-бот)",
            "≥ 10%",
        ] + bsr_values + [round(month_self_service_rate or 0, 1)])

    # 7) Рисуем таблицу
    constructor = MatplotlibTableConstructor2()
    fig = constructor.render_table(
        headers,
        table_data,
        title="Динамика за последние дни и предварительные данные по всем проектам"
    )

    # 8) Сохраняем в буфер и отдаем base64 preview
    buf = BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    buf.seek(0)
    b64 = base64.b64encode(buf.read()).decode("utf-8")

    yield Output(
        fig,
        metadata={
            "table_preview": MetadataValue.md(f"![table](data:image/png;base64,{b64})")
        },
    )

def extract_from_sql(context, daily_params: Dict, project_id, SQL_AGG):
    params = daily_params.copy()
    params['project_ids'] = project_id
    with open(SQL_AGG, "r", encoding="utf-8") as f:
        sql = f.read()
    with context.resources.source_dwh as conn:
        df = pd.read_sql_query(sql, conn, params=params)

    context.log.info(f"Data: rows={len(df)}, columns={df.columns.tolist()}")

    return df.to_dict(orient="records")

def remap_project_titles(
    data: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:

    project_titles = {
        'O!Money - WhatsApp':        'О!Bank - WhatsApp',
        'O! Money - WhatsApp':       'О!Bank - WhatsApp',
        'O!Money - Telegram':        'О!Bank - Telegram',
        'O! Money - Telegram':       'О!Bank - Telegram',
        'O!Store - ostore.kg':       'ИМ - Сайт | ostore.kg',
        'O! Store - ostore.kg':      'ИМ - Сайт | ostore.kg',
        'O!Store - Telegram':        'ИМ - Telegram',
        'O! Store - Telegram':       'ИМ - Telegram',
        'O!Money - wiki.dengi.kg':   'O!Bank - wiki.dengi.kg',
    }

    for item in data:
        # проверяем, есть ли текущий project в словаре
        if hasattr(item, 'project') and item.project in project_titles:
            # переназначаем
            item.project = project_titles[item.project]

    return data

def left_join_array(left_list, right_list, key):

    # 1) Собираем индекс для правой коллекции: значение ключа → сам словарь
    right_index = {item[key]: item for item in right_list}

    result = []
    # 2) Для каждого элемента из левой коллекции
    for left_item in left_list:
        # Копируем, чтобы не мутировать оригинал
        merged = left_item.copy()
        join_value = left_item.get(key)

        # 3) Ищем совпадение в правой коллекции
        right_item = right_index.get(join_value)
        if right_item:
            # 4) Добавляем все поля из правого словаря,
            #    кроме повторяющегося ключа
            for fld, val in right_item.items():
                if fld != key:
                    merged[fld] = val

        result.append(merged)

    return result

def group_by_shifts(items):
    """
    Группирует список словарей по ключу date_title,
    суммируя поле 'total' для одинаковых групп.
    """
    grouped: Dict[str, Dict[str, Any]] = {}

    for item in items:
        key = f"{item['date']}_{item['title']}"
        total = item.get('total', 0)
        try:
            total = float(total)
        except (TypeError, ValueError):
            total = 0.0

        if key not in grouped:
            grouped[key] = {
                'date': item['date'],
                'title': item['title'],
                'shifts': item.get('shifts'),
                'total': total,
            }
        else:
            grouped[key]['total'] += total

    return list(grouped.values())


def _to_dt(v):
    if isinstance(v, dt):
        return v
    if isinstance(v, date):
        return dt(v.year, v.month, v.day)
    if isinstance(v, str):
        # сначала пробуем ISO "YYYY-MM-DD"
        try:
            return dt.fromisoformat(v)
        except ValueError:
            # если формат другой, уточни маску
            return dt.strptime(v, "%Y-%m-%d")
    raise TypeError(f"Unsupported date value: {v!r}")

def to_hms(value) -> str | None:
    """Преобразует дату/время к строке 'HH:MM:SS' (похоже на PHP: date('H:i:s', strtotime(...)))."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.strftime("%H:%M:%S")

    s = str(value).strip()
    # Небольшая поддержка ISO c 'Z'
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"

    dt = None
    # Пробуем несколько форматов + fromisoformat
    try:
        dt = datetime.fromisoformat(s)
    except Exception:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M",
                    "%d.%m.%Y %H:%M:%S", "%d.%m.%Y %H:%M",
                    "%Y-%m-%dT%H:%M:%S", "%H:%M:%S", "%H:%M"):
            try:
                dt = datetime.strptime(s, fmt)
                break
            except Exception:
                pass
    return dt.strftime("%H:%M:%S") if dt else s

def _norm_date(items):
    for row in items:
        d = row.get("date")
        if d is None:
            continue
        if isinstance(d, (datetime, date)):
            row["date"] = d.strftime("%Y-%m-%d")
        else:
            s = str(d).strip()

            if len(s) >= 10 and s[4] == "-" and s[7] == "-":
                row["date"] = s[:10]
            else:
                try:
                    row["date"] = datetime.fromisoformat(s).date().isoformat()
                except Exception:
                    row["date"] = s

    return items

def strip_seconds_from_shifts(rows, sep=' · '):
    """
    Делает из списка/многострочной 'shifts' одну строку.
    Удаляет любые секунды: HH:MM:SS -> HH:MM.
    """
    pat = re.compile(r'(\b\d{1,2}:\d{2})(?::\d{2})\b')  # 18:00:00 -> 18:00, 16:30:45 -> 16:30
    cleaned = []
    for it in rows:
        v = it.get("shifts")
        if v is None:
            cleaned.append(it); continue

        # 1) приводим к одной строке
        if isinstance(v, (list, tuple)):
            s = sep.join(map(str, v))
        else:
            s = str(v)
            # заменяем перевод строки и <br> на разделитель
            s = s.replace("<br/>", "\n").replace("<br>", "\n")
            s = re.sub(r'[\r\n]+', sep, s)

        # 2) убираем секунды
        s = pat.sub(r"\1", s)

        cleaned.append({**it, "shifts": s})
    return cleaned
def _fmt_num(v: float):
    """Если число целое после округления — вернуть int, иначе float (1 знак)."""
    if v is None:
        return None
    v = round(float(v), 1)
    return int(v) if v.is_integer() else v

def _parse_dt_any(raw: str) -> datetime:
    if isinstance(raw, datetime):
        return raw
    s = str(raw).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d.%m.%Y %H:%M:%S", "%d.%m.%Y",
                "%d-%m-%Y %H:%M:%S", "%d-%m-%Y", "%Y/%m/%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    raise ValueError(f"Unsupported date: {raw!r}")

def smart_number(x, digits = 1, seperator=False):
    if x is None:
        return ''
    try:
        f = float(x)
    except (TypeError, ValueError):
        return x
    if math.isnan(f):
        return ''

    # если целое → возвращаем int
    if f.is_integer():
        return int(f)

    # если указали округление → применяем
    if isinstance(digits, int):
        return round(f, digits)

    # иначе — просто float как есть
    if seperator:
        return format_number(f)
    return f

def format_number(x):
    """
    Форматирует число с разделителем групп разрядов — пробелом.
    1000 -> "1 000"
    2345.5 -> "2 345.5"
    None/invalid -> "0"
    """
    try:
        f = float(x)
    except (TypeError, ValueError):
        return "0"
    sign = "-" if f < 0 else ""
    f_abs = abs(f)

    # Целое число
    if float(f_abs).is_integer():
        return sign + f"{int(f_abs):,}".replace(",", " ")
    # Дробное — показываем до 2 знаков (убираем лишние нули)
    s = f"{f_abs:.2f}".rstrip("0").rstrip(".")
    if "." in s:
        int_part, frac_part = s.split(".")
        int_part = f"{int(int_part):,}".replace(",", " ")
        return sign + int_part + "." + frac_part
    else:
        # на всякий случай
        int_part = f"{int(float(s)):,}".replace(",", " ")
        return sign + int_part



def try_parse_period(period: Any):
    """Принимает str | datetime | date | pd.Timestamp и возвращает datetime."""
    if period is None:
        return None
    if isinstance(period, datetime):
        return period
    if isinstance(period, date):
        return datetime(period.year, period.month, period.day)

    try:
        import pandas as pd
        if isinstance(period, pd.Timestamp):
            return period.to_pydatetime()
    except Exception:
        pass
    if hasattr(period, "to_pydatetime"):
        try:
            return period.to_pydatetime()
        except Exception:
            pass

    if not isinstance(period, str):
        period = str(period)

    for fmt in (
            "%Y-%m-%d %H:%M:%S", "%Y-%m-%d",
            "%d.%m.%Y %H:%M:%S", "%d.%m.%Y",
            "%d-%m-%Y %H:%M:%S", "%d-%m-%Y",
            "%d/%m/%Y %H:%M:%S", "%d/%m/%Y",
    ):
        try:
            return datetime.strptime(period, fmt)
        except (ValueError, TypeError):
            pass

    try:
        s = period.replace("T", " ").split(".")[0].rstrip("Z")
        return datetime.fromisoformat(s)
    except Exception:
        return None

def _normalize(items, period_field="date", row_field="projectName"):
    out = []
    for it in items:
        it = it.copy()
        dt = _parse_dt_any(it.get(period_field))
        it[period_field] = dt.strftime("%Y-%m-%d")
        if not it.get(row_field):
            it[row_field] = "Без проекта"
        for k, v in it.items():
            if isinstance(v, Decimal):
                it[k] = float(v)
            elif isinstance(v, float) and math.isnan(v):
                it[k] = None
        out.append(it)
    return out


def normalize_date_hour(row):
    """
    Принимает dict с полями hour или date+hour и возвращает
    {'date': YYYY-MM-DD, 'hour': HH:MM:SS}
    """
    if "date" in row and "hour" in row and len(row["hour"]) == 8:
        # уже в формате date + hour
        return row

    if "hour" in row and " " in row["hour"]:
        # было объединено: '2025-09-09 23:00:00'
        dt = datetime.strptime(row["hour"], "%Y-%m-%d %H:%M:%S")
        row["date"] = dt.strftime("%Y-%m-%d")
        row["hour"] = dt.strftime("%H:%M:%S")
        return row

    return row

def to_float_or_nan(x):
    if x is None:
        return np.nan
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    if not s:
        return np.nan
    s = s.replace("%", "").replace(",", ".")
    try:
        return float(s)
    except (TypeError, ValueError):
        return np.nan