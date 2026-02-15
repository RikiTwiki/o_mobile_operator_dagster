import base64
import os
from datetime import datetime
from io import BytesIO
from statistics import mean

from dagster import op, In, Out, AssetKey, Output, MetadataValue
from typing import List, Dict, Any, Optional
import pandas as pd
from matplotlib import pyplot as plt
from matplotlib.figure import Figure
from dagster import get_dagster_logger
from utils.statistic_dates import StatisticDates
from utils.templates.tables.MatplotlibTableConstructor2 import MatplotlibTableConstructor2, get_data_in_dates
from utils.templates.tables.basic_table_plt import BasicTable

from utils.mail.mail_report import MailReport

from utils.path import BASE_SQL_PATH
from utils.transformers.mp_report import daily_params, extract_from_sql, monthly_params

from utils.transformers.mp_report import process_aggregation_project_data

from utils.templates.charts.multi_axis_chart import MultiAxisChart


@op(
    required_resource_keys={"source_dwh", "report_utils", "mp_daily_config"},
    description="Агрегированные по дням данные (Saima)",
)
def saima_aggregated_by_day(context):
    params = daily_params(context)
    SQL_AGG = os.path.join(BASE_SQL_PATH, "mp", "get_aggregated.sql")
    return extract_from_sql(context, params, [1], SQL_AGG)

@op(
    required_resource_keys={"source_dwh", "report_utils", "mp_daily_config"},
    description="Агрегированные по дням данные ('General', 'Emag')",
)
def general_aggregated_by_day(context):
    params = daily_params(context)
    SQL_AGG = os.path.join(BASE_SQL_PATH, "mp", "get_aggregated.sql")
    return extract_from_sql(context, params, [3], SQL_AGG)

@op(
    required_resource_keys={"source_dwh", "report_utils", "mp_daily_config"},
    description="Агрегированные по дням данные ('AkchaBulak', 'Money', 'Agent', 'Bank', 'O!Bank')",
)
def other_aggregated_by_day(context):
    params = daily_params(context)
    SQL_AGG = os.path.join(BASE_SQL_PATH, "mp", "get_aggregated.sql")
    return extract_from_sql(context, params, [4,5,6,8,9], SQL_AGG)

@op(
    required_resource_keys={"source_dwh", "report_utils", "mp_daily_config"},
    description="Агрегированные по дням данные (Saima)",
)
def saima_entered_by_day(context):
    params = daily_params(context)
    SQL_ENTERED = os.path.join(BASE_SQL_PATH, "mp", "get_entered_chats.sql")
    return extract_from_sql(context, params, [1], SQL_ENTERED)

@op(
    required_resource_keys={"source_dwh", "report_utils", "mp_daily_config"},
    description="Агрегированные по дням данные ('General', 'Emag')",
)
def general_entered_by_day(context):
    params = daily_params(context)
    SQL_ENTERED = os.path.join(BASE_SQL_PATH, "mp", "get_entered_chats.sql")
    return extract_from_sql(context, params, [3], SQL_ENTERED)

@op(
    required_resource_keys={"source_dwh", "report_utils", "mp_daily_config"},
    description="Агрегированные по дням данные ('AkchaBulak', 'Money', 'Agent', 'Bank', 'O!Bank')",
)
def other_entered_by_day(context):
    params = daily_params(context)
    SQL_ENTERED = os.path.join(BASE_SQL_PATH, "mp", "get_entered_chats.sql")
    return extract_from_sql(context, params, [4,5,6,8,9], SQL_ENTERED)

@op(
    required_resource_keys={"source_dwh", "report_utils", "mp_daily_config"},
    description="Агрегированные по дням данные (Saima)",
)
def saima_handled_by_day(context):
    params = daily_params(context)
    SQL_ALL_HANDLED = os.path.join(BASE_SQL_PATH, "mp", "get_all_aggregated.sql")
    return extract_from_sql(context, params, [1], SQL_ALL_HANDLED)

@op(
    required_resource_keys={"source_dwh", "report_utils", "mp_daily_config"},
    description="Агрегированные по дням данные ('General', 'Emag')",
)
def general_handled_by_day(context):
    params = daily_params(context)
    SQL_ALL_HANDLED = os.path.join(BASE_SQL_PATH, "mp", "get_all_aggregated.sql")
    return extract_from_sql(context, params, [3], SQL_ALL_HANDLED)

@op(
    required_resource_keys={"source_dwh", "report_utils", "mp_daily_config"},
    description="Агрегированные по дням данные ('AkchaBulak', 'Money', 'Agent', 'Bank', 'O!Bank')",
)
def other_handled_by_day(context):
    params = daily_params(context)
    SQL_ALL_HANDLED = os.path.join(BASE_SQL_PATH, "mp", "get_all_aggregated.sql")
    return extract_from_sql(context, params, [4,5,6,8,9], SQL_ALL_HANDLED)

@op(
    required_resource_keys={"source_dwh", "report_utils", "mp_daily_config"},
    description="Агрегированные по дням данные (saima)",
)
def saima_bot_handled_by_day(context):
    params = daily_params(context)
    SQL_BOT_HANDLED = os.path.join(BASE_SQL_PATH, "mp", "bot_handled_chats.sql")
    return extract_from_sql(context, params, [1], SQL_BOT_HANDLED)

@op(
    required_resource_keys={"source_dwh", "report_utils", "mp_daily_config"},
    description="Агрегированные по дням данные (general, emag)",
)
def general_bot_handled_by_day(context):
    params = daily_params(context)
    SQL_BOT_HANDLED = os.path.join(BASE_SQL_PATH, "mp", "bot_handled_chats.sql")
    return extract_from_sql(context, params, [3], SQL_BOT_HANDLED)

@op(
    required_resource_keys={"source_dwh", "report_utils", "mp_daily_config"},
    description="Агрегированные по дням данные",
)
def other_bot_handled_by_day(context):
    params = daily_params(context)
    SQL_BOT_HANDLED = os.path.join(BASE_SQL_PATH, "mp", "bot_handled_chats.sql")
    return extract_from_sql(context, params, [4,5,6,8,9], SQL_BOT_HANDLED)


@op(
    required_resource_keys={"source_dwh", "report_utils", "mp_daily_config"},
    description="Агрегированные за месяц (saima)",
)
def saima_aggregated_by_month(context):
    params = monthly_params(context)
    SQL_AGG = os.path.join(BASE_SQL_PATH, "mp", "get_aggregated.sql")
    return extract_from_sql(context, params, [1], SQL_AGG)

@op(
    required_resource_keys={"source_dwh", "report_utils", "mp_daily_config"},
    description="Агрегированные за месяц (General, Emag)",
)
def general_aggregated_by_month(context):
    params = monthly_params(context)
    SQL_AGG = os.path.join(BASE_SQL_PATH, "mp", "get_aggregated.sql")
    return extract_from_sql(context, params, [3], SQL_AGG)

@op(
    required_resource_keys={"source_dwh", "report_utils", "mp_daily_config"},
    description="Агрегированные за месяц ('AkchaBulak', 'Money', 'Agent', 'Bank', 'O!')",
)
def other_aggregated_by_month(context):
    params = monthly_params(context)
    SQL_AGG = os.path.join(BASE_SQL_PATH, "mp", "get_aggregated.sql")
    return extract_from_sql(context, params, [4, 5, 6, 8, 9], SQL_AGG)


@op(
    required_resource_keys={"source_dwh", "report_utils", "mp_daily_config"},
    description="Показатель self-service по проектам Saima",
)
def saima_service_rate_by_day_op(context):
    """
    Аналог Php кода allSelfServiceRate: загружает данные по self-service rate для проектов 1
    """
    params = daily_params(context)
    sql_path = os.path.join(BASE_SQL_PATH, "mp", "self_service_rate.sql")
    return extract_from_sql(context, params, [1], sql_path)

@op(
    required_resource_keys={"source_dwh", "report_utils", "mp_daily_config"},
    description="Показатель self-service по проектам Saima",
)
def bank_service_rate_by_day_op(context):
    """
    Аналог Php кода allSelfServiceRate: загружает данные по self-service rate для проектов 3
    """
    params = daily_params(context)
    sql_path = os.path.join(BASE_SQL_PATH, "mp", "self_service_rate.sql")
    return extract_from_sql(context, params, [8, 9], sql_path)

@op(
    required_resource_keys={"source_dwh", "report_utils", "mp_daily_config"},
    description="Показатель self-service по проектам Saima",
)
def service_rate_saima_by_month_op(context):
    """
    Аналог Php кода allSelfServiceRate: загружает данные по self-service rate для проектов 1
    """
    params = monthly_params(context)
    sql_path = os.path.join(BASE_SQL_PATH, "mp", "self_service_rate.sql")
    records = extract_from_sql(context, params, [1], sql_path)
    if records and 'self_service_rate' in records[0]:
        rate = records[0]['self_service_rate']
    else:
        rate = 0.0
    rate_float = float(rate)
    context.log.info(f'return: {rate_float}')
    return rate_float

@op(
    required_resource_keys={"source_dwh", "report_utils", "mp_daily_config"},
    description="Показатель месяцы self-service по проектам Bank",
)
def service_rate_bank_by_month_op(context):
    """
    Аналог Php кода allSelfServiceRate: загружает данные по self-service rate для проектов 8, 9
    """
    params = monthly_params(context)
    sql_path = os.path.join(BASE_SQL_PATH, "mp", "self_service_rate.sql")
    records = extract_from_sql(context, params, [8, 9], sql_path)
    if records and 'self_service_rate' in records[0]:
        rate = records[0]['self_service_rate']
    else:
        rate = 0.0
    rate_float = float(rate)
    context.log.info(f'return: {rate_float}')
    return rate_float

# 1) Сборка дневной и месячной агрегации в единый словарь
@op(
    required_resource_keys={"source_dwh"},
    ins={
        "aggregated_by_day":   In(asset_key=AssetKey(["aggregated_by_day"])),
        "entered_by_day":      In(asset_key=AssetKey(["entered_by_day"])),
        "all_handled_by_day":  In(asset_key=AssetKey(["all_handled_by_day"])),
        "bot_handled_by_day":  In(asset_key=AssetKey(["bot_handled_by_day"])),
        "aggregated_by_month": In(asset_key=AssetKey(["aggregated_by_month"])),
    },
    out=Out(Dict),
)
def all_projects_data(
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
@op(out=Out(Dict))
def saima_project_data(context,
    saima_aggregated_by_day,
    saima_entered_by_day,
    saima_all_handled_by_day,
    saima_bot_handled_by_day,
    saima_aggregated_by_month
) -> Dict:
    return process_aggregation_project_data(context, saima_aggregated_by_day, saima_entered_by_day, saima_all_handled_by_day, saima_bot_handled_by_day, saima_aggregated_by_month)

@op(out=Out(Dict))
def general_project_data(context,
    general_aggregated_by_day,
    general_entered_by_day,
    general_all_handled_by_day,
    general_bot_handled_by_day,
    general_aggregated_by_month)-> Dict:
    return process_aggregation_project_data(context, general_aggregated_by_day, general_entered_by_day, general_all_handled_by_day, general_bot_handled_by_day, general_aggregated_by_month)

@op(out=Out(Dict))
def other_project_data(context,
    other_aggregated_by_day,
    other_entered_by_day,
    other_all_handled_by_day,
    other_bot_handled_by_day,
    other_aggregated_by_month) -> Dict:
    return process_aggregation_project_data(context, other_aggregated_by_day, other_entered_by_day, other_all_handled_by_day, other_bot_handled_by_day, other_aggregated_by_month)

# 2) Загрузка ассета all_service_rate_by_day
@op(ins={"all_service_rate_by_day": In(asset_key=AssetKey(["all_service_rate_by_day"]))}, out=Out(List[Dict]))
def load_self_service_rate(context, all_service_rate_by_day: List[Dict]) -> List[Dict]:
    context.log.info(f'return: {all_service_rate_by_day}')
    return all_service_rate_by_day

# 3) Загрузка ассета service_rate_all_by_month
@op(ins={"service_rate_all_by_month": In(asset_key=AssetKey(["service_rate_all_by_month"]))}, out=Out(float))
def load_month_self_service_rate(context, service_rate_all_by_month: List[Dict]) -> float:
    if service_rate_all_by_month:
        rate = service_rate_all_by_month[0].get("self_service_rate", 0)
        context.log.info(f'return: {float(rate)}')

        return float(rate)
    context.log.info(f'return: {0.0}')

    return 0.0

# 4) Рендер итоговой таблицы в matplotlib Figure
@op(
    out=Out(Figure),
)
def render_aggregation_project_data_op(
    context,
    project_data: Dict[str, Any],
        self_service_rate: Optional[List[Dict[str, Any]]] = None,
        month_self_service_rate: Optional[float] = None,
) :
    # 2.2 Инициализируем дефолты внутри
    if self_service_rate is None:
        self_service_rate = []
    if month_self_service_rate is None:
        month_self_service_rate = 0.0
    logger = get_dagster_logger()

    # 1) Собираем и логируем данные
    full_day_data = project_data["dayData"]
    logger.info(f"Received day_data: {len(full_day_data)} rows")

    # 2) Берём последние 8 дней (включая сегодня)
    last8 = sorted(full_day_data, key=lambda x: x["date"], reverse=True)[:8][::-1]

    # 3) Форматируем даты для заголовков
    formatted_dates = []
    for rec in last8:
        # ожидаем формат "YYYY-MM-DD" или "YYYY-MM-DD HH:MM:SS"
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
    fig = constructor.rqender_table(
        headers,
        table_data)

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



@op(ins={"hour_MPData_Messengers_Detailed": In(asset_key=AssetKey(["hour_MPData_Messengers_Detailed"]))}, out=Out(Figure))
def render_Hour_Jivo_Data_Aggregated(context, hour_MPData_Messengers_Detailed):
    items = sorted(hour_MPData_Messengers_Detailed, key=lambda x: datetime.strptime(x['date'], '%H:%M:%S'))

    table = BasicTable()
    table.Items = items
    table.Fields = [
        {'field': 'date', 'title': 'Часы'},
        {'field': 'total_chats', 'title': 'Поступило обращений', 'summary': 'sum'},
        {'field': 'total_handled_chats', 'title': 'Обработано обращений', 'summary': 'sum'},
        {'field': 'total_by_operator', 'title': 'Закрыто без задержки (операторами)', 'summary': 'sum'},
        {'field': 'bot_handled_chats', 'title': 'Закрыто с задержкой 12 часов (чат-ботом)', 'summary': 'sum'},
        {'field': 'average_reaction_time', 'title': 'Average Reaction Time', 'paint': 'asc', 'round': 2},
        {'field': 'average_speed_to_answer', 'title': 'Average Speed To Answer', 'paint': 'asc', 'round': 2},
        {'field': 'user_total_reaction_time', 'title': 'Chat Total Reaction Time', 'paint': 'asc', 'round': 2},
        {'field': 'user_count_replies', 'title': 'Chat Count Replies', 'paint': 'desc', 'round': 2},
    ]

    # Вызов метода, который подготовит данные и построит таблицу
    fig = table.getTable()
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

@op(out=Out(Figure),
)
def render_Mixed_Graph( project_data: Dict[str, Any],
):
    """
    Обёртка для быстрой настройки графика в стиле Jivo,
    аналогична PHP-функции renderMixedGraph.
    Изменить названия легенды, заголовок и параметры можно здесь.
    """
    project_data = project_data["dayData"]

    for item in project_data:
        item['date_dt'] = datetime.strptime(item['date'], '%Y-%m-%d')
    project_data.sort(key=lambda x: x['date_dt'])
    for item in project_data:
        del item['date_dt']

    # Вычисляем максимальное значение
    max_val = 100
    for item in project_data:
        item['bot_handled_chats'] = item.get('bot_handled_chats', 0)
        if item.get('total_chats', 0) > max_val:
            max_val = item['total_chats']

    # Параметры можно менять здесь
    chart = MultiAxisChart(
        graph_title="Данный график показывает кол-во обработанных обращений.",
        x_axes_name="Дата",
        y_axes_name="Кол-во",
        stack=True,
        show_tooltips=False,
        pointer=1,
        y_axes_min=0,
        y_axes_max=max_val,
        fields=[
            {"field": "date",                "title": "",                                      "type": "label"},
            {"field": "total",               "title": "Кол-во чатов обработанных специалистами",    "chartType": "bar",  "round": 1},
            {"field": "bot_handled_chats",   "title": "Кол-во чатов обработанных чат-ботом",         "chartType": "bar",  "round": 1},
            {"field": "total_chats",         "title": "Кол-во поступивших чатов",                  "chartType": "line", "round": 1},
        ],
        items=project_data
    )
    fig, ax = plt.subplots(
    figsize=(chart.width / chart.dpi, chart.height / chart.dpi),
    dpi=chart.dpi
    )
    chart.render_graphics(ax)
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

@op(
)
def save_report_locally(context, fig: Figure):
    """
    Сохраняет переданную matplotlib Figure в локальную папку reports/
    в формате PDF с таймстампом.
    """
    # Папка для отчётов
    output_dir = "reports"
    os.makedirs(output_dir, exist_ok=True)

    # Имя файла с текущей датой-временем
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"report_{ts}.pdf"
    file_path = os.path.join(output_dir, filename)

    # Сохраняем фигуру как PDF
    fig.savefig(file_path, format="pdf", bbox_inches="tight")
    context.log.info(f"PDF-отчёт сохранён локально: {file_path}")


