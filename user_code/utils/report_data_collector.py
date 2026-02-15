import json
import re
import zlib
from decimal import ROUND_HALF_UP, Decimal

from dagster import get_dagster_logger

from utils.getters.jira_data_getter_next import JiraDataGetterNext
from utils.templates.tables.basic_table import  BasicTable
from resources import ReportUtils
from utils.array_operations import left_join_array
from utils.getters.OperatorsCountGetter import Operatorscountgetter
from utils.getters.average_handling_time_data_getter import AverageHandlingTimeDataGetter
from utils.getters.calls_data_getter import CallsDataGetter
from utils.getters.critical_error_accuracy_data_getter import CriticalErrorAccuracyDataGetter
from utils.getters.customer_satisfaction_index_aggregation_data_getter import \
    CustomerSatisfactionIndexAggregationDataGetter
from utils.getters.ivr_transaction_data_getter import IVRTransactionDataGetter
from utils.getters.messaging_platform_data_getter import MessagingPlatformDataGetter
from utils.getters.naumen_service_level_data_getter import NaumenServiceLevelDataGetter
from utils.getters.occupancydatagetter import OccupancyDataGetter
from utils.getters.omni_data_getter import OmniDataGetter
from utils.getters.repeat_calls_data_getter import RepeatCallsDataGetter
from utils.templates.charts.multi_axis_chart import MultiAxisChart
from utils.templates.tables.GroupedPivotTable import GroupedPivotTable
from utils.templates.tables.IssueHierarchicalTable import IssueHierarchicalTable
from utils.templates.tables.pivot_table import PivotTable
from utils.templates.tables.table_constructor import TableConstructor
from utils.transformers.mp_report import normalize_date_hour, _normalize, smart_number
import matplotlib.dates as mdates
import matplotlib.transforms as mtransforms
from matplotlib import pyplot as plt
from matplotlib.ticker import FuncFormatter
from utils.templates.charts.basic_line_graphics import BasicLineGraphics, color_weekend_xticklabels
import base64
import io
import math
from datetime import datetime, timedelta

from dagster import get_dagster_logger

from typing import Any, List, Dict, Optional

def _to_int(x) -> int:
    try:
        if x is None:
            return 0
        # на случай если прилетает float/str
        return int(float(x))
    except Exception:
        return 0

def get_cea_data(conn, period, title, project_ids=None, start_date=None, end_date=None):
    if project_ids is None:
        project_ids = []
    getter = CriticalErrorAccuracyDataGetter(start_date=start_date, end_date=end_date, bpm_conn=conn)
    setup_data_getter(getter, period, start_date, end_date)
    getter.project_ids = title
    result = getter.get_aggregated_data_by_group(project_ids=project_ids)
    return result

def get_cea_data_by_project(conn, period, project_titles, start_date, end_date):
    getter = CriticalErrorAccuracyDataGetter(start_date=start_date, end_date=end_date, bpm_conn=conn)
    setup_data_getter(getter, period, start_date, end_date)
    getter.projects = project_titles
    result = getter.get_aggregated_data_by_project_title()

    get_dagster_logger().info(f"result = {result}")

    return result

def get_main_aggregation_data(conn, group, period, start_date, end_date, report_date):
    get_dagster_logger().info(f"group = {group}")
    get_dagster_logger().info(f"period = {period}")
    getter = NaumenServiceLevelDataGetter(conn=conn, start_date=start_date, end_date=end_date)
    setup_data_getter(getter, period, start_date, end_date, report_date)
    get_dagster_logger().info(f"getter.start_date = {getter.start_date, getter.end_date}")
    getter.filter_projects_by_group_array(group)
    get_dagster_logger().info(f"getter.project_id = {getter.project_id}")
    get_dagster_logger().info(f"getter.SelectedProjects = {getter.SelectedProjects}")
    main_data_dynamic_general = getter.get_aggregated_data_set_date()

    getter.reload_projects()
    getter.filter_projects_by_group_array(group, 'IVR')
    get_dagster_logger().info(f"getter.SelectedProjects = {getter.SelectedProjects}")
    main_data_dynamic_general_ivr = getter.get_aggregated_data_ivr_set_date()

    return left_join_array(main_data_dynamic_general, main_data_dynamic_general_ivr, 'date')

def get_main_aggregation_data_with_9999(conn, data, group, period, start_date, end_date, report_date):
    get_dagster_logger().info(f"group = {group}")
    get_dagster_logger().info(f"period = {period}")
    getter = CallsDataGetter(conn=conn, start_date=start_date, end_date=end_date)
    setup_data_getter(getter, period, start_date, end_date, report_date)

    get_dagster_logger().info(f"group: {group[0] if group else None}")

    items = getter.get_ivr_9999_by_day(group[0] if group else None)

    for row in items:
        d = (row.get('date') or '').strip()
        if d:
            try:
                row['date'] = datetime.strptime(d, '%d.%m.%Y').strftime('%Y-%m-%d 00:00:00')
            except ValueError:
                row['date'] = None
    items = [r for r in items if r.get('date')]

    get_dagster_logger().info(f"items: {items}")

    items = left_join_array(data, items, 'date')

    for row in items:
        row['ivr_total'] = _to_int(row.get('ivr_total')) + _to_int(row.get('ivr_from_9999'))
        row.pop('ivr_from_9999', None)

    get_dagster_logger().info(f"items: {items}")

    return items

def get_main_aggregation_data_filtered(conn, group, period, start_date, end_date):
    get_main_data_dynamic = NaumenServiceLevelDataGetter(conn=conn, start_date=start_date, end_date=end_date)
    setup_data_getter(get_main_data_dynamic, period, start_date, end_date)
    get_main_data_dynamic.filter_projects_by_group_array(group)
    main_data_dynamic_general = get_main_data_dynamic.get_aggregated_data_filtered_projects()

    get_main_data_dynamic.reload_projects()
    get_main_data_dynamic.filter_projects_by_group_array(group, 'IVR')
    main_data_dynamic_general_ivr = get_main_data_dynamic.get_aggregated_data_ivr_filtered_projects()
    return {
        "custom": main_data_dynamic_general,
        "ivr": main_data_dynamic_general_ivr,
    }

def get_average_handling_time_data_filtered_group(conn, project, period, start_date, end_date):
    getter = AverageHandlingTimeDataGetter(start_date=start_date, end_date=end_date, dwh_conn=conn)
    setup_data_getter(getter, period, start_date, end_date)
    getter.filter_projects_by_group_array(project)
    return getter.get_aggregated_data_filtered_group()


def get_average_handling_time_data(conn, project, period, start_date, end_date):
    getter = AverageHandlingTimeDataGetter(start_date=start_date, end_date=end_date, dwh_conn=conn)
    setup_data_getter(getter, period, start_date, end_date, start_date)
    getter.filter_projects_by_group_array(project)
    getter.set_date_format = True
    return getter.get_aggregated_data()

def get_occupancy_data_filtered(conn, bpm_conn, naumen_conn_1, naumen_conn_3,  group, period, positions, view_ids, project_name = None, start_date=None, end_date=None):
    getter = OccupancyDataGetter(start_date=start_date, end_date=end_date, bpm_conn=bpm_conn, conn_naumen_1=naumen_conn_1, conn_naumen_3=naumen_conn_3)
    setup_data_getter(getter, period, start_date, end_date, start_date)
    getter.filter_projects_by_group_array(group)
    return getter.get_aggregated_data_filtered(positions, view_ids, project_name)

def get_pst_cea_data(conn, period, position, start_date, end_date):
    getter = CriticalErrorAccuracyDataGetter(start_date=start_date, end_date=end_date, bpm_conn=conn)
    setup_data_getter(getter, period, start_date, end_date)
    return getter.get_aggregated_data_by_position(position)

def get_csi_data(conn, period, group, channels, start_date, end_date):
    getter = CustomerSatisfactionIndexAggregationDataGetter(conn=conn, start_date=start_date, end_date=end_date)
    setup_data_getter(getter, period, start_date, end_date, start_date)
    return getter.get_combined_rating_result(group, channels)

def get_ur_data(conn, naumen_conn_1, naumen_conn_3, period, group, position, view_ids, projects=None, start_date=None, end_date=None):
    getter = OccupancyDataGetter(bpm_conn=conn, conn_naumen_1=naumen_conn_1, conn_naumen_3=naumen_conn_3)
    flag=None
    if period == 'month':
        flag = 'month'
        period='day'

    get_dagster_logger().info(f"flag={flag}, period={period}")
    setup_data_getter(getter, period, start_date, end_date)
    getter.filter_projects_by_group_array(group)
    pos_ids = position if isinstance(position, (list, tuple, set)) else [position]
    return getter.get_utilization_rate_data_with_projects(pos_ids, view_ids, flag, projects)

def get_rc_data(conn, period, group, start_date, end_date):
    getter = RepeatCallsDataGetter(start_date=start_date, end_date=end_date, dwh_conn=conn)
    setup_data_getter(getter, period, start_date, end_date)
    return getter.get_aggregated_data(table='00:00:00', groups=group)

def get_rc_by_ivr(conn, bpm_conn, start_date, end_date):
    getter = CallsDataGetter(start_date=start_date, end_date=end_date, conn=conn, conn_bpm=bpm_conn)
    setup_data_getter(getter, period='day', start_date=start_date, end_date=end_date)
    return getter.get_repeat_calls_by_ivr(start_date=start_date, end_date=end_date)

def get_rc_data_filtered(conn, period, group, start_date, end_date):
    getter = RepeatCallsDataGetter(start_date=start_date, end_date=end_date, dwh_conn=conn)
    setup_data_getter(getter, period, start_date, end_date)
    return getter.get_aggregated_data_detailed(table='00:00:00', groups=group)

def get_mp_data(conn, period, start_date, end_date):
    getter = MessagingPlatformDataGetter(start_date=start_date, end_date=end_date, conn=conn)
    setup_data_getter(getter, period, start_date, end_date)
    return getter.get_aggregated()

def _normalize_to_ymd(s: str, date_formats=None) -> str:
    s = (s or "").strip()
    fmts = (date_formats or []) + [
        "%Y-%m-%d %H:%M:%S", "%Y-%m-%d",
        "%d.%m.%Y %H:%M:%S", "%d.%m.%Y",
        "%d-%m-%Y %H:%M:%S", "%d-%m-%Y",
        "%d/%m/%Y %H:%M:%S", "%d/%m/%Y",
    ]
    last_err = None
    for fmt in fmts:
        try:
            return datetime.strptime(s[:19], fmt).strftime("%Y-%m-%d")
        except Exception as e:
            last_err = e
    try:
        st = s.replace("T", " ").split(".")[0].rstrip("Z")
        return datetime.fromisoformat(st).strftime("%Y-%m-%d")
    except Exception:
        raise ValueError(f"Не удалось распарсить дату: {s!r}") from last_err


def _php_round(value: float, ndigits: int) -> float:
    q = Decimal(10) ** -ndigits
    return float(Decimal(value).quantize(q, rounding=ROUND_HALF_UP))

def get_data_in_dates(
    items: List[Dict[str, Any]],
    dates: List[str],
    date_key: str,
    data_key: str,
    ndigits: Optional[int] = None,
    date_formats=None,
) -> List[Any]:
    """
    Для каждой даты из `dates` возвращает значение `data_key` из `items`, где `date_key`
    совпадает по 'YYYY-MM-DD'. Если нет совпадения — "".
    Непарсабельные даты в items игнорируются (логируются как warning).
    """
    logger = get_dagster_logger()

    # 0) валидация входа
    if not isinstance(dates, (list, tuple)):
        raise TypeError(f"'dates' must be list/tuple, got {type(dates)}: {dates!r}")

    # 1) Предварительно нормализуем items в индекс по дате
    by_date: Dict[str, Any] = {}
    for it in (items or []):
        try:
            it_date_raw = it.get(date_key)
            if it_date_raw is None:
                continue
            ymd = _normalize_to_ymd(str(it_date_raw), date_formats)
        except Exception as e:
            logger.warning(f"[get_data_in_dates] skip item with bad {date_key}={it.get(date_key)!r}: {e}")
            continue

        # Берём последнее значение (или первое) — как у вас принято
        val = it.get(data_key, None)
        by_date[ymd] = val

    # 2) Собираем результат в порядке `dates`
    result: List[Any] = []
    for d in dates:
        try:
            ymd = _normalize_to_ymd(d, date_formats)
        except Exception as e:
            # если в dates мусор — покажем явную ошибку с контекстом
            raise ValueError(f"Bad date in 'dates': {d!r}") from e

        if ymd not in by_date:
            result.append("")
            continue

        val = by_date[ymd]
        if val is None:
            result.append("")
        elif ndigits is not None:
            try:
                num = _php_round(float(val), ndigits)
                result.append(smart_number(num))
            except Exception:
                result.append(val)
        else:
            result.append(smart_number(val))

    return result

def get_task_row(task, indicator, plan, items, abbreviation, plan_val, operator, month_val, period='date', date_list=None):
    task.genCellTD(indicator, "font-weight: bold;")

    task.genCellTD(plan)

    task.multiGenCellTD(
        get_data_in_dates(
            items,
            date_list,
            period,
            abbreviation,
            1
        ),
        None,
        {
            "plan": plan_val,
            "operator": f"{operator}",
            "style": "background: #FF4D6E; ",
        }
    )

    task.genCellTD(month_val, "font-weight: bold;")

    task.genRow()

    return task

def get_task_row_lite(task, indicator, plan, items, abbreviation, plan_val, operator, month_val, period='date', date_list=None):
    # 1. Название показателя
    task.genCellTD(indicator, "font-weight: bold;")

    # 2. Текст плана (колонку оставляем, чтобы таблица не "поехала")
    task.genCellTD(plan)

    # 3. Генерация ячеек с данными
    task.multiGenCellTD(
        get_data_in_dates(
            items,
            date_list,
            period,
            abbreviation,
            1
        ),
        None,
        None

    )

    # 4. Итог за месяц
    task.genCellTD(month_val, "font-weight: bold;")

    task.genRow()

    return task

def _clamp01(x: float) -> float:
    return 0.0 if x < 0 else 1.0 if x > 1 else x

def _lerp(a: int, b: int, t: float) -> int:
    return int(round(a + (b - a) * t))

def _heat_style(val: float | None, vmin: float, vmax: float, invert: bool = False) -> str | None:
    if val is None:
        return None
    if vmax == vmin:
        return "background-color: rgb(235,235,235);"  # все одинаковые
    t = (val - vmin) / (vmax - vmin)
    t = _clamp01(t)
    if invert:
        t = 1 - t

    # Excel-like: red -> green (мягкие оттенки)
    r1, g1, b1 = 255, 199, 206
    r2, g2, b2 = 198, 239, 206
    r = _lerp(r1, r2, t)
    g = _lerp(g1, g2, t)
    b = _lerp(b1, b2, t)
    return f"background-color: rgb({r},{g},{b});"

def gen_task_row_new(
        task,
        indicator,
        items,
        abbreviation: str,
        month_val,
        period: str = "date",
        date_list = None
):
    task.genCellTD(indicator, "font-weight: bold;")

    data = get_data_in_dates(
        items,
        date_list,
        period,
        abbreviation,
        1
    )

    def to_numeric(value):
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            # Убираем пробелы
            value = value.strip()
            if value == '' or value == '-' or value.lower() in ['null', 'none', 'n/a']:
                return None
            try:
                return float(value)
            except ValueError:
                return None
        return None

    numeric_data = [to_numeric(x) for x in data]

    valid_data = [x for x in numeric_data if x is not None]

    min_max = {
        "min": min(valid_data) if valid_data else 0,
        "max": max(valid_data) if valid_data else 0,
    }

    task.multiGenCellTD(
        data,
        None,
        None,
        min_max
    )

    task.genCellTD(month_val, "font-weight: bold;")

    task.genRow()

    return task

def get_operators_count(conn, bpm_conn, naumen_conn_1, naumen_conn_3, view_ids, positions, date, start_date, end_date):
    operators_count = Operatorscountgetter(report_day=date, conn=conn, bpm_conn=bpm_conn, naumen_conn_1=naumen_conn_1, naumen_conn_3=naumen_conn_3, start_date=start_date, end_date=end_date)
    operators_count.view_ids = view_ids
    operators_count.position_ids = positions
    get_dagster_logger().info(f'positions = {positions}')
    if positions[0] == 41 or positions[0] == 46:
        fact_operators = operators_count.get_gpo_fact_operators()
    else:
        fact_operators = operators_count.get_fact_operatorts()

    fact_and_labor_operators_difference = get_difference(fact_operators, operators_count.get_labor_data(), 'hour', 'fact_operators', 'fact_labor', 'fact_operators_and_fact_labor')
    fact_and_plan_operators_count = left_join_array(operators_count.get_labor_data(), fact_operators, 'hour')

    result = left_join_array(fact_and_plan_operators_count, fact_and_labor_operators_difference, 'hour')
    return result

from typing import List, Dict, Any

def get_difference(
    main: List[Dict[str, Any]],
    secondary: List[Dict[str, Any]],
    common_key: str = "date",
    decreasing: str = "total",
    subtraction: str = "total",
    difference: str = "total",
) -> List[Dict[str, Any]]:
    response: List[Dict[str, Any]] = []

    for item in main:
        search = next(
            (s for s in secondary if s.get(common_key) == item.get(common_key)),
            None
        )
        if search is not None:
            response.append({
                difference: item.get(decreasing, 0) - search.get(subtraction, 0),
                common_key: item.get(common_key),
            })
    return response

def add_chart_module(target, graph_title, axes_name, items, field, axes_max=100, axes_min=0, title=""):
    graphic = MultiAxisChart(
        graph_title=graph_title,
        y_axes_name=axes_name,
        y_axes_max=axes_max,
        y_axes_min=axes_min,
        target=target,
        fields=[
            {"field": "date", "title": "", "type": "label"},
            {
                'field': field,
                'title': title,
                'chartType': 'line',
                'round': 1
            }
        ],
        items=items,
        dot=True
    )
    fig, ax = plt.subplots(
        figsize=(graphic.width / graphic.dpi, graphic.height / graphic.dpi),
        dpi=graphic.dpi,
    )
    graphic.render_graphics(ax)

    buf = io.BytesIO()
    fig.savefig(buf, format="svg", bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")

def render_day_profile(detailed_data, key=True):
    table = BasicTable()

    table.Fields = [
        {"field": "hour", "title": "Часы"},
    ]

    if key:
        table.Fields.extend([
            {"field": "erlang", "title": "Эрланг", "paint": True, "summary": "sum", "round": 1},
            {"field": "fact_labor", "title": "График", "paint": True, "summary": "sum", "round": 1},
            {"field": "fact_operators", "title": "Факт", "paint": True, "summary": "sum", "round": 1},
            {
                "field": "fact_operators_and_fact_labor",
                "title": "Дельта Факт и График",
                "customPaint": True,
                "summary": "sum",
                "round": 1,
            },
        ])

    table.Fields.extend([
        {"field": "ivr_total", "title": "Поступило на IVR", "paint": True, "summary": "sum", "round": 1},
        {"field": "total_to_operators", "title": "Распределено на операторов", "paint": True, "summary": "sum",
         "round": 1},
        {"field": "answered", "title": "Отвечено операторами", "paint": True, "summary": "sum", "round": 1},
        {"field": "total_queue", "title": "Завершены в очереди", "paint": True, "summary": "sum", "round": 1},

        {"field": "maximum_waiting_time", "title": "Maximum Waiting Time", "summary": "max", "round": 1, "paint": True,
         "time_format": True},
        {"field": "max_pickup_time", "title": "Maximum Ringing Time", "paint": True, "summary": "max", "round": 1,
         "time_format": True},
        {"field": "max_speaking_time", "title": "Maximum Speaking Time", "paint": True, "summary": "max", "round": 1,
         "time_format": True},
        {"field": "max_holding_time", "title": "Maximum Holding Time", "paint": True, "summary": "max", "round": 1,
         "time_format": True},

        {
            "field": "SL",
            "title": "Service Level",
            "paint": "desc",
            "round": 1,
            "advancedSummaryAvg": {"numerator": "sla_answered", "denominator": "sla_total", "round": 1},
            "paint_type": "asc"
        },
        {
            "field": "ACD",
            "title": "Handled Calls Rate",
            "paint": "desc",
            "round": 1,
            "advancedSummaryAvg": {"numerator": "answered", "denominator": "total_to_operators", "round": 1},
            "paint_type": "asc"
        },
        {
            "field": "customer_satisfaction_index",
            "title": "Customer Satisfaction Index",
            "paint": "desc",
            "round": 1,
            "advancedSummaryAvg": {"numerator": "good_marks", "denominator": "all_marks", "round": 1},
            "paint_type": "asc"
        },
    ])

    if key:
        table.Fields.append({
            "field": "occupancy",
            "title": "Occupancy",
            "paint": "desc",
            "round": 1,
            "advancedSummaryAvg": {"numerator": "handling_time", "denominator": "product_time", "round": 1},
            "paint_type": "asc"
        })

    table.Items = detailed_data
    return table.getTable()

def render_pst_handled_time_table(pst_detailed_daily_data, key=True):
    table = BasicTable()

    # базовое поле
    table.Fields = [
        {"field": "hour", "title": "Часы"},
    ]

    # если key=True → добавить блок
    if key:
        table.Fields.extend([
            {"field": "erlang", "title": "Эрланг", "round": 1, "summary": "sum", "paint": True},
            {"field": "fact_labor", "title": "График", "round": 1, "summary": "sum", "paint": True},
            {"field": "fact_operators", "title": "Факт", "round": 1, "summary": "sum", "paint": True},
            {
                "field": "fact_operators_and_fact_labor",
                "title": "Дельта Факт и График",
                "customPaint": True,
                "round": 1,
                "summary": "sum",
            },
        ])

    # основные поля
    table.Fields.extend([
        {"field": "total", "title": "Всего закрытых чатов", "paint": True, "round": 1, "summary": "sum"},
        {"field": "operator_answers_count", "title": "Закрыто без задержки (операторами)", "paint": True, "round": 1,
         "summary": "sum"},
        {"field": "bot_answers_count", "title": "Закрыто с задержкой 12 часов (чат-ботом)", "paint": "asc", "round": 1,
         "summary": "sum"},

        {"field": "max_reaction_time", "title": "Maximum Reaction Time", "paint": True, "round": 1, "summary": "max",
         "time_format": True},
        {"field": "max_speed_to_answer", "title": "Maximum Speed To Answer", "paint": True, "round": 1,
         "summary": "max", "time_format": True},

        {
            "field": "average_reaction_time",
            "title": "Average Reaction Time",
            "paint": True,
            "round": 1,
            "avgFor24": {
                "numerator": "average_reaction_time",
                "denominator": "average_reaction_time",
                "time_format": True,
            },
            "time_format": True,
        },
        {
            "field": "average_speed_to_answer",
            "title": "Average Speed To Answer",
            "paint": True,
            "round": 1,
            "avgFor24": {
                "numerator": "chat_total_reaction_time",
                "denominator": "chat_count_replies",
                "time_format": True,
            },
            "time_format": True,
        },
        {
            "field": "customer_satisfaction_index",
            "title": "Customer Satisfaction Index",
            "paint": "desc",
            "round": 1,
            "paint_type": "asc",
            "advancedSummaryAvg": {
                "numerator": "good_marks",
                "denominator": "all_marks",
                "round": 1,
            },
        },
    ])

    table.Items = pst_detailed_daily_data

    return table.getTable()

def get_daily_detailed_data(conn, bpm_conn, naumen_conn_1, naumen_conn_3, group, views, positions, channels, project_name=None, start_date=None, end_date=None, report_date=None):
    occupancy_report_day = get_occupancy_data_filtered(conn, bpm_conn, naumen_conn_1, naumen_conn_3,  group, 'hour', positions, views, project_name, start_date, end_date)
    ost_employers_data = get_operators_count(conn, bpm_conn, naumen_conn_1, naumen_conn_3, views, positions, report_date, start_date, end_date)

    main_data_report_day = get_main_aggregation_data(bpm_conn, group, 'hour', start_date, end_date, report_date)
    handling_times_report_day = get_average_handling_time_data(bpm_conn, group, 'hour', start_date, end_date)

    get_dagster_logger().info(f"handling_times_report_day = {handling_times_report_day}")

    csi_data_report_day = get_csi_data(bpm_conn, 'hour', group, channels, start_date, end_date)
    csi_data_report_day = [normalize_date_hour(r) for r in csi_data_report_day]

    #Общий ГОО
    main_data_report_day_joined = left_join_array(main_data_report_day, main_data_report_day, 'hour')
    detailed_daily_data_obank = left_join_array(main_data_report_day_joined, handling_times_report_day['items'], 'hour')
    detailed_daily_data_obank = left_join_array(detailed_daily_data_obank, occupancy_report_day['items'], 'hour')
    detailed_daily_data_obank = left_join_array(detailed_daily_data_obank, csi_data_report_day, 'hour')
    return left_join_array(detailed_daily_data_obank, ost_employers_data, 'hour')

def render_client_requests(conn, project_ids, date_list, end_date):
    requests_data = OmniDataGetter(start_date=date_list[0], end_date=end_date, conn=conn)
    requests_data.splits = []
    requests_data.project_ids = project_ids
    requests_data.set_days_trunked_data()
    items = requests_data.get_requests_data()
    get_dagster_logger().info(f"items = {items}")
    result = []
    unique_ids: Dict[str, int] = {}

    for item in items:
        key = f"{item['request_title']}-{item['request_label_title']}-{item['reason_title']}-{item['project_title']}-{item['request_status_title']}"

        if key not in unique_ids:
            unique_ids[key] = len(unique_ids) + 1

        project_title = {
            "Money": "O!Деньги",
            "Bank": "Халык"
        }.get(item["project_title"], item["project_title"])

        result.append({
            "id": unique_ids[key],
            "date": item["date"],
            "project_title": project_title,
            "request_title": item["request_title"],
            "reason_title": item["reason_title"],
            "request_label_title": item["request_label_title"],
            "count": item["count"],
            "status": item["request_status_title"],
        })

    table = PivotTable()
    table.Items = result
    table.ShowDoD = True
    table.ShowTop20 = True
    table.SliceCount = True
    table.topCount = 40
    table.ShowId = False
    table.ShowRank = True
    table.HideRow = True
    table.OrderByLastDate = 'desc'
    table.PaintType = "asc"
    table.TitleStyle = "width: 250px;"
    table.Fields = [
        {
            "field": "date",
            "type": "period",
            "format": "%d.%m",
            "title": "ID",
        },
        {"field": "id", "type": "row"},
        {"field": "count", "type": "value"},
    ]
    table.ShowSummaryXAxis = False
    table.DescriptionColumns = [
        {"field": "project_title", "title": "Проект", "style": "width: 250px;"},
        {"field": "status", "title": "Статус", "style": "width: 250px;"},
        {"field": "request_title", "title": "Запрос", "style": "width: 250px;"},
        {"field": "request_label_title", "title": "Подгруппа", "style": "width: 250px;"},
        {"field": "reason_title", "title": "Причина", "style": "width: 250px;"},
    ]
    return table.getTable()

def render_ivr_transitions(items, project=None):
    table = PivotTable()
    table.Items = items
    table.ShowRank = True
    table.ShowId = False
    table.ShowDoD = True
    table.SliceCount = False
    table.ShowTop20 = True
    table.PaintType = "asc"
    table.ShowSummaryXAxis = False
    table.TitleStyle = "width: 70px;"
    table.Fields = [
        {
            "field": "date",
            "type": "period",
            "format": "%d.%m",
            "title": "Дата",
        },
        {"field": "id", "type": "row"},
        {"field": "count", "type": "value", "round": 1},
    ]

    if project == "Saima":
        table.DescriptionColumns = [
            {
                "field": "parent_title",
                "title": "Клавиша",
                "style": "width: 200px; word-wrap: break-all;",
            },
            {
                "field": "title",
                "title": "Подклавиша",
                "style": "width: 200px; word-wrap: break-all;",
            },
            {
                "field": "description",
                "title": "Описание",
                "style": "width: 400px; word-wrap: break-all;",
            },
        ]
    else:
        table.DescriptionColumns = [
            {
                "field": "title",
                "title": "Название",
                "style": "width: 400px; word-wrap: break-all;",
            },
            {
                "field": "description",
                "title": "Описание",
                "style": "width: 400px; word-wrap: break-all;",
            },
        ]

    return table.getTable()

def gen_chart_line(
    items,
    fields,
    target = 60,
    is_target = True,
    color_map = None,
    default_color = '#000',
    label_interval = 1,
    ylabel = "Значение",
    graph_title = "Данный график показывает конверсию по звонкам"
):
    """
    Параметры:
    - items: список словарей с данными
    - fields: список описаний полей, например:
        [("ivr_total", "Поступило на IVR", "#F0047F"), ("ivr_answered", "Отвечено")]
      цвет можно указывать в трёх элементах или через color_map
    - color_map: {'field_name': '#RRGGBB'}
    - default_color: цвет по умолчанию, если не указан
    - label_interval: показывать подписи над точками с шагом label_interval (1 = все подписи)
    """

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
        now = datetime.now()
        for fmt in ('%d.%m.%Y', '%d.%m', '%d.%m.%y', '%Y-%m-%d'):
            try:
                dt = datetime.strptime(s, fmt)
                if fmt == '%d.%m':
                    dt = dt.replace(year=datetime.now().year)
                    if dt > now + timedelta(days=30):
                        dt = dt.replace(year=now.year - 1)
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
            left_pad = xticks[0] - step * 1.7
            right_pad = xticks[-1] + step * 1.7
        else:
            left_pad = xticks[0] - timedelta(hours=12)
            right_pad = xticks[0] + timedelta(hours=12)
        ax.set_xlim(left_pad, right_pad)

    ax.xaxis.set_major_formatter(mdates.DateFormatter('%d.%m'))
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))

    def choose_color(field_name, title: str) -> str:
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

        # Вместо простого sorted(...)
        combined = list(sorted(zip(parsed_dates, y), key=lambda x: x[0]))

        # Теперь распаковка пройдет без ошибок типизации
        sorted_dates, sorted_y = zip(*combined)

        # Теперь рисуем отсортированные данные
        ax.plot(sorted_dates, sorted_y, linewidth=2.5, label=ds['name'], color=color, zorder=3)

        # Для маркеров и аннотаций тоже используем отсортированные данные
        for idx, (dt_point, value) in enumerate(zip(sorted_dates, sorted_y)):
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
        ax.set_ylim(min_val, y_top)
        range_val = y_top - min_val
        band_height = max(6.0, range_val * 0.1)
        band_height = float(int(band_height))
        start_band = math.floor(min_val / band_height) * band_height
        end_band = math.ceil(y_top / band_height) * band_height
        i = start_band
        idx = 0
        while i < end_band:
            if idx % 2 == 1:
                ax.axhspan(i, i + band_height, alpha=0.05, color='gray', zorder=0)
            i += band_height
            idx += 1

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
            ncol=n_series
        )

    # --- Сохраняем результат в SVG и возвращаем base64 ---
    buf = io.BytesIO()
    fig.savefig(buf, format="svg", bbox_inches="tight")
    plt.close(fig)

    buf.seek(0)
    b64 = base64.b64encode(buf.read()).decode("utf-8")
    return b64

def render_pivot_table(items, val, sum=None):
    items = _normalize(items, period_field="date", row_field="projectName")
    if sum:
        sum = _normalize(sum, period_field="date", row_field="projectName")

    items = change_project_name(items)
    table = PivotTable()
    table.RoundValue = 1
    table.Items = items
    table.ShowId = False
    table.ShowProjectTitle = True

    if sum:
        table.ShowCalculated = True
        table.ShowAvgCalculated = True
        table.AllProjectsData = sum
        table.ShowSummaryXAxis = False
        table.ShowSummaryYAxis = False

    table.ShowAverageYAxis = True
    table.ShowMinYAxis = False
    table.ShowMaxYAxis = False
    table.OrderByTotal = "desc"
    table.PaintType = "asc"
    table.ShowTotalPercentage = False
    table.TitleStyle = "width: 250px;"
    table.Fields = [
        {"field": "date", "type": "period", "format": "%d.%m"},
        {"field": "projectName", "type": "row"},
        {"field": val, "type": "value", "round": 1},
    ]
    return table.getTable()



def change_project_name(items):
    def mapper(item: Dict[str, Any]) -> Dict[str, Any]:
        if "projectName" in item:
            if item["projectName"] == "Bank":
                item["projectName"] = "Халык"
            elif item["projectName"] == "Money":
                item["projectName"] = "О!Деньги"
        return item

    return list(map(mapper, items))

def get_ivr_transitions(nod_dinosaur_conn, callers, parents, period, start_date, end_date, report_date, date_list):
    getter = IVRTransactionDataGetter(start_date=start_date, end_date=end_date, nod_dinosaur_conn=nod_dinosaur_conn)
    setup_data_getter(getter, period, start_date, end_date, report_date, date_list)
    return getter.get_level_data(callers=callers, parents=parents)

def get_saima_ivr_transitions_data(nod_dinosaur_conn, callers, parents, period, start_date, end_date, report_date, date_list):
    getter = IVRTransactionDataGetter(start_date=start_date, end_date=end_date, nod_dinosaur_conn=nod_dinosaur_conn)
    setup_data_getter(getter, period, start_date, end_date, report_date, date_list)
    items = getter.get_other_projects_data(callers=callers, execs=[], parents=parents)
    first_lvl_ids = {57, 58, 59, 60}
    second_lvl_ids = {122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133}
    by_id = {it.get("id"): it for it in items}

    for it in items:
        if it.get("id") in second_lvl_ids:
            parent = by_id.get(it.get("parent_id"))
            if parent:
                it["parent_title"] = parent.get("title")
            it["parent_id"] = 53

    items = [it for it in items if it.get("id") not in first_lvl_ids]

    for it in items:
        if not it.get("parent_title"):
            it["parent_title"] = it.get("title", "")
            it["title"] = ""

    return items

def render_chat_data(conn, period, project_ids, pst_position_ids, project_replacements=None, start_date=None, end_date=None):
    if project_replacements is None:
        project_replacements = {}
    getter = MessagingPlatformDataGetter(start_date=start_date, end_date=end_date, conn=conn)
    setup_data_getter(getter, period, start_date, end_date)
    getter.project_ids = project_ids
    getter.pst_positions = pst_position_ids
    chat_data = getter.get_detailed()

    get_dagster_logger().info(f"chat_data = {chat_data}")

    if project_replacements:
        for item in chat_data:
            if "project" in item and item["project"] in project_replacements:
                item["project"] = project_replacements[item["project"]]

    table = PivotTable()
    table.RoundValue = 1
    table.RowTitleStyle = "font-weight: bold; text-align: left"
    table.PaintType = "asc"
    table.OrderByTotal = "desc"

    table.HideRow = False
    table.ShowId = False

    table.ShowProjectTitle = False

    table.Items = chat_data
    table.Fields = [
        {"field": "date", "type": "period", "format": "%d.%m"},
        {"field": "project", "type": "row"},
        {"field": "total", "type": "value", "round": 1},
    ]
    return table.getTable()


def render_jira_graph(conn, project, start_date, end_date):
    get_dagster_logger().info(f"project = {project}")
    items = get_jira_data(conn=conn, project=project, start_date=start_date, end_date=end_date)
    get_dagster_logger().info(f"items = {items}")
    max_value = get_max_value(items)
    chart = MultiAxisChart(
        graph_title="",
        y_axes_name="Количество",
        y_axes_min=0,
        y_axes_max=max_value,
        fields=[
            {"field": "date", "title": "", "type": "label"},
            {
                "field": "sla_reached",
                "title": "Кол-во закрытых задач вовремя",
                "chartType": "grouped_bars",
                "round": 0,
            },
            {
                "field": "all_resolved_issues",
                "title": "Кол-во закрытых задач",
                "chartType": "grouped_bars",
                "round": 0,
            },
            {
                "field": "sla",
                "title": "SLA",
                "chartType": "line",
                "round": 0,
            },
        ],
        items=items,
        dot=True
    )
    chart.combine(3, max_percent=max_value)

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


def get_max_value(items):
    max_val = None
    for item in items:
        value = item["all_resolved_issues"] if isinstance(item, dict) else item.all_resolved_issues
        if max_val is None or value > max_val:
            max_val = value

    if max_val is None:
        return 0

    if max_val >= 100:
        max_val = round(max_val / 90) * 100

    return max_val * 1.5

def render_pst_daily_profile(conn, bpm_conn, naumen_conn_1, naumen_conn_3, project_ids, groups, channels, period, timetable_views, position_ids, start_date, end_date, report_date):
    pst_employers_data = get_operators_count(view_ids = timetable_views, positions=position_ids, date = report_date, conn=conn, bpm_conn=bpm_conn, naumen_conn_1= naumen_conn_1, naumen_conn_3 = naumen_conn_3, start_date=start_date, end_date=end_date)

    mp_aggregated_data = get_mp_aggregated_data(bpm_conn, period, project_ids, start_date, end_date, report_date)
    get_dagster_logger().info(f"mp_aggregated_data = {mp_aggregated_data}")

    pst_csi_data_report_day = get_csi_data(bpm_conn, period, groups, channels, start_date, end_date)
    pst_csi_data_report_day = [normalize_date_hour(r) for r in pst_csi_data_report_day]

    detailed_daily_data_gpo = left_join_array(pst_employers_data, mp_aggregated_data, period)

    result =  left_join_array(detailed_daily_data_gpo, pst_csi_data_report_day, period)
    return result



def get_mp_aggregated_data(conn, period, project_ids, start_date, end_date, report_date=None):
    getter = MessagingPlatformDataGetter(start_date=start_date, end_date=end_date, conn=conn)
    setup_data_getter(getter, period, start_date, end_date, report_date)
    getter.date_format = 'HH24:MI:SS'
    getter.date_field = 'hour'
    getter.project_ids = project_ids

    get_dagster_logger().info(f"start_date = {getter.start_date}")

    get_dagster_logger().info(f"end_date = {getter.end_date}")

    return getter.get_aggregated_data_detailed()


def render_technical_works_table(conn, period, project_ids, key=None, start_date=None, end_date=None, report_date=None):
    logger = get_dagster_logger()
    logger.info(f"render_technical_works_table - period: {period}, project_ids: {project_ids}, key: {key}")
    logger.info(f"render_technical_works_table - start_date: {start_date}, end_date: {end_date}, report_date: {report_date}")
    
    getter = OmniDataGetter(start_date=start_date, end_date=end_date, conn=conn)
    setup_data_getter(getter, period, start_date, end_date, report_date=report_date)
    getter.date_format = 'HH24:MI:SS'
    getter.project_ids = project_ids
    
    logger.info(f"render_technical_works_table - getter.start_date: {getter.start_date}, getter.end_date: {getter.end_date}")
    logger.info(f"render_technical_works_table - getter.project_ids: {getter.project_ids}")
    logger.info(f"render_technical_works_table - getter.trunc: {getter.trunc}")
    
    items = getter.get_omni_actual_technical_works_data()

    print('items', items)
    get_dagster_logger().info(f"render_technical_works_table - items count: {len(items)}")
    get_dagster_logger().info(f"render_technical_works_table - items = {items}")
    table = BasicTable()
    table.Fields = [
        {"title": "Дата начала", "field": "date_begin", "date_format": "%d.%m.%Y, %H:%M"},
        {"title": "Дата завершения", "field": "date_end", "date_format": "%d.%m.%Y, %H:%M"},
        {"title": "Время простоя", "field": "downtime"},
        {"title": "Сервис", "field": "service"},
        {"title": "Описание", "field": "description"},
    ]
    if not key:
        table.Fields.append({"title": "Количество обращений", "field": "technical_work_count"})
    table.Items = items
    return table.getTable()

def render_pst_daily_profile_table(indicators, formatted_dates, date_list):
    task = get_task_title(formatted_dates)

    for indicator in indicators:
        gen_task_row(task, *indicator, date_list=date_list)

    return task

def render_pst_daily_profile_table_lite(indicators, formatted_dates, date_list):
    task = get_task_title_lite(formatted_dates)

    for indicator in indicators:
        get_task_row_lite(task, *indicator, date_list=date_list)

    return task

def render_pst_daily_profile_table_new(indicators, formatted_dates, date_list):
    task = get_task_title_new(formatted_dates)

    for indicator in indicators:
        gen_task_row_new(task, *indicator, date_list=date_list)

    return task



def get_task_title(formatted_dates):
    task = TableConstructor()
    task.genCellTH("Показатель", "word-wrap: break-word; width: 250px;")
    task.genCellTH("План")
    task.multiGenCellTH(formatted_dates)
    task.genCellTH("Среднее значение", "word-wrap: break-word; width: 160px;")
    task.genRow()
    return task

def get_task_title_lite(formatted_dates):
    task = TableConstructor()
    task.genCellTH("Показатель", "word-wrap: break-word; width: 250px;")
    task.genCellTH("Ед.измерения")
    task.multiGenCellTH(formatted_dates)
    task.genCellTH("Среднее значение", "word-wrap: break-word; width: 160px;")
    task.genRow()
    return task

def get_task_title_new(formatted_dates):
    task = TableConstructor()
    task.genCellTH("Показатель", "word-wrap: break-word; width: 250px;")
    task.multiGenCellTH(formatted_dates)
    task.genCellTH("Среднее значение", "word-wrap: break-word; width: 160px;")
    task.genRow()
    return task

def gen_task_row(task, indicator, plan, items, abbreviation, plan_val, operator, month_val, period='date', date_list=None):
    task.genCellTD(indicator, "font-weight: bold;")
    task.genCellTD(plan)

    logger = get_dagster_logger()

    logger.info(f"date_list: {date_list}")

    task.multiGenCellTD(
        get_data_in_dates(
            items,
            date_list,
            period,
            abbreviation,
            1,
        ),
        None,
        {
            "plan": plan_val,
            "operator": str(operator),
            "style": "background: #FF4D6E; ",
        },
    )

    task.genCellTD(month_val, "font-weight: bold;")
    task.genRow()
    return task

def set_jira_issues_data(items, projects):
    result = []
    grouped_data = {}

    for item in items:
        id_ = str(item.get("id", ""))
        title = item.get("title", "")
        issuetype = str(item.get("issuetype", ""))
        date = item.get("date", "")
        count = int(item.get("count", 0) or 0)
        not_resolv = int(item.get("not_resolved", 0) or 0)
        resolved = int(item.get("resolved", 0) or 0)

        raw_id = f"{id_}-{title}-{issuetype}"
        crc_id = zlib.crc32(raw_id.encode("utf-8"))

        key = f"{id_}-{title}-{date}-{issuetype}"

        if key not in grouped_data:
            grouped_data[key] = {
                "id": crc_id,
                "date": date,
                "project_title": (projects.get(id_, "")),
                "title": title,
                "count": 0,
                "not_resolved": 0,
                "resolved": 0,
            }

        grouped_data[key]["count"] += count
        grouped_data[key]["not_resolved"] += not_resolv
        grouped_data[key]["resolved"] += resolved

    result = list(grouped_data.values())
    return result

def render_jira_issues_table(items, key=False):
    table = PivotTable()
    table.Items = items
    table.ShowDoD = True
    table.ShowTop20 = True
    table.topCount = 10
    table.OrderByLastDate = 'desc'
    table.ShowId = False
    table.HideRow = True
    table.ShowRank = True
    table.ShowSummaryXAxis = False
    table.PaintType = "asc"
    table.TitleStyle = "width: 250px;"
    table.Fields = [
        {"field": "date", "type": "period", "format": "%d.%m"},
        {"field": "id", "type": "row"},
        {"field": "count", "type": "value"},
    ]

    if key:
        table.DescriptionColumns = [
            {"field": "title", "title": "Заявка", "style": "width: 500;"},
        ]
    else:
        table.DescriptionColumns = [
            {"field": "project_title", "title": "Проект", "style": "width: 250px;"},
            {"field": "title", "title": "Инцидент", "style": "width: 300px;"},
        ]

    return table.getTable()

def render_jira_issues_table_new(items):
    sums = {}
    for title, subtasks in items.items():
        total = 0
        for row in subtasks:
            val = row.get("count")
            total += int(0 if val is None else val)
        sums[title] = total

    sorted_titles = sorted(sums.keys(), key=lambda t: sums[t], reverse=True)

    sortedData: dict = {}
    for title in sorted_titles:
        sortedData[title] = items[title]

    tableBuilder = IssueHierarchicalTable()
    tableBuilder.setRawData(sortedData)

    return tableBuilder.getTable()

def aggregateData(data: list[dict]) -> dict:
    result1: dict = {}
    result1["date"] = data[1]["date"]

    for entry in data:
        for key, value in entry.items():
            if key != "date":
                if key not in result1:
                    result1[key] = 0
                result1[key] += value if isinstance(value, (int, float)) else 0
    return result1

def render_jira_sla_issues_table(data):
    table = BasicTable()
    table.Fields = [
        {"field": "number", "title": "#"},
        {"field": "title", "title": "Заявка", "total_title": True},
        {
            "field": "count",
            "title": "Поступило задач",
            "round": 1,
            "summary": "sum",
            "paint": True,
        },
        {
            "field": "all_resolved_issues",
            "title": "Закрыто задач",
            "round": 1,
            "summary": "sum",
            "paint": True,
        },
        {
            "field": "sla_reached",
            "title": "Закрыто вовремя",
            "round": 1,
            "summary": "sum",
            "paint": True,
        },
        {
            "field": "sla",
            "title": "SLA%",
            "round": 1,
            "advancedSummaryAvg": {
                "numerator": "sla_reached",
                "denominator": "all_resolved_issues",
                "round": 1,
            },
            "paint": "desc",
        },
    ]
    table.Items = data
    return table

def render_jira_issues_status_table(items, unique_issue_data) -> PivotTable:
    table = PivotTable()
    table.RoundValue = 1
    table.RowTitleStyle = "font-weight: bold;"
    table.PaintType = "asc"
    table.ShowMaxYAxis = False
    table.ShowMinYAxis = False
    table.ShowCalculated = True
    table.ShowId = False
    table.HideRow = True
    table.AllProjectsData = unique_issue_data
    table.SummaryXTitle = "Итого уникальных задач"
    table.ShowAverageYAxis = False
    table.OrderByTotal = "desc"
    table.Items = items
    table.Fields = [
        {
            "field": "month",
            "type": "period",
            "format": "%b.%y",
            "title": "ID",
            "style": "width: 10px;",
        },
        {"field": "id", "type": "row"},
        {"field": "count", "type": "value"},
    ]
    table.DescriptionColumns = [
        {
            "field": "title",
            "title": "Заявка",
            "style": "width: 500;",
        },
        {
            "field": "subtask_title",
            "title": "Подзадача",
            "style": "width: 300px;",
        },
        {
            "field": "status",
            "title": "Статус",
            "style": "width: 300px;",
        },
    ]
    return table

def render_jira_issues_status_table_new(raw_group: Any, mode = None):
    if isinstance(raw_group, str):
        try:
            raw_group = json.loads(raw_group)
        except json.JSONDecodeError as e:
            raise ValueError("Invalid JSON passed to renderJiraIssuesStatusTableNew: " + str(e))

    # 2) Определяем entries
    if isinstance(raw_group, list) and raw_group and isinstance(raw_group[0], dict) and "month" in raw_group[0]:
        entries = raw_group
        root_group: Dict[str, Any] = {}
    else:
        entries = (raw_group or {}).get("entries", [])
        root_group = raw_group or {}

    def sanitize_title(title: str) -> str:
        return re.sub(r"^(?:MAIN SERVICES|MASS ACCIDENTS|CUSTOMER SERVICES)\s*", "", title.strip(), flags=re.I)

    def sanitize_subtasks(subtasks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for it in subtasks:
            it = dict(it)
            st = it.get("subtask_title")
            if isinstance(st, str):
                it["subtask_title"] = re.sub(
                    r"^(?:MAIN SERVICES|MASS ACCIDENTS|CUSTOMER SERVICES)\s*", "", st.strip(), flags=re.I | re.U
                )
            out.append(it)
        return out

    groups: List[Dict[str, Any]] = []

    if mode == "ma":
        # Агрегация по названию заявки и месяцу
        aggregated: Dict[str, Dict[str, Dict[str, Any]]] = {}
        for entry in entries:
            if not isinstance(entry, dict) or "month" not in entry or "subtasks" not in entry:
                continue
            month = entry["month"]
            count = int(entry.get("count", 0))
            items_raw = entry["subtasks"]
            items = json.loads(items_raw) if isinstance(items_raw, str) else (items_raw or [])
            raw_title = entry.get("title", root_group.get("title", "Без названия"))
            raw_title = sanitize_title(raw_title if isinstance(raw_title, str) else "Без названия")
            raw_subtasks = sanitize_subtasks(items)
            key_title = raw_title.casefold()

            if key_title not in aggregated or month not in aggregated[key_title]:
                aggregated.setdefault(key_title, {})[month] = {
                    "title": raw_title,
                    "count": count,
                    "subtasks": list(raw_subtasks),
                }
            else:
                aggregated[key_title][month]["count"] += count
                aggregated[key_title][month]["subtasks"].extend(raw_subtasks)

        for months_data in aggregated.values():
            for month, data in months_data.items():
                groups.append({
                    "title": data["title"],
                    "область": "Все области КР",
                    "ответственный": "Алексей Сураев",
                    "month": month,
                    "unique_count": data["count"],
                    "items": data["subtasks"],
                })
    else:
        # Оригинальная логика: по каждой записи
        for entry in entries:
            if not isinstance(entry, dict) or "month" not in entry or "subtasks" not in entry:
                continue
            month = entry["month"]
            unique_count = int(entry.get("count", 0))
            items_raw = entry["subtasks"]
            items = json.loads(items_raw) if isinstance(items_raw, str) else (items_raw or [])

            title = root_group.get("title")
            if isinstance(title, str):
                title = sanitize_title(title)

            raw_subtasks = sanitize_subtasks(items)

            groups.append({
                "title": title,
                "область": root_group.get("область", "Все области КР"),
                "month": month,
                "unique_count": unique_count,
                "items": raw_subtasks,
            })
    return GroupedPivotTable(groups)

def get_unique_issues_data(data):
    result: Dict[str, int] = {}

    for issue in data:
        month = issue["month"] if isinstance(issue, dict) else issue.month
        subtask_title = issue.get("subtask_title") if isinstance(issue, dict) else getattr(issue, "subtask_title", None)
        count = issue.get("count") if isinstance(issue, dict) else getattr(issue, "count", 0)

        if month not in result:
            result[month] = 0

        if not subtask_title:
            result[month] += int(count or 0)

    formatted_result = [
        {"month": month, "count": count}
        for month, count in result.items()
    ]

    return formatted_result

def aggregate_data(data: List[Dict[str, Any]]) -> Dict[str, Any]:
    result1: Dict[str, Any] = {}
    result1["date"] = data[1]["date"]
    for entry in data:
        for key, value in entry.items():
            if key == "date":
                continue
            if key not in result1:
                result1[key] = 0
            try:
                result1[key] += float(value)
            except (TypeError, ValueError):
                result1[key] += 0
    return result1

def get_jira_data(conn, project, start_date, end_date):
    jira_data = JiraDataGetterNext(start_date=start_date, end_date=end_date,
                                   dwh_conn=conn)

    def cb():
        return ("sjisd.project_key = ANY(%(jira_project_key)s::text[])",
                {"jira_project_key": project["jira_project_key"]})

    get_dagster_logger().info(f'project_key = {project["jira_project_key"]}')
    get_dagster_logger().info(f"id = {project['id']}")
    return jira_data.get_sla_by_groups(
        conn=conn,
        groups=[project["id"]],
        group=False,
        callback=cb,
    )




def get_jira_sla_data(conn, project_keys=None):
    if project_keys is None:
        project_keys = []
    getter = JiraDataGetterNext(start_date=start_date, end_date=end_date, conn=conn)
    getter.set_months_trunked_data()
    getter.setTitle = True
    getter.ProjectKeys = project_keys
    closed = getter.get_jira_sla_group_by_project()
    created = getter.get_created_issues_by_responsible_groups_with_id()
    return left_join_array(closed, created, 'title')

def get_jira_sla_data_new(jira, dwh_conn, jira_nur, jira_saima, project_keys=None, start_date=None, end_date=None):
    if project_keys is None:
        project_keys = []
    getter = JiraDataGetterNext(start_date=start_date, end_date=end_date, jira=jira,
                                jira_nur=jira_nur, jira_saima=jira_saima, dwh_conn=dwh_conn)
    getter.set_months_trunked_data()
    # getter.setTitle = True
    getter.ProjectKeys = project_keys
    data = getter.get_jira_sla_by_issue_type_and_subtask(project_keys)
    return data

def setup_data_getter(data_getter, period, start_date, end_date, report_date=None, date_list=None):
    if period == 'hour':
        data_getter.start_date = report_date
        data_getter.end_date = end_date
        data_getter.set_hours_trunked_data()
        data_getter.date_field = 'hour'
    elif period == 'day':
        data_getter.start_date = start_date
        data_getter.end_date = end_date
        data_getter.set_days_trunked_data()
    elif period == 'month':
        data_getter.start_date = start_date
        data_getter.end_date = end_date
        data_getter.set_months_trunked_data()
    elif period == 'week':
        data_getter.start_date = date_list[0]
        data_getter.end_date = end_date
        data_getter.set_days_trunked_data()
