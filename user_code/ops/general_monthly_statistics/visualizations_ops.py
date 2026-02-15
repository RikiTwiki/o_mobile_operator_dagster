from collections import defaultdict
from datetime import datetime
from pathlib import Path

from dagster import op
from sqlalchemy.util import py314

from resources import ReportUtils
from utils.getters.topics_data_getter import TopicsDataGetter
from utils.mail.mail_report import MailReport
from utils.templates.tables.basic_table import BasicTable
from utils.templates.tables.pivot_table import PivotTable

dates = ReportUtils()
dates = dates.get_utils()
start_date = dates.StartDate
end_date = dates.EndDate
date_list = dates.Dates
report_date = dates.ReportDate
month_start = dates.MonthStart
month_end = dates.MonthEnd
start_date_year = dates.StartDateYear



@op
def render_main_data_dynamic_general_joined_monthly_op(context, main_data_dynamic_general_joined_monthly):
    table = BasicTable()
    table.Fields = [
        {'field': 'date', 'title': 'Дни'},
        {'field': 'ivr_total', 'title': 'Поступило на IVR'},
        {'field': 'ivr_ivr', 'title': 'Завершены в IVR'},
        {'field': 'total_to_operators', 'title': 'Распределено на операторов'},
        {'field': 'answered', 'title': 'Отвечено операторами'},
        {'field': 'lost', 'title': 'Потерянные'},
    ]
    table.Items = main_data_dynamic_general_joined_monthly

    html = table.getTable()
    return html

@op
def render_main_data_month_dynamic_general_joined_op(context, main_data_month_dynamic_general_joined):
    table = BasicTable()
    table.Fields = [
        {'field': 'date', 'title': 'Дни'},
        {'field': 'ivr_total', 'title': 'Поступило на IVR'},
        {'field': 'ivr_ivr', 'title': 'Завершены в IVR'},
        {'field': 'total_to_operators', 'title': 'Распределено на операторов'},
        {'field': 'answered', 'title': 'Отвечено операторами'},
        {'field': 'lost', 'title': 'Потерянные'},
    ]
    table.Items = main_data_month_dynamic_general_joined

    html = table.getTable()
    return html

@op
def render_topics_data_op(context, topics_data):
    table = PivotTable()
    table.Items = topics_data
    table.OrderByTotal = 'desc'
    table.PaintType = 'asc'
    table.ShowTotalPercentage = False
    table.ShowMaxYAxis = False
    table.ShowMinYAxis = False
    table.ShowAverageYAxis = False
    table.ShowSummaryYAxis = False
    table.TitleStyle = 'width: 250px;'
    table.Fields = [
        {
            'field': 'date',
            'type': 'period',
            'format': '%d.%m',
            'title': 'Название',
        },
        {'field': 'general_group', 'type': 'row'},
        {'field': 'count', 'type': 'value'},
    ]

    html = table.getTable()
    return html

#ToDo
@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1'})
def render_general_group_data_op(context, topics_data):
    table = PivotTable()
    table.Items = topics_data
    table.OrderByTotal = 'desc'
    table.PaintType = 'asc'
    table.ShowTotalPercentage = False
    table.ShowMaxYAxis = False
    table.ShowMinYAxis = False
    table.ShowAverageYAxis = False
    table.ShowSummaryYAxis = False
    table.TitleStyle = 'width: 250px;'
    table.Fields = [
        {
            'field': 'date',
            'type': 'period',
            'format': '%d.%m',
            'title': 'Название',
        },
        {'field': 'general_group', 'type': 'row'},
        {'field': 'count', 'type': 'value'},
    ]

    topics_data = TopicsDataGetter(start_date=month_start, end_date=month_end, conn=context.resources.source_dwh)
    topics_data.set_months_trunked_data()
    table = PivotTable()

    tables = {}
    sorted_general = table._getRows()
    for general_level in sorted_general:
        if general_level is None:
            continue

        items = topics_data.get_general_group_data(8, general_level)
        if not items:
            continue


        t = PivotTable()
        t.Items = items
        t.OrderByTotal = 'desc'
        t.PaintType = 'asc'
        t.ShowTotalPercentage = False
        t.ShowMaxYAxis = False
        t.ShowMinYAxis = False
        t.ShowAverageYAxis = False
        t.ShowSummaryYAxis = False
        t.TitleStyle = 'width: 250px;'
        t.Fields = [
            {'field': 'date', 'type': 'period', 'format': '%d.%m', 'title': 'Всп. группа'},
            {'field': 'auxiliary_group', 'type': 'row'},
            {'field': 'count', 'type': 'value'},
        ]
        tables[general_level] = t.getTable()
    return tables

@op
def render_main_index_op(context, main_index):
    table = BasicTable()
    table.Fields = [
        {'field': 'date', 'title': 'Месяцы'},
        {'field': 'service_level', 'title': 'Service Level'},
        {'field': 'handled_calls_rate', 'title': 'Handled Calls Rate'},
        {'field': 'average_handling_time', 'title': 'Average Handling Time'},
        {'field': 'repeat_calls', 'title': 'Repeat Calls'},
        {'field': 'customer_satisfaction_index', 'title': 'Customer Satisfaction Index'},
    ]
    table.Items = main_index

    html = table.getTable()
    return html

@op
def render_service_level_arpu_op(context, main_data_dynamic_general_arpu):
    service_level_arpu = get_basic_data(main_data_dynamic_general_arpu, 'service_level')
    table = BasicTable()
    table.Fields = [
        {'field': 'date', 'title': 'Месяцы'},
        {'field': 'High', 'title': 'High'},
        {'field': 'Low', 'title': 'Low'},
        {'field': 'Middle', 'title': 'Middle'},
        {'field': 'New Arpu', 'title': 'New Arpu'},
        {'field': 'TOP', 'title': 'TOP'},
        {'field': 'UNI', 'title': 'UNI'},
        {'field': 'Very High', 'title': 'Very High'},
    ]

    table.Items = service_level_arpu

    html = table.getTable()
    return html

@op
def render_max_pickup_time_arpu_op(context, handling_times_data_month_general_arpu):
    max_pickup_time_arpu = get_basic_data(handling_times_data_month_general_arpu, 'max_pickup_time', 2)
    table = BasicTable()
    table.Fields = [
        {'field': 'date', 'title': 'Месяцы'},
        {'field': 'High', 'title': 'High'},
        {'field': 'Low', 'title': 'Low'},
        {'field': 'Middle', 'title': 'Middle'},
        {'field': 'New Arpu', 'title': 'New Arpu'},
        {'field': 'TOP', 'title': 'TOP'},
        {'field': 'UNI', 'title': 'UNI'},
        {'field': 'Very High', 'title': 'Very High'},
    ]
    table.Items = max_pickup_time_arpu

    html = table.getTable()
    return html

@op
def render_csi_data_month_main_op(context, csi_data_month_main):
    table = BasicTable()
    table.Fields = csi_data_month_main['labels']
    table.Items = csi_data_month_main['items']
    html = table.getTable()
    return html





def get_basic_data(data, key, round_=0):
    bucket = defaultdict(dict)
    for item in data or []:
        d = item.get("date")
        a = item.get("arpu")
        v = item.get(key)
        if d is None or a is None or v is None:
            continue
        date_key = d if isinstance(d, str) else d.strftime("%Y-%m-%d")
        bucket[date_key][a] = round(float(v), round_) if round_ > 0 else v

    out = []
    for d, arpu_vals in bucket.items():
        try:
            ds = datetime.fromisoformat(str(d)).strftime("%Y-%m-%d")
        except Exception:
            ds = str(d)  # fallback, если формат нестандартный
        row = {"date": ds}
        row.update(dict(arpu_vals))
        out.append(row)
    return out

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1'})
def render_data_with_bad_marks_op(context):
    topics_data = TopicsDataGetter(start_date=month_start, end_date=month_end, conn=context.resources.source_dwh)
    topics_data.set_days_trunked_data()
    table = PivotTable()
    table.Items = topics_data.get_data_with_bad_marks(8)
    table.OrderByTotal = 'desc'
    table.PaintType = 'asc'
    table.TitleStyle = 'width: 250px;'
    table.Fields = [
        {'field': 'date', 'type': 'period', 'format': '%d.%m', 'title': 'Тематика'},
        {'field': 'themes_joined', 'type': 'row'},
        {'field': 'total', 'type': 'value'},
    ]

    html = table.getTable()
    return html

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1'})
def render_data_without_trunc8_op(context):
    topics_data = TopicsDataGetter(start_date=month_start, end_date=month_end, conn=context.resources.source_dwh)
    topics_data.set_days_trunked_data()

    table = BasicTable()
    table.Fields = [
        {'field': 'general_group', 'title': 'Основная группа'},
        {'field': 'auxiliary_group', 'title': 'Вспомогательная группа'},
        {'field': 'topic', 'title': 'Тематика'},
        {'field': 'title', 'title': 'Название'},
        {'field': 'count', 'title': 'Количество', 'paint': True, 'summary': 'sum'},
    ]
    table.Items = topics_data.get_data_without_trunc(8)
    html = table.getTable()
    return html

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1'})
def render_data_without_trunc16_op(context):
    topics_data = TopicsDataGetter(start_date=month_start, end_date=month_end, conn=context.resources.source_dwh)
    topics_data.set_days_trunked_data()

    table = BasicTable()
    table.Fields = [
        {'field': 'general_group', 'title': 'Основная группа'},
        {'field': 'auxiliary_group', 'title': 'Вспомогательная группа'},
        {'field': 'topic', 'title': 'Тематика'},
        {'field': 'title', 'title': 'Название'},
        {'field': 'count', 'title': 'Количество', 'paint': True, 'summary': 'sum'},
    ]
    table.Items = topics_data.get_data_without_trunc(16)
    html = table.getTable()
    return html

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1'})
def render_data_without_trunc7_op(context):
    topics_data = TopicsDataGetter(start_date=month_start, end_date=month_end, conn=context.resources.source_dwh)
    topics_data.set_days_trunked_data()

    table = BasicTable()
    table.Fields = [
        {'field': 'general_group', 'title': 'Основная группа'},
        {'field': 'auxiliary_group', 'title': 'Вспомогательная группа'},
        {'field': 'topic', 'title': 'Тематика'},
        {'field': 'title', 'title': 'Название'},
        {'field': 'count', 'title': 'Количество', 'paint': True, 'summary': 'sum'},
    ]
    table.Items = topics_data.get_data_without_trunc(7)
    html = table.getTable()
    return html

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1'})
def render_data_without_trunc9_op(context):
    topics_data = TopicsDataGetter(start_date=month_start, end_date=month_end, conn=context.resources.source_dwh)
    topics_data.set_days_trunked_data()

    table = BasicTable()
    table.Fields = [
        {'field': 'general_group', 'title': 'Основная группа'},
        {'field': 'auxiliary_group', 'title': 'Вспомогательная группа'},
        {'field': 'topic', 'title': 'Тематика'},
        {'field': 'title', 'title': 'Название'},
        {'field': 'count', 'title': 'Количество', 'paint': True, 'summary': 'sum'},
    ]
    table.Items = topics_data.get_data_without_trunc(9)
    html = table.getTable()
    return html


@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1'})
def render_data_without_trunc6_op(context):
    topics_data = TopicsDataGetter(start_date=month_start, end_date=month_end, conn=context.resources.source_dwh)
    topics_data.set_days_trunked_data()

    table = BasicTable()
    table.Fields = [
        {'field': 'general_group', 'title': 'Основная группа'},
        {'field': 'auxiliary_group', 'title': 'Вспомогательная группа'},
        {'field': 'topic', 'title': 'Тематика'},
        {'field': 'title', 'title': 'Название'},
        {'field': 'count', 'title': 'Количество', 'paint': True, 'summary': 'sum'},
    ]
    table.Items = topics_data.get_data_without_trunc(6)
    html = table.getTable()
    return html

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1'})
def render_data_without_trunc10_op(context):
    topics_data = TopicsDataGetter(start_date=month_start, end_date=month_end, conn=context.resources.source_dwh)
    topics_data.set_days_trunked_data()

    table = BasicTable()
    table.Fields = [
        {'field': 'general_group', 'title': 'Основная группа'},
        {'field': 'auxiliary_group', 'title': 'Вспомогательная группа'},
        {'field': 'topic', 'title': 'Тематика'},
        {'field': 'title', 'title': 'Название'},
        {'field': 'count', 'title': 'Количество', 'paint': True, 'summary': 'sum'},
    ]
    table.Items = topics_data.get_data_without_trunc(10)
    html = table.getTable()
    return html

@op(required_resource_keys={'source_dwh', 'source_naumen_3', 'source_naumen_1'})
def render_data_without_trunc14_op(context):
    topics_data = TopicsDataGetter(start_date=month_start, end_date=month_end, conn=context.resources.source_dwh)
    topics_data.set_days_trunked_data()

    table = BasicTable()
    table.Fields = [
        {'field': 'general_group', 'title': 'Основная группа'},
        {'field': 'auxiliary_group', 'title': 'Вспомогательная группа'},
        {'field': 'topic', 'title': 'Тематика'},
        {'field': 'title', 'title': 'Название'},
        {'field': 'count', 'title': 'Количество', 'paint': True, 'summary': 'sum'},
    ]
    table.Items = topics_data.get_data_without_trunc(14)
    html = table.getTable()
    return html

@op
def render_price_plans_with_bad_marks_op(context, price_plans_with_bad_marks):
    table = PivotTable()
    table.Items = price_plans_with_bad_marks
    table.OrderByTotal = 'desc'
    table.PaintType = 'asc'
    table.TitleStyle = 'width: 250px;'
    table.Fields = [
        {'field': 'date', 'type': 'period', 'format': '%d.%m', 'title': 'Тарифный план'},
        {'field': 'price_plane_title', 'type': 'row'},
        {'field': 'total', 'type': 'value'},
    ]

    html = table.getTable()
    return html

@op(required_resource_keys={"report_utils"})
def report_pdf_monthly_general(context, fig1, fig2, fig3, fig4, fig5, fig6, fig7, fig8, fig9, fig10, fig11, fig12, fig13, fig14, fig15, fig16):
    report = MailReport()
    report.image_mode = "file"

    APP_ROOT = Path(__file__).resolve().parents[2]
    LOGO_PATH = APP_ROOT / "utils" / "logo_png" / "logo.png"

    report.logo_file_override = str(LOGO_PATH)
    dates = context.resources.report_utils.get_utils()
    report_date = datetime.strptime(dates.ReportDate, "%Y-%m-%d")

    p1 = ""
    p1 += report.set_basic_header(f"ОБЩАЯ ЕЖЕДНЕВНАЯ СТАТИСТИКА ИСС НА {report_date.strftime('%d.%m.%Y')}")
    p1 += report.add_content_to_statistic(fig1)
    p1 += report.add_content_to_statistic(fig2)
    p1 += report.add_content_to_statistic(fig3)

    p2 = ""
    p2 += report.add_content_to_statistic(fig4)
    p2 += report.add_content_to_statistic(fig5)
    p2 += report.add_content_to_statistic(fig6)

    p3 = ""
    p3 += report.add_content_to_statistic(fig7)
    p3 += report.add_content_to_statistic(fig8)
    p3 += report.add_content_to_statistic(fig9)

    p4 = ""
    p4 += report.add_content_to_statistic(fig10)
    p4 += report.add_content_to_statistic(fig11)
    p4 += report.add_content_to_statistic(fig12)

    p5 = ""
    p5 += report.add_content_to_statistic(fig13)
    p5 += report.add_content_to_statistic(fig14)
    p5 += report.add_content_to_statistic(fig15)

    p6 = ""
    p6 += report.add_content_to_statistic(fig16)
    # p6 += report.add_content_to_statistic(fig17)
    # p6 += report.add_content_to_statistic(fig18)
    #
    # p7 = ""
    # p7 += report.add_content_to_statistic(fig19)

