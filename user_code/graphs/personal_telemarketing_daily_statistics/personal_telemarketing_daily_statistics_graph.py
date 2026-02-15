# user_code/graphs/personal_telemarketing_daily_statistics/personal_telemarketing_daily_statistics_graph.py
from dagster import graph

# Берём парсинг дат и геттеры данных из общего телемаркетинга
from ops.telemarketing_daily_statistics.process_ops import (
    parse_dates_telemarketing,
    sales_data_getter_op,
    calls_data_getter_op,

    upload_report_instance_personal_gtm_op,

)

# Персональные визуализации и сборка PDF
from ops.personal_telemarketing_daily_statistics.visualization_ops import (
    render_personal_sales_table_op,
    render_personal_sales_dynamic_graph_op,
    render_personal_activation_table_op,
    render_coffee_table_op,
    render_lunch_table_op,
    render_dnd_table_op,
    render_work_time_table_op,
    render_request_time_table_op,
    render_summary_request_table_op,
    render_occupancy_table_op,
    render_calls_duration_table_op,
    render_personal_aht_table_op,
    report_pdf_personal_gtm,
)


@graph()
def personal_telemarketing_daily_statistics_graph():
    # Даты отчёта (sd/ed/rp и пр.)
    sd, ed, msd, med, rp = parse_dates_telemarketing()

    # ДАННЫЕ ПРОДАЖ
    (
        general_sales_data,
        sales_gtm,
        total_aspu,
        sales_personal_gtm,
        sales_a,
        sales_conversion,
        sales_profit,
        sales_renewal,
        sales_payments,
        sales_profit_graphic,
        sales_renewal_graphic,
        sales_activation,
        sales_activation_graphic,
        sales_status,
        personal_activation_data,
        total_aspu_renewal_data,
        total_aspu_activation_data,
        bonus,
        bonus_graphic,
        renewal_status,
        first_price_plan,
        aspu,
        mistake,
        sales_transitions_data,
        sales_transitions_data_day,
    ) = sales_data_getter_op(start_date=sd, end_date=ed, report_date=rp)

    # ДАННЫЕ ЗВОНКОВ/СТАТУСОВ
    (
        general_calls_data,
        calls_gtm,
        calls_percent_gtm,
        calls_detail_gtm,
        sales_b,
        calls_rate,
        call_aht,
        coffee,
        dinner,
        dnd,
        occupancy,
        occupancy_graph,
        work_time,
        personal_aht,
        employee,
        request,
        income,
        income_calls,
        summary_request,
        calls_duration,
        project_spent_time,
        project_spent_time_user,
    ) = calls_data_getter_op(start_date=sd, end_date=ed)

    # ─── ВИЗУАЛИЗАЦИИ (персональные) ───────────────────────────────────────────
    tbl_sales_personal = render_personal_sales_table_op(sales_personal_gtm)
    img_sales_dynamic = render_personal_sales_dynamic_graph_op(sales_a)
    tbl_personal_activation = render_personal_activation_table_op(personal_activation_data)

    tbl_coffee = render_coffee_table_op(coffee)
    tbl_lunch = render_lunch_table_op(dinner)
    tbl_dnd = render_dnd_table_op(dnd)
    tbl_work_time = render_work_time_table_op(work_time)
    tbl_request_time = render_request_time_table_op(request)
    tbl_summary_request = render_summary_request_table_op(summary_request)

    tbl_occupancy = render_occupancy_table_op(occupancy)
    tbl_calls_duration = render_calls_duration_table_op(calls_duration)
    tbl_personal_aht = render_personal_aht_table_op(personal_aht)

    # ─── СБОРКА PDF ─────────────────────────────────────────────────────────────
    final_pdf_path = report_pdf_personal_gtm(
        tbl_sales_personal,
        img_sales_dynamic,
        tbl_personal_activation,
        tbl_coffee,
        tbl_lunch,
        tbl_dnd,
        tbl_work_time,
        tbl_request_time,
        tbl_summary_request,
        tbl_occupancy,
        tbl_calls_duration,
        tbl_personal_aht,
    )

    upload_report_instance_personal_gtm_op(final_pdf_path)