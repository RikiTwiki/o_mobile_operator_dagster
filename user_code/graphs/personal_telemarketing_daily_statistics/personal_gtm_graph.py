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
from ops.personal_telemarketing_daily_statistics.personal_gtm_visualization_ops import (
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
def personal_gtm_graph():
    # Даты отчёта (sd/ed/rp и пр.)
    sd, ed, msd, med, rp = parse_dates_telemarketing()

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

    tbl_coffee = render_coffee_table_op(coffee)
    tbl_lunch = render_lunch_table_op(dinner)
    tbl_dnd = render_dnd_table_op(dnd)
    tbl_work_time = render_work_time_table_op(work_time)
    tbl_request_time = render_request_time_table_op(request)
    tbl_summary_request = render_summary_request_table_op(summary_request)

    tbl_occupancy = render_occupancy_table_op(occupancy)

    # ─── СБОРКА PDF ─────────────────────────────────────────────────────────────
    final_pdf_path = report_pdf_personal_gtm(
        tbl_coffee,
        tbl_lunch,
        tbl_dnd,
        tbl_work_time,
        tbl_request_time,
        tbl_summary_request,
        tbl_occupancy,
    )

    upload_report_instance_personal_gtm_op(final_pdf_path)