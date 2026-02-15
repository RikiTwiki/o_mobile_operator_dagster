from dagster import graph

from ops.telemarketing_daily_statistics.process_ops import get_connection_table_data_op, calls_data_getter_op, get_connections_ratio_op, get_all_data_op, utilization_op, sales_data_getter_op, parse_dates_telemarketing, upload_report_instance_gtm_op

from ops.telemarketing_daily_statistics.gtm_visualization_ops import render_general_performance_table_op, report_pdf_gtm, \
    render_calls_table_op, render_sales_status_chart_op, render_general_connections_ratio_chart_op, \
    render_day_profile_op, render_attempts_services_connections_table_op, render_sales_table_op, \
    render_call_aht_graphic_op, render_occupancy_graphic_op, render_project_spent_time_op


@graph()
def gtm_test_graph():
    sd, ed, msd, med, rp = parse_dates_telemarketing()

    utilization = utilization_op(start_date=sd, end_date=ed)

    (general_calls_data, calls_gtm, calls_percent_gtm, calls_detail_gtm,
     sales_b, calls_rate, call_aht,
     coffee, dinner, dnd,
     occupancy, occupancy_graph, work_time, personal_aht,
     employee, request, income, income_calls,
     summary_request, calls_duration, project_spent_time, project_spent_time_user) = calls_data_getter_op(start_date=sd, end_date=ed)

    general_performance_data = get_connection_table_data_op(start_date=sd, end_date=ed, start_date_month=msd, end_date_month=med)

    general_performance_data_table = render_general_performance_table_op(general_performance_data)

    general_calls_data_project_spent_time_table = render_sales_table_op(project_spent_time)

    general_calls_data_project_spent_time_user_table = render_project_spent_time_op(project_spent_time_user)

    general_calls_data_call_aht_chart = render_call_aht_graphic_op(general_performance_data)

    general_calls_data_occupancy_graph = render_occupancy_graphic_op(occupancy_graph)

    utilization_chart = render_occupancy_graphic_op(utilization)

    final_path = report_pdf_gtm(general_performance_data_table, general_calls_data_project_spent_time_table, general_calls_data_project_spent_time_user_table, general_calls_data_call_aht_chart, general_calls_data_occupancy_graph, utilization_chart)

    upload_report_instance_gtm_op(final_path)