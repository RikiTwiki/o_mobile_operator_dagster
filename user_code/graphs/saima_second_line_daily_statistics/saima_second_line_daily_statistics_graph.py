from dagster import graph

from ops.saima_second_line_daily_statistics.process_ops import (
    main_data_op,
    jira_handling_daily_data_op,
    omni_second_line_requests_daily_op,
    omni_second_line_requests_avg_op,
    jira_video_call_reasons_daily_op,
    jira_resolution_daily_op,
    jira_second_line_month_kpis_op, upload_report_instance_saima_second_line_op,
)

# visualization ops
from ops.saima_second_line_daily_statistics.visualization_ops import (
    render_jira_durations_graph_op,
    report_pdf_saima_second_line_op, indicators_table_second_line_op,
    requests_conversion_table_op, render_jira_graph_op, render_jira_line_graph_op, render_reasons_daily_pivot_op,
    render_jira_resolution_status_tables_op, render_jira_help_graph_op, render_jira_help_bar_graph_op,
)

from ops.saima_daily_statistics.process_ops import upload_report_instance_saima_op


@graph()
def saima_second_line_daily_statistics_graph():
    # --- DATA ---
    main_data = main_data_op()
    handling_daily = jira_handling_daily_data_op()
    omni_daily = omni_second_line_requests_daily_op()
    omni_avg = omni_second_line_requests_avg_op()

    reasons_daily = jira_video_call_reasons_daily_op()
    resolution_daily = jira_resolution_daily_op()

    month_kpis = jira_second_line_month_kpis_op()

    # --- TABLES ---
    kpi_table = indicators_table_second_line_op(
        main_data=main_data,
        handling_daily=handling_daily,
        month_kpis=month_kpis,
    )

    conversion_table = requests_conversion_table_op(
        data=omni_daily,
        jira_handling_daily_data=handling_daily,
    )

    render_jira_graph = render_jira_graph_op(items=main_data)

    render_jira_line_graph = render_jira_line_graph_op(items=main_data)

    render_reasons_daily_pivot = render_reasons_daily_pivot_op(reasons_daily_data=reasons_daily)

    render_jira_resolution_status_tables = render_jira_resolution_status_tables_op(resolution_daily)

    render_jira_help_graph = render_jira_help_graph_op(handling_daily)

    render_jira_help_bar_graph = render_jira_help_bar_graph_op(handling_daily)

    # --- PDF ---
    final_path = report_pdf_saima_second_line_op(
        kpi_table=kpi_table,
        conversion_table=conversion_table,
        render_jira_graph=render_jira_graph,
        render_jira_line_graph=render_jira_line_graph,
        render_reasons_daily_pivot=render_reasons_daily_pivot,
        render_jira_resolution_status_tables=render_jira_resolution_status_tables,
        render_jira_help_graph=render_jira_help_graph,
        render_jira_help_bar_graph=render_jira_help_bar_graph,
    )

    upload_report_instance_saima_second_line_op(final_path)