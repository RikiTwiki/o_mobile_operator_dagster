from dagster import graph

from ops.text_projects_daily_statistics.process_ops import process_aggregation_project_data_op, get_service_rate_by_day, get_mp_statuses_op, get_service_rate_by_month, day_data_messengers_entered_chats_op

from ops.text_projects_daily_statistics.visualization_ops import render_aggregation_project_data_op, create_html, render_Hour_Jivo_Data_Aggregated, render_Mixed_Graph, \
render_jivo_total_31_days_detailed, render_jivo_total_31_days_users_detailed, render_total_31_days_users_detailed_with_shifts, \
    render_styled_reaction_time_graph, statuses_op,   render_jivo_avg_reaction_time_31_days_detailed, \
    render_jivo_avg_reaction_time_31_days_users_detailed, render_styled_speed_answer_time_graph, \
    render_jivo_avg_speed_to_answer_31_days_detailed, render_jivo_avg_speed_answer_time_31_days_users_detailed

from ops.text_projects_daily_statistics.process_ops import compute_statistic_dates_op, day_data_get_users_detailed_op, day_mp_data_get_users_detailed_by_view_op, get_month_avg_reaction_time, get_month_avg_speed_to_answer, upload_report_instance_op
from ops.text_projects_daily_statistics.process_ops import parse_dates, hour_mp_data_messenger_detailed_op, get_project_ids_op, load_month_self_service_rate, load_self_service_rate, day_data_messengers_detailed_op


@graph()
def text_projects_daily_statistics_graph(saima_target=100):
    sd, ed, msd, med, rp = parse_dates()

    all_project_ids, saima_project_ids, general_store_project_ids, other_project_ids, all_service_ids, bank_service_ids = get_project_ids_op(
        month_start=msd,
        month_end=med, )

    all_projects_data = process_aggregation_project_data_op(start_date=sd, end_date=ed, report_date=rp, month_start=msd, month_end=med, project_ids=all_project_ids)
    saima_projects_data = process_aggregation_project_data_op(start_date=sd, end_date=ed, report_date=rp, month_start=msd, month_end=med, project_ids=saima_project_ids)
    general_store_projects_data = process_aggregation_project_data_op(start_date=sd, end_date=ed, report_date=rp, month_start=msd, month_end=med, project_ids=general_store_project_ids)
    other_projects_data = process_aggregation_project_data_op(start_date=sd, end_date=ed, report_date=rp, month_start=msd, month_end=med, project_ids=other_project_ids)
    month_average_reaction_time = get_month_avg_reaction_time(all_projects_data)
    month_average_speed = get_month_avg_speed_to_answer(all_projects_data)

    all_self_service_rate = get_service_rate_by_day(start_date=sd, end_date=ed, project_ids=all_project_ids)
    saima_self_service_rate = get_service_rate_by_day(start_date=sd, end_date=ed, project_ids=saima_project_ids)

    general_and_eshop_self_service_rate = get_service_rate_by_day(start_date=sd, end_date=ed, project_ids=general_store_project_ids)

    bank_self_service_rate = get_service_rate_by_day(start_date=sd, end_date=ed, project_ids=bank_service_ids)

    month_all_self_service_rate = get_service_rate_by_month(start_date=sd, end_date=ed, project_ids=all_project_ids)
    month_saima_self_service_rate = get_service_rate_by_month(start_date=sd, end_date=ed, project_ids=saima_project_ids)

    month_general_and_eshop_self_service_rate = get_service_rate_by_month(start_date=sd, end_date=ed, project_ids=general_store_project_ids)

    month_bank_self_service_rate = get_service_rate_by_month(start_date=sd, end_date=ed, project_ids=bank_service_ids)

    hour_mp_data_messenger_detailed = hour_mp_data_messenger_detailed_op(start_date=sd, report_date=rp, end_date=ed, project_ids=all_project_ids)

    day_data_messengers_detailed = day_data_messengers_detailed_op(start_date=sd,  end_date=ed, project_ids=all_project_ids)

    day_data_get_users_detailed = day_data_get_users_detailed_op(start_date=sd, end_date=ed, project_ids=all_project_ids)

    day_mp_data_get_users_detailed_by_view = day_mp_data_get_users_detailed_by_view_op(start_date=sd, end_date=ed, project_ids=all_project_ids)
    statuses = get_mp_statuses_op(start_date=sd, end_date=ed, month_start=msd)

    text_fig1 = render_aggregation_project_data_op(all_projects_data, all_self_service_rate, month_all_self_service_rate)
    text_fig2 = render_aggregation_project_data_op(project_data=saima_projects_data, self_service_rate=saima_self_service_rate, month_self_service_rate=month_saima_self_service_rate, target=saima_target)
    text_fig3 = render_aggregation_project_data_op(general_store_projects_data, general_and_eshop_self_service_rate, month_general_and_eshop_self_service_rate)
    text_fig4 = render_aggregation_project_data_op(other_projects_data, bank_self_service_rate, month_bank_self_service_rate)

    text_fig5 = render_Hour_Jivo_Data_Aggregated(hour_mp_data_messenger_detailed)

    text_fig6 = render_Mixed_Graph(all_projects_data)
    text_fig7 = render_Mixed_Graph(saima_projects_data)
    text_fig8 = render_Mixed_Graph(general_store_projects_data)
    text_fig9 = render_Mixed_Graph(other_projects_data)

    text_fig10 = render_jivo_total_31_days_detailed(day_data_messengers_detailed)
    text_fig11 = render_jivo_total_31_days_users_detailed(day_data_get_users_detailed)
    text_fig12 = render_total_31_days_users_detailed_with_shifts(day_mp_data_get_users_detailed_by_view)

    text_fig13 = render_styled_reaction_time_graph(all_projects_data)
    text_fig14 = render_styled_reaction_time_graph(saima_projects_data, target=saima_target)
    text_fig15 = render_styled_reaction_time_graph(general_store_projects_data)
    text_fig16 = render_styled_reaction_time_graph(other_projects_data)

    text_fig17 = render_jivo_avg_reaction_time_31_days_detailed(day_data_messengers_detailed)
    text_fig18 = render_jivo_avg_reaction_time_31_days_users_detailed(day_data_get_users_detailed)

    text_fig19 = render_styled_speed_answer_time_graph(all_projects_data)
    text_fig20 = render_styled_speed_answer_time_graph(saima_projects_data, target=saima_target)
    text_fig21 = render_styled_speed_answer_time_graph(general_store_projects_data)
    text_fig22 = render_styled_speed_answer_time_graph(other_projects_data)

    text_fig23 = render_jivo_avg_speed_to_answer_31_days_detailed(day_data_messengers_detailed)
    text_fig24 = render_jivo_avg_speed_answer_time_31_days_users_detailed(day_data_get_users_detailed)

    text_figs = statuses_op(data=statuses)

    text_final_path = create_html(text_fig1, text_fig2, text_fig3, text_fig4, text_fig5, text_fig6, text_fig7, text_fig8, text_fig9, text_fig10, text_fig11, text_fig12, text_fig13, text_fig14, text_fig15, text_fig16, text_fig17, text_fig18, text_fig19, text_fig20, text_fig21, text_fig22, text_fig23, text_fig24, text_figs,
                month_average_reaction_time, month_average_speed)

    upload_report_instance_op(text_final_path)