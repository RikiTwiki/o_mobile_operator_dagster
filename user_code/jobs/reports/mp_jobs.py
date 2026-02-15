from dagster import define_asset_job, job, graph

from assets.mp_report.opss import all_projects_data, load_self_service_rate, \
    load_month_self_service_rate, render_aggregation_project_data_op, save_report_locally, saima_aggregated_by_day, saima_entered_by_day, saima_handled_by_day, \
saima_bot_handled_by_day, saima_aggregated_by_month, saima_project_data, saima_service_rate_by_day_op, service_rate_saima_by_month_op, general_entered_by_day

from assets.mp_report.opss import bank_service_rate_by_day_op, general_aggregated_by_day, \
    general_bot_handled_by_day, general_handled_by_day, general_aggregated_by_month, general_project_data, \
    other_aggregated_by_day, other_entered_by_day, other_handled_by_day, other_bot_handled_by_day, \
    other_aggregated_by_month, other_project_data, service_rate_bank_by_month_op, render_Hour_Jivo_Data_Aggregated, render_Mixed_Graph, day_data_messengers_Detailed_op, render_Jivo_Total31_DaysDetailed, \
    day_MPData_Get_Users_Detailed_op, day_MPData_Get_Users_Detailed_By_View_op, \
    render_Jivo_Total31_Days_Users_Detailed, render_Total31_Days_Users_Detailed_With_Shifts, render_styled_reaction_time_graph, render_Jivo_avgreactiontime31_DaysDetailed, \
    render_Jivo_avgreactiontime31_Days_Users_Detailed, render_styled_speed_answer_time_graph, render_Jivo_speed_answer31_DaysDetailed, \
    render_Jivo_speed_answer31_Days_Users_Detailed, create_pdf_op, normal_shif_logins_op, normal_Shift_Statuses_op, cross_Day_Shift_Sheets_op, \
    cross_Day_Shift_statuses_op, all_users_op, statuses_op



mp_assets_jobs  = define_asset_job(
    name="mp_assets_jobs",
    selection=[
        "hourly_params",
        "daily_params",
        "monthly_params",
        "mp_data_messengers_detailed_by_hour",
        "mp_data_messengers_enteredchats_by_hour",
        "hour_MPData_Messengers_Detailed",
        "all_service_rate_by_day",
        "saima_service_rate_by_day",
        "bank_service_rate_by_day",
        "service_rate_all_by_month",
        "service_rate_saima_by_month",
        "service_rate_bank_by_month",
        "aggregated_by_day",
        "entered_by_day",
        "all_handled_by_day",
        "bot_handled_by_day",
        "aggregated_by_month"
    ],
)

# @graph
# def mp_graph():
#     saima_aggregated_byday = saima_aggregated_by_day()
#     saima_entered_byday = saima_entered_by_day()
#     saima_all_handled_byday = saima_handled_by_day()
#     saima_bot_handled_byday = saima_bot_handled_by_day()
#     saima_aggregated_bymonth = saima_aggregated_by_month()
#     saima_project_dataa = saima_project_data(saima_aggregated_byday, saima_entered_byday, saima_all_handled_byday, saima_bot_handled_byday, saima_aggregated_bymonth)
#
#     general_aggregated_byday = general_aggregated_by_day()
#     general_entered_byday = general_entered_by_day()
#     general_all_handled_byday = general_handled_by_day()
#     general_bot_handled_byday = general_bot_handled_by_day()
#     general_aggregated_bymonth = general_aggregated_by_month()
#     general_project_dataa = general_project_data(general_aggregated_byday, general_entered_byday, general_all_handled_byday, general_bot_handled_byday, general_aggregated_bymonth)
#
#     other_aggregated_byday = other_aggregated_by_day()
#     other_entered_byday = other_entered_by_day()
#     other_all_handled_byday = other_handled_by_day()
#     other_bot_handled_byday = other_bot_handled_by_day()
#     other_aggregated_bymonth = other_aggregated_by_month()
#     other_project_dataa = other_project_data(other_aggregated_byday, other_entered_byday, other_all_handled_byday, other_bot_handled_byday, other_aggregated_bymonth)
#
#     saima_self_rate = saima_service_rate_by_day_op()
#     saima_month_rate = service_rate_saima_by_month_op()
#     other_self_rate = bank_service_rate_by_day_op()
#     other_month_rate = service_rate_bank_by_month_op()
#
#     day_data_messengers_Detailed = day_data_messengers_Detailed_op()
#     day_MPData_Get_Users_Detailed = day_MPData_Get_Users_Detailed_op()
#     day_MPData_Get_Users_Detailed_By_View = day_MPData_Get_Users_Detailed_By_View_op()
#
#     normal_shif_logins = normal_shif_logins_op()
#     normal_Shift_Statuses = normal_Shift_Statuses_op(normal_shif_logins)
#     cross_Day_Shift_Sheets = cross_Day_Shift_Sheets_op()
#     cross_Day_Shift_statuses = cross_Day_Shift_statuses_op(cross_Day_Shift_Sheets)
#     all_users = all_users_op()
#     statuses = statuses_op(normal_Shift_Statuses = normal_Shift_Statuses,cross_Day_Shift_statuses = cross_Day_Shift_statuses,all_users = all_users)
#
#     project_data = all_projects_data()
#     self_rate = load_self_service_rate()
#     month_rate = load_month_self_service_rate()
#     fig = render_aggregation_project_data_op(
#         project_data=project_data,
#         self_service_rate=self_rate,
#         month_self_service_rate=month_rate,
#     )
#     fig2 = render_aggregation_project_data_op(
#         project_data=saima_project_dataa,
#         self_service_rate=saima_self_rate,
#         month_self_service_rate=saima_month_rate,
#     )
#     fig3 = render_aggregation_project_data_op(project_data = general_project_dataa)
#
#     fig4 = render_aggregation_project_data_op(project_data = other_project_dataa,
#                                               self_service_rate=other_self_rate,
#                                               month_self_service_rate=other_month_rate,
#     )
#
#     fig5 = render_Hour_Jivo_Data_Aggregated()
#     fig6 = render_Mixed_Graph(project_data=project_data)
#     fig7 = render_Mixed_Graph(project_data=saima_project_dataa)
#     fig8 = render_Mixed_Graph(project_data=general_project_dataa)
#     fig9 = render_Mixed_Graph(project_data=other_project_dataa)
#     fig10 = render_Jivo_Total31_DaysDetailed(data=day_data_messengers_Detailed)
#     fig11 = render_Jivo_Total31_Days_Users_Detailed(data=day_MPData_Get_Users_Detailed)
#     fig12 = render_Total31_Days_Users_Detailed_With_Shifts(data=day_MPData_Get_Users_Detailed_By_View)
#     fig13 = render_styled_reaction_time_graph(items=project_data)
#     # fig14 = render_styled_reaction_time_graph(items=saima_project_dataa)
#     # fig15 = render_styled_reaction_time_graph(items=general_project_dataa)
#     # fig16 = render_styled_reaction_time_graph(items=other_project_dataa)
#
#     fig17 = render_Jivo_avgreactiontime31_DaysDetailed(day_data_messengers_Detailed)
#     fig18 = render_Jivo_avgreactiontime31_Days_Users_Detailed(day_MPData_Get_Users_Detailed)
#
#     fig19 = render_styled_speed_answer_time_graph(items=project_data)
#
#     fig20 = render_Jivo_speed_answer31_DaysDetailed(day_data_messengers_Detailed)
#     fig21 = render_Jivo_speed_answer31_Days_Users_Detailed(day_MPData_Get_Users_Detailed)
#
#     create_pdf_op(fig, fig5, fig7, fig10, fig13)
#     # save_report_locally(fig)
#     # save_report_locally(fig2)
#     # save_report_locally(fig3)
#     # save_report_locally(fig4)
#     # save_report_locally(fig5)
#     # save_report_locally(fig6)
#     # save_report_locally(fig7)
#     # save_report_locally(fig8)
#     # save_report_locally(fig9)
#     # save_report_locally(fig10)
#     # save_report_locally(fig11)
#     # save_report_locally(fig12)
#     # save_report_locally(fig13)
#     # save_report_locally(fig17)
#     # save_report_locally(fig18)
#     # save_report_locally(fig19)
#     # save_report_locally(fig20)
#     # save_report_locally(fig21)
#
#
#
#
# ops_jobs = mp_graph.to_job(
#     name="ops_job")


