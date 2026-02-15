from dagster import graph
from ops.general_daily_statistics.vizualisation_ops import table_ivr_transitions_op,  render_jira_issues_table_op

from ops.general_daily_statistics.process_ops import get_occupancy_data_dynamic_general_goo_op, \
    get_occupancy_data_month_op, get_goo_utilization_rate_date_op, get_gpo_utilization_rate_date_op, \
    get_goo_utilization_rate_date_month_general_op, get_gpo_utilization_rate_date_month_general_op, \
    get_aggregated_data_by_position_id_op, get_all_utilization_rate_date_op, get_utilization_rate_date_month_general_op, \
    get_all_occupancy_date, get_occupancy_data_month_general_op, get_cea_data_dynamic_by_groups_op, \
    get_graphic_aggregated_data_op, get_csi_data_by_project_name_op, get_csi_data_dynamic_op, \
    get_handling_times_data_dynamic_op, get_handling_times_data_dynamic_month_op, get_aht_general_average_data_op, \
    get_average_ringing_time_current_month_general_op, get_average_holding_time_current_month_general_op, \
    get_csi_data_month_main_op, csi_general_average_data_op, conversion_average_data_op, rc_data_dynamic_main_op, \
    rc_general_average_data_op, main_data_dynamic_general_op, main_data_dynamic_general_ivr_op, \
    self_service_rate_current_month_general_array_op, graphic_calls_dynamic_general_joined_op, max_ivr_total_op, \
    main_data_month_general_op, sl_general_average_data_op, acd_general_average_data_op, ssr_general_average_data_op, \
    main_data_dynamic_general_arpu_op, main_data_dynamic_general_language_op, cea_data_dynamic_general_op, \
    cea_current_month_general_op, closed_tasks_data_general_dynamic_op, closed_tasks_sla_month_op, \
    ivr_main_data_report_day_op, main_data_report_day_op, handling_times_data_report_day_op, \
    occupancy_data_report_day_op, csi_data_report_day_general_op, operators_count_plan_op, operators_count_factop, \
    fact_and_labor_operators_difference_goo_op, detailed_daily_data_goo_op, digital_self_service_rate_op, \
    month_saima_handled_chats_op, month_bank_handled_chats_op, month_agent_handled_chats_op, \
    mp_data_messengers_aggregated_op, hour_mp_data_messengers_detailed_op, mp_month_data_messengers_aggregated_saima_op, \
    month_average_reaction_time_op, month_average_speed_to_answer_op, cea_data_op, month_average_cea_data_op, \
    csi_data_op, csi_report_day_data_op, month_csi_data_aggregated_op, month_average_csi_data_op, \
    month_average_conversion_csi_data_op, detailed_daily_data_gpo_op, chat_data_op, technical_works_data_report_day_op, \
    request_data_op, day_bot_handled_chats_op, day_bot_projects_handled_chats_op, month_bot_handled_op, \
    ivr_transitions_op, jira_data_saima_op, jira_data_nur_op, sl_jira_data_op, nur_saima_aggregated_data_op, \
    upload_report_instance_general_op, digital_self_service_rate_month_op
from ops.general_daily_statistics.vizualisation_ops import main_table_constructor_op, \
    report_pdf_general, table_handled_time_gpo_op, table_handled_time_goo_op, table_technical_works_op, \
    omni_messenger_detailed_table_op, graphic_calls_dynamic_general_op, table_calls_dynamic_general_op, \
    table_requests_data_op, table_calls_sl_op, table_aht_op, table_calls_acd_op, table_digital_ssr_op, \
    table_csi_all_projects_op, table_cea_all_projects_op, table_occupancy_by_position_op, \
    table_utilization_rate_by_position_op, table_repeat_calls_op, graphic_sl_dynamic_general_op, \
    graphic_hcr_dynamic_general_op, graphic_aht_dynamic_general_op, graphic_ssr_dynamic_general_op, \
    graphic_bot_ssr_general_op, graphic_csi_op, graphic_cea_op, graphic_occupancy_op, graphic_urd_dynamic_general_op, \
    graphic_rc_dynamic_general_op, table_ivr_calls_dynamic_general_op, graphic_sl_by_segments_op, \
    graphic_sl_by_languages_op

from ops.general_daily_statistics.vizualisation_ops import render_gpo_table_op


@graph()
def general_daily_statistics_graph():
    occupancy_data_dynamic_general_goo = get_occupancy_data_dynamic_general_goo_op()
    occupancy_current_month_general = get_occupancy_data_month_op()
    goo_utilization_rate_date = get_goo_utilization_rate_date_op()
    gpo_utilization_rate_date = get_gpo_utilization_rate_date_op()
    goo_utilization_rate_date_month_general = get_goo_utilization_rate_date_month_general_op()
    gpo_utilization_rate_date_month_general = get_gpo_utilization_rate_date_month_general_op()

    aggregated_data_by_position_id = get_aggregated_data_by_position_id_op()
    all_utilization_rate_date = get_all_utilization_rate_date_op()
    utilization_rate_date_month_general = get_utilization_rate_date_month_general_op(all_utilization_rate_date)
    all_occupancy_date = get_all_occupancy_date()
    occupancy_data_month_general = get_occupancy_data_month_general_op(all_occupancy_date)
    cea_data_dynamic_by_groups = get_cea_data_dynamic_by_groups_op()
    graphic_aggregated_data = get_graphic_aggregated_data_op()
    csi_data_by_project_name = get_csi_data_by_project_name_op()
    csi_data_dynamic = get_csi_data_dynamic_op()
    handling_times_data_dynamic_general = get_handling_times_data_dynamic_op()
    handling_times_data_dynamic_month = get_handling_times_data_dynamic_month_op()
    aht_general_average_data = get_aht_general_average_data_op(handling_times_data_dynamic_month)
    average_ringing_time_current_month_general = get_average_ringing_time_current_month_general_op(handling_times_data_dynamic_month)
    average_holding_time_current_month_general = get_average_holding_time_current_month_general_op(handling_times_data_dynamic_month)
    csi_data_month_main = get_csi_data_month_main_op()
    csi_general_average_data = csi_general_average_data_op(csi_data_month_main)
    conversion_average_data = conversion_average_data_op(csi_data_month_main)
    rc_data_dynamic_main = rc_data_dynamic_main_op()
    rc_general_average_data = rc_general_average_data_op()

    main_data_dynamic_general = main_data_dynamic_general_op()
    main_data_dynamic_general_ivr = main_data_dynamic_general_ivr_op()
    self_service_rate_current_month_general_array = self_service_rate_current_month_general_array_op(main_data_dynamic_general_ivr)
    graphic_calls_dynamic_general_joined = graphic_calls_dynamic_general_joined_op(main_data_dynamic_general, main_data_dynamic_general_ivr)
    main_data_month_general = main_data_month_general_op()
    sl_general_average_data = sl_general_average_data_op(main_data_month_general)
    acd_general_average_data = acd_general_average_data_op(main_data_month_general)
    ssr_general_average_data = ssr_general_average_data_op()
    main_data_dynamic_general_arpu = main_data_dynamic_general_arpu_op()
    main_data_dynamic_general_language = main_data_dynamic_general_language_op()
    cea_data_dynamic_general = cea_data_dynamic_general_op()
    cea_current_month_general = cea_current_month_general_op()
    closed_tasks_data_general_dynamic = closed_tasks_data_general_dynamic_op()
    closed_tasks_sla_month = closed_tasks_sla_month_op()
    ivr_main_data_report_day_general = ivr_main_data_report_day_op()
    main_data_report_day_general = main_data_report_day_op()
    handling_times_data_report_day_general = handling_times_data_report_day_op()
    occupancy_data_report_day_general = occupancy_data_report_day_op()
    csi_data_report_day_general = csi_data_report_day_general_op()
    operators_count_plan = operators_count_plan_op()
    operators_count_fact = operators_count_factop()
    fact_and_labor_operators_difference_goo = fact_and_labor_operators_difference_goo_op(operators_count_fact, operators_count_plan)
    detailed_daily_data_goo = detailed_daily_data_goo_op(main_data_report_day_general, handling_times_data_report_day_general, occupancy_data_report_day_general,
                                                         csi_data_report_day_general, ivr_main_data_report_day_general, operators_count_plan, operators_count_fact,
                                                         fact_and_labor_operators_difference_goo)

    digital_self_service_rate = digital_self_service_rate_op()
    month_saima_handled_chats = month_saima_handled_chats_op()
    month_bank_handled_chats = month_bank_handled_chats_op()
    month_agent_handled_chats = month_agent_handled_chats_op(month_saima_handled_chats, month_bank_handled_chats)

    self_service_rate = digital_self_service_rate_month_op()

    mp_data_messengers_aggregated = mp_data_messengers_aggregated_op()
    hour_mp_data_messengers_detailed = hour_mp_data_messengers_detailed_op()
    mp_month_data_messengers_aggregated_saima = mp_month_data_messengers_aggregated_saima_op()
    month_average_reaction_time = month_average_reaction_time_op(mp_month_data_messengers_aggregated_saima)
    month_average_speed_to_answer = month_average_speed_to_answer_op(mp_month_data_messengers_aggregated_saima)
    cea_data = cea_data_op()
    month_average_cea_data = month_average_cea_data_op()
    csi_data = csi_data_op()
    csi_report_day_data = csi_report_day_data_op()
    month_csi_data_aggregated = month_csi_data_aggregated_op()
    month_average_csi_data = month_average_csi_data_op(month_csi_data_aggregated)
    month_average_conversion_csi_data = month_average_conversion_csi_data_op(month_csi_data_aggregated)
    detailed_daily_data_gpo = detailed_daily_data_gpo_op(hour_mp_data_messengers_detailed, csi_report_day_data)

    chat_data = chat_data_op()
    technical_works_data_report_day = technical_works_data_report_day_op()
    request_data = request_data_op()
    ivr_transitions = ivr_transitions_op()
    day_bot_handled_chats = day_bot_handled_chats_op()
    month_bot_handled_chats = month_bot_handled_op()
    day_bot_projects_handled_chats = day_bot_projects_handled_chats_op()
    jira_data_saima = jira_data_saima_op()
    jira_data_nur = jira_data_nur_op()
    nur_saima_aggregated_data = nur_saima_aggregated_data_op(saima_data=jira_data_saima, nur_data=jira_data_nur)


    # Срез по основным показателям. / Все проекты. ГОО
    main_table_constructor = main_table_constructor_op(main_data_dynamic_general, sl_general_average_data,
                                                       acd_general_average_data, main_data_dynamic_general_ivr,
                                                       self_service_rate_current_month_general_array,
                                                       goo_utilization_rate_date,
                                                       goo_utilization_rate_date_month_general,
                                                       occupancy_current_month_general, occupancy_data_dynamic_general_goo,
                                                       handling_times_data_dynamic_general,
                                                       aht_general_average_data,
                                                       average_ringing_time_current_month_general,
                                                       average_holding_time_current_month_general,
                                                       cea_data_dynamic_general, cea_current_month_general,
                                                       csi_data_dynamic,
                                                       csi_general_average_data, conversion_average_data,
                                                       rc_data_dynamic_main, rc_general_average_data,
                                                       closed_tasks_data_general_dynamic, closed_tasks_sla_month)

    # Все проекты. ГПО
    render_gpo_table = render_gpo_table_op(digital_self_service_rate,
                            self_service_rate,
                            mp_data_messengers_aggregated,
                            month_average_reaction_time,
                            month_average_speed_to_answer,
                            gpo_utilization_rate_date,
                            gpo_utilization_rate_date_month_general,
                            cea_data,
                            month_average_cea_data,
                            csi_data,
                            month_average_csi_data,
                            month_average_conversion_csi_data
                            )

    # Профиль дня. Все проекты. / ГОО
    table_handled_time_goo = table_handled_time_goo_op(detailed_daily_data_goo)

    # ГПО
    table_handled_time_gpo = table_handled_time_gpo_op(detailed_daily_data_gpo)

    # Таблица актуальных аварийно-плановых работ
    table_technical_works = table_technical_works_op(technical_works_data_report_day)

    # Сводка по проектам MP
    omni_messenger_detailed_table = omni_messenger_detailed_table_op(chat_data)

    # Все вызовы на проектные линии
    table_calls_dynamic_general = table_calls_dynamic_general_op(graphic_aggregated_data, graphic_calls_dynamic_general_joined)

    # ТОП 40 запросов OMNI
    table_requests_data = table_requests_data_op(request_data)

    # Сводка по переходам IVR. Линия - 707.
    table_ivr_transitions = table_ivr_transitions_op(ivr_transitions)

    # Статистика по основным показателям / Конверсия вызовов. Все проекты.
    graphic_calls_dynamic_general = graphic_calls_dynamic_general_op(graphic_calls_dynamic_general_joined)

    # Service Level. Все проекты.
    graphic_sl_dynamic_general = graphic_sl_dynamic_general_op(graphic_calls_dynamic_general_joined)

    # Service Level. Все проекты.
    table_calls_sl = table_calls_sl_op(items=graphic_aggregated_data, projects_data=graphic_calls_dynamic_general_joined)

    # Handler Calls Rate. Все проекты.
    graphic_hcr_dynamic_general = graphic_hcr_dynamic_general_op(graphic_calls_dynamic_general_joined)

    # Handler Calls Rate. Все проекты.
    table_calls_acd = table_calls_acd_op(items=graphic_aggregated_data, projects_data=graphic_calls_dynamic_general_joined)

    # Average Handling Time. Все проекты.
    graphic_aht_dynamic_general = graphic_aht_dynamic_general_op(handling_times_data_dynamic_general)

    # Average Handling Time. Все проекты.
    table_aht = table_aht_op(items=graphic_aggregated_data, projects_data=handling_times_data_dynamic_general)

    # IVR Self Service Rate. Все проекты
    graphic_ssr_dynamic_general = graphic_ssr_dynamic_general_op(graphic_calls_dynamic_general_joined)

    # IVR Self Service Rate. Все проекты
    table_ivr_calls_dynamic_general = table_ivr_calls_dynamic_general_op(graphic_aggregated_data, graphic_calls_dynamic_general_joined)

    # Digital Self Service Rate
    graphic_bot_ssr_general = graphic_bot_ssr_general_op(day_bot_handled_chats)

    # Digital Self Service Rate
    table_digital_ssr = table_digital_ssr_op(day_bot_projects_handled_chats, day_bot_handled_chats)

    # Customer Satisfaction Index.
    graphic_csi = graphic_csi_op(csi_data_dynamic)

    # Customer Satisfaction Index.
    table_csi_all_projects = table_csi_all_projects_op(csi_data_by_project_name)

    # Critical Error Accuracy
    graphic_cea = graphic_cea_op(cea_data_dynamic_general)

    # Critical Error Accuracy
    table_cea_all_projects = table_cea_all_projects_op(cea_data_dynamic_by_groups=cea_data_dynamic_by_groups, cea_data_dynamic_general= cea_data_dynamic_general)

    # Occupancy (Загруженность)
    graphic_occupancy = graphic_occupancy_op(all_occupancy_date)

    # Occupancy
    table_occupancy_by_position = table_occupancy_by_position_op(aggregated_data_by_position_id, all_occupancy_date)

    # Utilization Rate
    graphic_urd_dynamic_general = graphic_urd_dynamic_general_op(all_utilization_rate_date)

    # Utilization Rate
    table_utilization_rate_by_position = table_utilization_rate_by_position_op(aggregated_data_by_position_id, all_utilization_rate_date)

    # Repeat Calls
    graphic_rc_dynamic_general = graphic_rc_dynamic_general_op(rc_data_dynamic_main)

    # Repeat Calls
    table_repeat_calls = table_repeat_calls_op(graphic_aggregated_data, rc_data_dynamic_main)

    graphics_sl_dynamic_general = sl_jira_data_op()

    render_jira_issues_table = render_jira_issues_table_op(nur_saima_aggregated_data)

    graphic_sl_by_segments = graphic_sl_by_segments_op(main_data_dynamic_general_arpu)


    graphic_sl_by_languages = graphic_sl_by_languages_op(main_data_dynamic_general_language)


    final_path = report_pdf_general(main_table_constructor, # 1.1
                       render_gpo_table, # 1.2
                       table_handled_time_goo, # 2.1
                       table_handled_time_gpo, # 2.2
                       table_technical_works, # 3
                       table_requests_data, # 4
                       table_ivr_transitions, # 5
                       omni_messenger_detailed_table, # 5.1
                       graphic_calls_dynamic_general, # 6.1
                       table_calls_dynamic_general, # 6.1
                       sl_general_average_data,
                       graphic_sl_dynamic_general, # 6.2
                       table_calls_sl, # 6.2
                       acd_general_average_data,
                       graphic_hcr_dynamic_general, # 6.3
                       table_calls_acd,  # 6.3
                       aht_general_average_data,
                       graphic_aht_dynamic_general, # 6.4
                       table_aht, # 6.4
                       ssr_general_average_data,
                       graphic_ssr_dynamic_general,  #
                       table_ivr_calls_dynamic_general,  # 6.5
                       month_bot_handled_chats,
                       graphic_bot_ssr_general,  # 6.6
                       table_digital_ssr, # 6.6
                       csi_general_average_data,
                       graphic_csi, # 6.7
                       table_csi_all_projects, # 6.7
                       cea_current_month_general,
                       graphic_cea, # 6.8
                       table_cea_all_projects, # 6.8
                       occupancy_data_month_general,
                       graphic_occupancy, # 6.9
                       table_occupancy_by_position, # 6.9
                       utilization_rate_date_month_general,
                       graphic_urd_dynamic_general, # 6.10
                       table_utilization_rate_by_position, # 6.10
                       rc_general_average_data,
                       graphic_rc_dynamic_general, # 6.11
                       table_repeat_calls, # 6.11
                       graphics_sl_dynamic_general, # 7
                       render_jira_issues_table,# 8
                       graphic_sl_by_segments,
                       graphic_sl_by_languages
                    )

    upload_report_instance_general_op(final_path)