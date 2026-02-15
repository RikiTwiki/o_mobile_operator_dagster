from dagster import graph

from ops.bank_daily_statistics.process_ops import main_data_dynamic_op, main_data_dynamic_filtered_op, \
    main_data_month_general_op1, service_level_current_month_general_op, handled_calls_current_month_general_op, \
    handling_times_data_op, handling_times_month_data_op, average_handling_time_current_month_rounded_value_op, \
    average_ringing_time_current_month_rounded_value_op, average_holding_time_current_month_rounded_value_op, \
    cea_data_op1, cea_current_month_value_op, csi_data_op1, csi_month_data_op, csi_current_month_value_op, \
    csi_conversion_current_month_value_op, ost_ur_data_op, ost_ur_month_data_rounded_op, ost_occupancy_data_op, \
    ost_occupancy_month_data_rounded_op, ost_rc_data_op, ost_rc_current_month_value_op, detailed_daily_data_op, \
    merged_main_data_month_general_op, sl_current_month_joined_op, hcr_current_month_joined_op, \
    ssr_current_month_joined_op, merged_aht_op, aht_by_project_op, merged_aht_month_rounded_op, max_ivr_total_op1, \
    mp_data_messengers_aggregated_op1, mp_month_data_op, month_average_reaction_time_op1, \
    month_average_speed_to_answer_op1, pst_ur_data_op, pst_ur_month_data_rounded_op, pst_cea_data_op, \
    pst_cea_current_month_value_op, pst_csi_data_op, pst_csi_month_data_op, pst_csi_current_month_value_op, \
    pst_csi_conversion_current_month_value_op, pst_detailed_daily_data_op, merged_ost_rc_data_op, \
    merged_ost_rc_data_filtered_op, merged_ost_rc_current_month_data_op, ivr_transitions_bank_op, \
    upload_report_instance_bank_op
from ops.bank_daily_statistics.visualization_ops import voice_indicators_title_op, bank_daily_statistics_report, \
    pst_text_indicators_op, table_handled_time_op, handled_time_table_op, table_technical_works_op1, client_requests_op, \
    mp_chat_pivot_table_op, graphic_calls_dynamic_general_op1, table_calls_dynamic_general_op1, graphic_sl_bank_op, \
    table_sl_bank_op, graphic_acd_bank_op, table_acd_bank_op, graphic_aht_bank_op, table_aht_bank_op, \
    graphic_ivr_ssr_bank_op, table_ivr_ssr_bank_op, graphic_rc_bank_op, table_rc_bank_op, render_ivr_transitions_op, \
    jira_data_graph_bank_op


@graph()
def bank_daily_statistics_graph():
    main_data_dynamic = main_data_dynamic_op()
    main_data_dynamic_filtered = main_data_dynamic_filtered_op()
    main_data_month_general = main_data_month_general_op1()
    service_level_current_month_general = service_level_current_month_general_op(main_data_month_general)
    handled_calls_current_month_general = handled_calls_current_month_general_op(main_data_month_general)
    handling_times_data = handling_times_data_op()
    handling_times_month_data = handling_times_month_data_op()
    average_handling_time_current_month_rounded_value = average_handling_time_current_month_rounded_value_op(handling_times_month_data)
    average_ringing_time_current_month_rounded_value = average_ringing_time_current_month_rounded_value_op(handling_times_month_data)
    average_holding_time_current_month_rounded_value = average_holding_time_current_month_rounded_value_op(handling_times_month_data)

    cea_data = cea_data_op1()
    cea_current_month_value = cea_current_month_value_op()
    csi_data = csi_data_op1()
    csi_month_data = csi_month_data_op()
    csi_current_month_value = csi_current_month_value_op(csi_month_data)
    csi_conversion_current_month_value = csi_conversion_current_month_value_op(csi_month_data)

    ost_ur_data = ost_ur_data_op()
    ost_ur_month_data_rounded = ost_ur_month_data_rounded_op()
    ost_occupancy_data = ost_occupancy_data_op()
    ost_occupancy_month_data_rounded = ost_occupancy_month_data_rounded_op()
    ost_rc_data = ost_rc_data_op()
    ost_rc_current_month_value = ost_rc_current_month_value_op()
    detailed_daily_data = detailed_daily_data_op()

    merged_main_data_month_general = merged_main_data_month_general_op()
    sl_current_month_joined = sl_current_month_joined_op(merged_main_data_month_general)
    hcr_current_month_joined = hcr_current_month_joined_op(merged_main_data_month_general)
    ssr_current_month_joined = ssr_current_month_joined_op(merged_main_data_month_general)

    merged_aht = merged_aht_op()
    aht_by_project = aht_by_project_op()

    merged_aht_month_rounded = merged_aht_month_rounded_op()

    merged_ost_rc_data = merged_ost_rc_data_op()
    merged_ost_rc_data_filtered = merged_ost_rc_data_filtered_op()
    merged_ost_rc_current_month_data = merged_ost_rc_current_month_data_op()

    max_ivr_total = max_ivr_total_op1(main_data_dynamic)
    mp_data_messengers_aggregated = mp_data_messengers_aggregated_op1()
    mp_month_data = mp_month_data_op()
    month_average_reaction_time = month_average_reaction_time_op1(mp_month_data)
    month_average_speed_to_answer = month_average_speed_to_answer_op1(mp_month_data)

    pst_ur_data = pst_ur_data_op()
    pst_ur_month_data_rounded = pst_ur_month_data_rounded_op()
    pst_cea_data = pst_cea_data_op()
    pst_cea_current_month_value = pst_cea_current_month_value_op()
    pst_csi_data = pst_csi_data_op()
    pst_csi_month_data = pst_csi_month_data_op()
    pst_csi_current_month_value = pst_csi_current_month_value_op(pst_csi_month_data)
    pst_csi_conversion_current_month_value = pst_csi_conversion_current_month_value_op(pst_csi_month_data)
    pst_detailed_daily_data = pst_detailed_daily_data_op()
    ivr_transitions = ivr_transitions_bank_op()



    # Срез по основным показателям. / Голос
    voice_indicators_title = voice_indicators_title_op(main_data_dynamic, service_level_current_month_general, handled_calls_current_month_general,
                           handling_times_data, average_handling_time_current_month_rounded_value, average_ringing_time_current_month_rounded_value, average_holding_time_current_month_rounded_value,
                           cea_data, cea_current_month_value, csi_data, csi_current_month_value, csi_conversion_current_month_value,
                           ost_ur_data, ost_ur_month_data_rounded, ost_occupancy_data, ost_occupancy_month_data_rounded, ost_rc_data,
                           ost_rc_current_month_value)

    # Текст
    pst_text_indicators = pst_text_indicators_op(
    mp_data_messengers_aggregated=mp_data_messengers_aggregated,
    month_average_reaction_time=month_average_reaction_time,
    month_average_speed_to_answer=month_average_speed_to_answer,
    pst_ur_data=pst_ur_data,
    pst_ur_month_data_rounded=pst_ur_month_data_rounded,
    pst_cea_data=pst_cea_data,
    pst_cea_current_month_value=pst_cea_current_month_value,
    pst_csi_data=pst_csi_data,
    pst_csi_current_month_value=pst_csi_current_month_value,
    pst_csi_conversion_current_month_value=pst_csi_conversion_current_month_value)

    # Профиль дня / Голос
    handled_time_table = handled_time_table_op(detailed_daily_data)

    # Текст
    table_handled_time = table_handled_time_op(pst_detailed_daily_data)

    # Таблица актуальных аварийно-плановых работ
    table_technical_works = table_technical_works_op1()

    # ТОП40 причин обращений
    client_requests = client_requests_op()

    # Сводка по переходам IVR.
    render_ivr_transitions = render_ivr_transitions_op(ivr_transitions)

    # Сводка по проектам MP
    mp_chat_pivot_table = mp_chat_pivot_table_op()

    # Статистика по основным показателям / Общая динамика звонков за 31 день
    graphic_calls_dynamic_general = graphic_calls_dynamic_general_op1(items=main_data_dynamic)

    table_calls_dynamic_general = table_calls_dynamic_general_op1(main_data_dynamic_filtered=main_data_dynamic_filtered)

    # Service Level
    graphic_sl_bank = graphic_sl_bank_op(main_data_dynamic)

    # Service Level
    table_sl_bank = table_sl_bank_op(main_data_dynamic_filtered=main_data_dynamic_filtered, main_data_dynamic=main_data_dynamic)

    # Handler Calls Rate
    graphic_acd_bank = graphic_acd_bank_op(main_data_dynamic)

    # Handler Calls Rate
    table_acd_bank = table_acd_bank_op(main_data_dynamic_filtered, main_data_dynamic)

    # Average Handling Time
    graphic_aht_bank = graphic_aht_bank_op(merged_aht)

    # Average Handling Time
    table_aht_bank = table_aht_bank_op(aht_by_project, merged_aht)

    # IVR Self Service Rate
    graphic_ivr_ssr_bank = graphic_ivr_ssr_bank_op(main_data_dynamic)

    # IVR Self Service Rate
    table_ivr_ssr_bank = table_ivr_ssr_bank_op(main_data_dynamic_filtered, main_data_dynamic)

    # Repeat Calls. Все проекты
    graphic_rc_bank = graphic_rc_bank_op(merged_ost_rc_data)

    # Repeat Calls. Все проекты
    table_rc_bank = table_rc_bank_op(merged_ost_rc_data_filtered, merged_ost_rc_data)

    # Общая динамика SLA по закрытым задачам
    jira_data_graph =jira_data_graph_bank_op()

    final_path = bank_daily_statistics_report(voice_indicators_title, # 1.1
                                 pst_text_indicators, # 1.2
                                 handled_time_table, # 2.1
                                 table_handled_time, # 2.2
                                 table_technical_works, # 3
                                 client_requests, # 4
                                 render_ivr_transitions, # 5
                                 mp_chat_pivot_table, # 6
                                 graphic_calls_dynamic_general, # 7.1
                                 table_calls_dynamic_general,
                                 graphic_sl_bank, # 7.2
                                 table_sl_bank,
                                 graphic_acd_bank, # 7.3
                                 table_acd_bank,
                                 graphic_aht_bank, # 7.4
                                 table_aht_bank,
                                 graphic_ivr_ssr_bank, # 7.5
                                 table_ivr_ssr_bank,
                                 graphic_rc_bank, # 7.6
                                 table_rc_bank,
                                 jira_data_graph,
                                 )

    upload_report_instance_bank_op(final_path)