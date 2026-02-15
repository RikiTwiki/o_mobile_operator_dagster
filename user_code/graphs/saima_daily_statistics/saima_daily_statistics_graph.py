from dagster import graph

from ops.chat_bot_statistics.process_ops import saima_daily_detailed_data_op
from ops.chat_bot_statistics.visualization_ops import graphic_sl_op
from ops.saima_daily_statistics.process_ops import jira_data_saima_ms_op, issues_ma_op, jira_data_saima_cs_op, \
    jira_data_saima_unique_ms_op, jira_data_saima_unique_ma_op, jira_data_saima_unique_cs_op, \
    repeat_calls_by_ivr_saima_op, average_repeat_ivr_saima_op, main_data_dynamic_saima_op, \
    main_data_month_general_saima_op, service_level_month_saima_op, handled_calls_month_saima_op, \
    self_service_level_month_saima_op, handling_times_data_saima_op, handling_times_month_data_saima_op, \
    average_handling_time_month_saima_op, average_ringing_time_month_saima_op, average_holding_time_month_saima_op, \
    cea_data_saima_op, cea_data_month_saima_op, csi_data_saima_op, csi_data_month_saima_op, csi_month_value_saima_op, \
    ost_rc_data_saima_op, ost_rc_data_month_saima_op, detailed_daily_data_saima_op, max_ivr_total_saima_op, \
    mp_data_messengers_aggregated_saima_op, mp_month_data_saima_op, month_average_reaction_time_saima_op, \
    month_average_speed_to_answer_saima_op, pst_csi_data_saima_op, pst_csi_data_month_saima_op, \
    pst_csi_current_month_value_saima_op, pst_csi_conversion_current_month_value_saima_op, \
    csi_conversion_month_value_saima_op, pst_cea_data_saima_op, pst_cea_data_month_saima_op, data_omni_saima_op, \
    pst_detailed_daily_data_saima_op, ivr_transitions_data_saima_op, upload_report_instance_saima_op
from ops.saima_daily_statistics.visualization_ops import jira_data_graph_saima_op, sl_saima_jira_cs_op, \
    sl_saima_jira_ms_ma_op, report_pdf_saima, render_jira_data_saima_ms_op, render_unique_ms_op, render_unique_ma_op, \
    voice_indicators_saima_op, text_indicators_op, voice_indicators_general_saima_op, handled_time_table_saima_op, \
    table_handle_time_saima_op, table_technical_works_saima_op, client_requests_saima_op, \
    render_ivr_transitions_data_saima_op, mp_chat_pivot_table_saima_op, graphic_calls_dynamic_saima_op, \
    graphic_sl_saima_op, graphic_acd_saima_op, graphic_ivr_ssr_saima_op, graphic_rc_saima_op, graphic_aht_saima_op


@graph()
def saima_daily_statistics_graph():

    saima_daily_detailed_data = saima_daily_detailed_data_op()

    main_data_dynamic_saima = main_data_dynamic_saima_op()
    main_data_month_general_saima = main_data_month_general_saima_op()
    service_level_month_saima = service_level_month_saima_op(main_data_month_general_saima)
    handled_calls_month_saima = handled_calls_month_saima_op(main_data_month_general_saima)
    self_service_level_month_saima = self_service_level_month_saima_op(main_data_month_general_saima)
    handling_times_data = handling_times_data_saima_op()
    handling_times_month_data = handling_times_month_data_saima_op()
    average_handling_time_month_saima = average_handling_time_month_saima_op(handling_times_month_data)
    average_ringin_time_month_saima = average_ringing_time_month_saima_op(handling_times_month_data)
    average_holding_time_month_saima = average_holding_time_month_saima_op(handling_times_month_data)
    cea_data_saima = cea_data_saima_op()
    cea_data_month_saima = cea_data_month_saima_op()
    csi_data_saima = csi_data_saima_op()
    csi_data_month_saima = csi_data_month_saima_op()
    csi_month_value_saima = csi_month_value_saima_op(csi_data_month_saima)
    csi_conversion_month_value_saima = csi_conversion_month_value_saima_op(csi_data_month_saima)
    ost_rc_data_saima = ost_rc_data_saima_op()
    ost_rc_data_month_saima = ost_rc_data_month_saima_op()
    detailed_daily_data_saima = detailed_daily_data_saima_op()
    max_ivr_total_saima = max_ivr_total_saima_op(main_data_dynamic_saima)
    mp_data_messengers_aggregated_saima = mp_data_messengers_aggregated_saima_op()
    mp_month_data = mp_month_data_saima_op()
    month_average_reaction_time_saima = month_average_reaction_time_saima_op(mp_month_data)
    month_average_speed_to_answer_saima = month_average_speed_to_answer_saima_op(mp_month_data)
    pst_cea_data_saima = pst_cea_data_saima_op()
    pst_cea_data_month_saima = pst_cea_data_month_saima_op()
    pst_csi_data_saima = pst_csi_data_saima_op()
    pst_csi_data_month_saima = pst_csi_data_month_saima_op()
    pst_csi_current_month_value_saima = pst_csi_current_month_value_saima_op(pst_csi_data_month_saima)
    pst_csi_conversion_current_month_value_saima = pst_csi_conversion_current_month_value_saima_op(pst_csi_data_month_saima)
    data_omni_saima = data_omni_saima_op()
    pst_detailed_daily_data = pst_detailed_daily_data_saima_op()
    ivr_transitions_data_saima = ivr_transitions_data_saima_op()

    saima_graphic = graphic_sl_op(saima_daily_detailed_data)

    # Заявки в Jira
    jira_data_graph_saima = jira_data_graph_saima_op()

    # Количество заявок и SLA по типам за последние 30 дней
    sl_saima_jira_cs = sl_saima_jira_cs_op()
    sl_saima_jira_ms_ma = sl_saima_jira_ms_ma_op()

    # ТОП 10 созданных основных заявок
    jira_data_saima_ms = jira_data_saima_ms_op()
    issues_ma = issues_ma_op()
    jira_data_saima_cs = jira_data_saima_cs_op()

    # Неотработанные заявки в Jira, созданные после 01.01.2025
    jira_data_saima_unique_ms = jira_data_saima_unique_ms_op()
    jira_data_saima_unique_ma = jira_data_saima_unique_ma_op()
    jira_data_saima_unique_cs = jira_data_saima_unique_cs_op()

    # repeat_calls
    repeat_calls_by_ivr_saima = repeat_calls_by_ivr_saima_op()
    average_repeat_ivr_saima = average_repeat_ivr_saima_op(repeat_calls_by_ivr_saima)


    render_jira_data_saima_ms = render_jira_data_saima_ms_op(jira_data_saima_ms)
    render_issues_ma = render_jira_data_saima_ms_op(issues_ma)
    render_jira_data_cs = render_jira_data_saima_ms_op(jira_data_saima_cs)

    render_unique_ms = render_unique_ms_op(jira_data_saima_unique_ms)
    render_unique_ma = render_unique_ma_op(jira_data_saima_unique_ma)
    render_unique_cs = render_unique_ms_op(jira_data_saima_unique_cs)

    # Срез по основным показателям. Голос.
    voice_indicators_saima = voice_indicators_saima_op(main_data_dynamic=main_data_dynamic_saima, service_level_month_general=service_level_month_saima,
                                                       handled_calls_month_general=handled_calls_month_saima, handling_times_data=handling_times_data,
                                                       average_handling_time_month_rounded_value=average_handling_time_month_saima,
                                                       average_ringing_time_month_rounded_value=average_ringin_time_month_saima,
                                                         average_holding_time_mont_rounded_value=average_holding_time_month_saima,
                                                       cea_data=cea_data_saima, cea_month_value=cea_data_month_saima, csi_data=csi_data_saima,
                                                       csi_month_value=csi_month_value_saima, csi_conversion_month_value=csi_conversion_month_value_saima,
                                                         ost_rc_data=ost_rc_data_saima, ost_rc_month_value=ost_rc_data_month_saima,
                                                        repeat_calls_by_ivr=repeat_calls_by_ivr_saima, average_repeat_ivr_pct=average_repeat_ivr_saima,
                                                       )

    text_indicators = text_indicators_op(mp_data_messengers_aggregated=mp_data_messengers_aggregated_saima, month_average_reaction_time=month_average_reaction_time_saima,
                                         month_average_speed_to_answer=month_average_speed_to_answer_saima, pst_cea_data=pst_cea_data_saima, pst_cea_month_value=pst_cea_data_month_saima,
                                         pst_csi_data=pst_csi_data_saima, pst_csi_month_value=pst_csi_current_month_value_saima,
                                         pst_csi_conversion_month_value=pst_csi_conversion_current_month_value_saima)

    voice_indicators_general_saima = voice_indicators_general_saima_op(data_omni_saima=data_omni_saima)

    handled_time_table_saima = handled_time_table_saima_op(detailed_daily_data_saima)
    table_handle_time_saima = table_handle_time_saima_op(pst_detailed_daily_data)
    table_technical_works_saima = table_technical_works_saima_op()
    client_requests_saima = client_requests_saima_op()
    render_ivr_transitions_data_saima = render_ivr_transitions_data_saima_op(ivr_transitions_data_saima)
    mp_chat_pivot_table_saima = mp_chat_pivot_table_saima_op()
    graphic_calls_dynamic_saima = graphic_calls_dynamic_saima_op(main_data_dynamic_saima)
    graphic_sl_saima = graphic_sl_saima_op(main_data_dynamic_saima)
    graphic_acd_saima = graphic_acd_saima_op(main_data_dynamic_saima)
    graphic_aht_saima = graphic_aht_saima_op(handling_times_data)
    graphic_ivr_ssr_saima = graphic_ivr_ssr_saima_op(main_data_dynamic_saima)
    graphic_rc_saima = graphic_rc_saima_op(ost_rc_data_saima)

    final_path = report_pdf_saima(saima_graphic, jira_data_graph_saima, sl_saima_jira_cs, sl_saima_jira_ms_ma,
                     render_jira_data_saima_ms, render_issues_ma, render_jira_data_cs,
                     render_unique_ms, render_unique_ma, render_unique_cs, voice_indicators_saima,
                     text_indicators, voice_indicators_general_saima,
                     handled_time_table_saima, table_handle_time_saima, table_technical_works_saima,
                     client_requests_saima, render_ivr_transitions_data_saima, mp_chat_pivot_table_saima,
                     graphic_calls_dynamic_saima, graphic_sl_saima, graphic_acd_saima, graphic_aht_saima, graphic_ivr_ssr_saima,
                     graphic_rc_saima, service_level_month_saima, handled_calls_month_saima, average_handling_time_month_saima,
                     self_service_level_month_saima, ost_rc_data_month_saima)

    upload_report_instance_saima_op(final_path)