from dagster import graph

# Импорты процессинговых опов (Data Collection)
from ops.saima_daily_statistics.process_ops import (
    main_data_dynamic_saima_op,
    service_level_month_saima_op,
    handling_times_data_saima_op,
    handling_times_month_data_saima_op,
    average_handling_time_month_saima_op,
    csi_data_saima_op,
    csi_data_month_saima_op,
    csi_month_value_saima_op,
    ost_rc_data_saima_op,
    ost_rc_data_month_saima_op,
    repeat_calls_by_ivr_saima_op,
    average_repeat_ivr_saima_op,
    mp_data_messengers_aggregated_saima_op,
    mp_month_data_saima_op,
    month_average_reaction_time_saima_op,
    month_average_speed_to_answer_saima_op,
    pst_csi_data_saima_op,
    pst_csi_data_month_saima_op,
    pst_csi_current_month_value_saima_op,
    data_omni_saima_op,
    main_data_month_general_saima_op, upload_report_instance_saima_short_op
)

# Импорты опов визуализации
from ops.saima_daily_statistics.visualization_ops import (
    jira_data_graph_saima_op,
    sl_saima_jira_cs_op,
    graphic_calls_dynamic_saima_op,
    client_requests_saima_op,
    table_technical_works_saima_op
)
from ops.saima_daily_statistics.saima_daily_statistics_lite.visualization_lite_ops import (
    voice_indicators_saima_lite_op,
    text_indicators_lite_op,
    report_pdf_saima_lite,
    voice_indicators_general_saima_lite_op
)

from ops.saima_daily_statistics.process_ops import handled_calls_month_saima_op


@graph()
def saima_daily_statistics_lite_graph():
    # 1. Сбор базовых данных (Processing Ops)
    main_data_dynamic = main_data_dynamic_saima_op()
    main_data_month = main_data_month_general_saima_op()
    sl_month = service_level_month_saima_op(main_data_month)
    table_technical_works_saima = table_technical_works_saima_op()
    client_requests_saima = client_requests_saima_op()
    handled_calls_month_saima = handled_calls_month_saima_op(main_data_month_general_saima_op())
    handling_times_data = handling_times_data_saima_op()
    handling_times_month_data = handling_times_month_data_saima_op()
    average_handling_time_month_saima = average_handling_time_month_saima_op(handling_times_month_data)
    csi_d = csi_data_saima_op()
    csi_m_val = csi_month_value_saima_op(csi_data_month_saima_op())
    repeat_c_ivr = repeat_calls_by_ivr_saima_op()
    avg_repeat_ivr = average_repeat_ivr_saima_op(repeat_c_ivr)

    # Подготовка таблиц
    lite_voice = voice_indicators_saima_lite_op(
        main_data_dynamic=main_data_dynamic,
        service_level_month_general=sl_month,
        handled_calls_month_general=handled_calls_month_saima,
        handling_times_data=handling_times_data,
        average_handling_time_month_rounded_value=average_handling_time_month_saima,
        csi_data=csi_d,
        csi_month_value=csi_m_val,
        ost_rc_data=ost_rc_data_saima_op(),
        ost_rc_month_value=ost_rc_data_month_saima_op(),
        repeat_calls_by_ivr=repeat_c_ivr,
        average_repeat_ivr_pct=avg_repeat_ivr
    )

    lite_text = text_indicators_lite_op(
        mp_data_messengers_aggregated=mp_data_messengers_aggregated_saima_op(),
        month_average_reaction_time=month_average_reaction_time_saima_op(mp_month_data_saima_op()),
        month_average_speed_to_answer=month_average_speed_to_answer_saima_op(mp_month_data_saima_op()),
        pst_csi_data=pst_csi_data_saima_op(),
        pst_csi_month_value=pst_csi_current_month_value_saima_op(pst_csi_data_month_saima_op())
    )

    # Мы присваиваем результат переменной final_path
    final_path = report_pdf_saima_lite(
        fig1=jira_data_graph_saima_op(),
        table1=sl_saima_jira_cs_op(),
        fig2=lite_voice,
        fig3=lite_text,
        fig4=voice_indicators_general_saima_lite_op(data_omni_saima=data_omni_saima_op()),
        fig7=table_technical_works_saima,
        fig8=client_requests_saima,  # Используем переменную, чтобы не было дубля _op2
        graph=graphic_calls_dynamic_saima_op(main_data_dynamic),
        service_level_month=sl_month
    )

    upload_report_instance_saima_short_op(final_path)

    return final_path
