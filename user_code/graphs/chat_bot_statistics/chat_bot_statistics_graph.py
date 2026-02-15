from dagster import graph

from ops.chat_bot_statistics.process_ops import (
    has_note_daily_control_data_op,
    has_error_nur_daily_control_data_op,
    has_error_saima_daily_control_data_op,
    has_error_bank_daily_control_data_op,
    daily_detailed_data_op,
    saima_daily_detailed_data_op,
    bank_daily_detailed_data_op,
    nur_daily_detailed_data_op,
    daily_bot_percentage_op,
    daily_data_bot_percentage_by_project_op,
    bea_daily_detailed_data_op,
    nur_daily_detailed_data_cea_op,
    bank_daily_detailed_data_cea_op,
    saima_daily_detailed_data_cea_op,
    cost_tokens_op,
    cost_usd_op,
    cost_usd_by_token_op,
    dialogs_count_by_projects_op,
    replies_count_by_projects_op,
    request_data_saima_op,
    request_data_obank_op,
    request_data_nur_op, akchabulak_daily_detailed_data_op, obrand_daily_detailed_data_op,
    upload_report_instance_chat_bot_op,
)

from ops.chat_bot_statistics.visualization_ops import (
    graphic_sl_op,
    report_pdf,
    render_single_line_graph_op,
    render_basic_table_op,
    render_basic_table_without_error,
    render_total_31_days_detailed_op,
    render_cea_mix_graph_op,
    multilines_graph,
    request_table_op,
    render_cost_tokens_line_graph_nur_op,
    render_tokens_line_graph_nur_op,
    render_cost_tokens_line_graph_bank_op,
    render_tokens_line_graph_bank_op,
    render_cost_tokens_line_graph_saima_op,
    render_tokens_line_graph_saima_op,
    multilines_graph2,
    multilines_graph3,
)


@graph()
def chat_bot_statistics_graph():
    has_note_daily_control_data = has_note_daily_control_data_op()
    has_error_nur_daily_control_data = has_error_nur_daily_control_data_op()
    has_error_saima_daily_control_data = has_error_saima_daily_control_data_op()
    has_error_bank_daily_control_data = has_error_bank_daily_control_data_op()

    daily_detailed_data = daily_detailed_data_op()
    saima_daily_detailed_data = saima_daily_detailed_data_op()
    bank_daily_detailed_data = bank_daily_detailed_data_op()
    akchabulak_daily_detailed_data = akchabulak_daily_detailed_data_op()
    nur_daily_detailed_data = nur_daily_detailed_data_op()
    obrand_daily_detailed_data = obrand_daily_detailed_data_op()

    daily_bot_percentage = daily_bot_percentage_op()
    daily_data_bot_percentage_by_project = daily_data_bot_percentage_by_project_op()
    bea_daily_detailed_data = bea_daily_detailed_data_op()

    nur_daily_detailed_data_cea = nur_daily_detailed_data_cea_op()
    bank_daily_detailed_data_cea = bank_daily_detailed_data_cea_op()
    saima_daily_detailed_data_cea = saima_daily_detailed_data_cea_op()

    cost_tokens = cost_tokens_op()
    cost_usd = cost_usd_op()
    cost_usd_by_token = cost_usd_by_token_op()
    request_data_saima = request_data_saima_op()
    request_data_obank = request_data_obank_op()
    request_data_nur = request_data_nur_op()

    dialogs_count_by_projects = dialogs_count_by_projects_op(cost_usd)
    replies_count_by_projects = replies_count_by_projects_op(cost_usd)


    # --- графики ---
    # 1.1 Статистика по обработанным чатам / Все проекты
    chat_fig1 = graphic_sl_op(daily_detailed_data)

    # 1.2 Процент автоматизации
    chat_fig2 = render_single_line_graph_op(daily_bot_percentage)
    chat_fig3 = render_total_31_days_detailed_op(daily_data_bot_percentage_by_project, daily_bot_percentage)

    # 1.3 Saima
    chat_fig4 = graphic_sl_op(saima_daily_detailed_data)

    # 1.4 O!Bank + О!Деньги
    chat_fig5 = graphic_sl_op(bank_daily_detailed_data)

    # 1.5 AkchaBulak
    chat_fig6 = graphic_sl_op(akchabulak_daily_detailed_data)

    # 1.6 Nur
    chat_fig7 = graphic_sl_op(nur_daily_detailed_data)

    #1.7 O!Brand
    chat_fig8 = graphic_sl_op(obrand_daily_detailed_data)

    # 2.1.1 BEA / Динамика оцененных чатов за 31 день / Все проекты
    chat_fig9 = render_cea_mix_graph_op(bea_daily_detailed_data)

    # 2.1.2 Nur
    chat_fig10 = render_cea_mix_graph_op(nur_daily_detailed_data_cea)

    # 2.1.3 O!Bank
    chat_fig11 = render_cea_mix_graph_op(bank_daily_detailed_data_cea)

    # 2.1.4 Saima
    chat_fig12 = render_cea_mix_graph_op(saima_daily_detailed_data_cea)

    # 2.2.1 Запросы и причины чатов с ошибками / Nur
    chat_fig13 = render_basic_table_op(has_error_nur_daily_control_data)

    # 2.2.2 Bank
    chat_fig14 = render_basic_table_op(has_error_saima_daily_control_data)

    # 2.2.3 Saima
    chat_fig15 = render_basic_table_op(has_error_bank_daily_control_data)

    # 2.3 Чаты с замечаниями
    chat_fig16 = render_basic_table_without_error(has_note_daily_control_data)

    # 3.1 COST / Расходы по всем проектам (USD)
    chat_fig17 = multilines_graph(cost_usd)

    # 3.2 Стоимость затрат за один диалог закрытый чат-ботом (центы)
    chat_fig18 = multilines_graph2(dialogs_count_by_projects)

    # 3.3 Стоимость затрат за один ответ чат-бота (центы)
    chat_fig19 = multilines_graph3(replies_count_by_projects)

    # 3.4.1 Nur / Расходы USD
    chat_fig20 = render_cost_tokens_line_graph_nur_op(cost_usd_by_token=cost_usd_by_token)

    # 3.4.2 Расход токенов (млн)
    chat_fig21 = render_tokens_line_graph_nur_op(cost_tokens=cost_tokens)

    # 3.5.1 Bank / Расходы USD
    chat_fig22 = render_cost_tokens_line_graph_bank_op(cost_usd_by_token=cost_usd_by_token)

    # 3.5.2 Расход токенов (млн)
    chat_fig23 = render_tokens_line_graph_bank_op(cost_tokens=cost_tokens)

    # 3.6.1 Saima / Расходы USD
    chat_fig24 = render_cost_tokens_line_graph_saima_op(cost_usd_by_token=cost_usd_by_token)

    # 3.6.2 Расход токенов (млн)
    chat_fig25 = render_tokens_line_graph_saima_op(cost_tokens=cost_tokens)

    # 4.1 ТОП40 запросов OMNI в чатах, закрытых специалистами / Saima
    chat_fig26 = request_table_op(request_data_saima)

    # 4.2 O!Bank + О!Деньги
    chat_fig27 = request_table_op(request_data_obank)

    # 4.3 Nur
    chat_fig28 = request_table_op(request_data_nur)


    # --- формирование отчетов ---
    final_path = report_pdf(chat_fig1, chat_fig2, chat_fig3, chat_fig4, chat_fig5,
               chat_fig6, chat_fig7, chat_fig8, chat_fig9, chat_fig10, chat_fig11,
               chat_fig12, chat_fig13, chat_fig14, chat_fig15, chat_fig16,
               chat_fig17, chat_fig18, chat_fig19, chat_fig20, chat_fig21,
               chat_fig22, chat_fig23, chat_fig24, chat_fig25, chat_fig26, chat_fig27, chat_fig28,
               )

    upload_report_instance_chat_bot_op(final_path)