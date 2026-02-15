from dagster import graph

from ops.csf_daily_statistics.process_ops import parse_dates, month_statuses_data_op, \
    statuses_data_op, count_statuses_data_op, get_fullness_data, get_daily_detailed_data_op, get_position_ids, \
    get_views, get_projects_detail_data, get_groups, get_project_names, upload_report_instance_csf_op
from ops.csf_daily_statistics.process_ops import sl_data_joined_op, occupancy_op, operators_count_fact_op, \
    operators_count_fact_monthly_op


from ops.csf_daily_statistics.visualizations_ops import graphic_sl_dynamic_general, create_html_op, \
    graphic_calls_dynamic_general, daily_operators, monthly_operators, statuses_login_data_op, \
    graphic_occupancy_dynamic_general, graphic_productive_time_dynamic, month_statuses_main, month_statuses_secondary, \
    count_statuses_main, count_statuses_secondary, build_projects_tables, render_day_profile_new, graphic_fullness_data
from ops.text_projects_daily_statistics.process_ops import parse_dates
from ops.csf_daily_statistics.visualizations_ops import render_day_profile


@graph()
def csf_daily_statistics_graph():
    sd, ed, msd, med, rp = parse_dates()
    sl_data_joined = sl_data_joined_op(start_date=sd, end_date=ed)
    occupancy = occupancy_op(start_date=sd, end_date=ed)
    operators_count_fact = operators_count_fact_op(start_date=sd, end_date=ed, report_date=rp)
    operators_count_fact_monthly = operators_count_fact_monthly_op(start_date=sd, end_date=ed, report_date=rp)
    statuses_data = statuses_data_op(start_date=sd, end_date=ed)
    month_statuses_data = month_statuses_data_op(statuses_data)
    count_statuses_data = count_statuses_data_op(statuses_data)
    fullness_data = get_fullness_data(start_date=sd, report_date=rp)

    # 1.1
    csf_fig1 = graphic_sl_dynamic_general(data=sl_data_joined)

    # 1.2
    csf_fig2 = graphic_calls_dynamic_general(data=sl_data_joined)

    # 2.1
    csf_fig3 = graphic_occupancy_dynamic_general(data=occupancy)

    # 2.2
    csf_fig4 = graphic_productive_time_dynamic(data=occupancy)

    # 3.1
    csf_fig5 = daily_operators(data=operators_count_fact)

    # 3.2
    csf_fig6 = monthly_operators(data=operators_count_fact_monthly, start_date=sd, report_date=rp)

    # 4.1.1
    csf_fig7 = month_statuses_main(month_statuses_data=month_statuses_data, start_date=sd, report_date=rp)

    # 4.1.2
    csf_fig8 = month_statuses_secondary(month_statuses_data=month_statuses_data, start_date=sd, report_date=rp)

    # 4.2.1
    csf_fig9 = count_statuses_main(count_statuses_data=count_statuses_data, start_date=sd, report_date=rp)

    # 4.2.2
    csf_fig10 = count_statuses_secondary(count_statuses_data=count_statuses_data, start_date=sd, report_date=rp)

    general, saima, obank_money_bank, agent, terminals, akcha_bulak, entrepreneurs = get_groups()

    v8, v16, v9 = get_views()

    pos35, pos39, pos36 = get_position_ids()

    line_707, saima_project_name, obank, oagent, oterminals, akcha_bulak_project_name, okassa = get_project_names()

    nd_table = get_daily_detailed_data_op(start_date=sd, end_date=ed, report_date=rp, month_start=msd, month_end=med,
                                          group=general, views=v8, position_ids=pos35, project_name=line_707)
    sd_table = get_daily_detailed_data_op(start_date=sd, end_date=ed, report_date=rp, month_start=msd, month_end=med,
                                          group=saima, views=v16, position_ids=pos39, project_name=saima_project_name)
    md_table = get_daily_detailed_data_op(start_date=sd, end_date=ed, report_date=rp, month_start=msd, month_end=med,
                                          group=obank_money_bank, views=v9, position_ids=pos36, project_name=obank)
    ad_table = get_daily_detailed_data_op(start_date=sd, end_date=ed, report_date=rp, month_start=msd, month_end=med,
                                          group=agent, views=v9, position_ids=pos36, project_name=oagent)
    td_table = get_daily_detailed_data_op(start_date=sd, end_date=ed, report_date=rp, month_start=msd, month_end=med,
                                          group=terminals, views=v9, position_ids=pos36, project_name=oterminals)
    abd_table = get_daily_detailed_data_op(start_date=sd, end_date=ed, report_date=rp, month_start=msd, month_end=med,
                                           group=akcha_bulak, views=v9, position_ids=pos36,
                                           project_name=akcha_bulak_project_name)
    ed_table = get_daily_detailed_data_op(start_date=sd, end_date=ed, report_date=rp, month_start=msd, month_end=med,
                                          group=entrepreneurs, views=v8, position_ids=pos35, project_name=okassa)

    projects_data = get_projects_detail_data(start_date=sd, end_date=ed)
    csf_project_tables = build_projects_tables(modules_data=projects_data)

    csf_figs1 = statuses_login_data_op(data=statuses_data, start_date=sd, report_date=rp)

    csf_fig11 = render_day_profile_new(nd_table)
    csf_fig12 = render_day_profile_new(sd_table)
    csf_fig13 = render_day_profile_new(md_table)
    csf_fig14 = render_day_profile(ad_table)
    csf_fig15 = render_day_profile(td_table)
    csf_fig16 = render_day_profile_new(abd_table)
    csf_fig17 = render_day_profile(ed_table)

    csf_fullness_data = graphic_fullness_data(fullness_data)

    final_path = create_html_op(csf_fig1=csf_fig1, csf_fig2=csf_fig2, csf_fig3=csf_fig3, csf_fig4=csf_fig4,
                                csf_fig5=csf_fig5, csf_fig6=csf_fig6, csf_fig7=csf_fig7, csf_fig8=csf_fig8,
                                csf_fig9=csf_fig9, csf_fig10=csf_fig10, csf_fig11=csf_fig11, csf_fig12=csf_fig12,
                                csf_fig13=csf_fig13, csf_fig14=csf_fig14, csf_fig15=csf_fig15, csf_fig16=csf_fig16,
                                csf_fig17=csf_fig17, csf_tables=csf_project_tables, csf_tables2=csf_figs1, csf_fullness_data=csf_fullness_data)

    upload_report_instance_csf_op(final_path)