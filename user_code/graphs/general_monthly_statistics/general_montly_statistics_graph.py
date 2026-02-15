from dagster import graph

from ops.general_monthly_statistics.process_ops import main_data_dynamic_general_joined_monthly_op, \
    main_data_month_dynamic_general_op, main_data_dynamic_general_arpu_op, main_data_month_dynamic_general_ivr_op, \
    main_data_month_dynamic_general_joined_op, topics_data_op, main_index_op, price_plans_with_bad_marks_op, \
    handling_times_data_month_general_op, handling_times_data_month_general_arpu_op, rc_data_dynamic_main_op, \
    csi_data_month_main_op
from ops.general_monthly_statistics.visualizations_ops import render_main_data_dynamic_general_joined_monthly_op, \
    render_main_data_month_dynamic_general_joined_op, render_topics_data_op, render_general_group_data_op, \
    render_main_index_op, render_service_level_arpu_op, render_max_pickup_time_arpu_op, render_csi_data_month_main_op, \
    render_data_with_bad_marks_op, render_data_without_trunc8_op, render_data_without_trunc16_op, \
    render_data_without_trunc7_op, render_data_without_trunc9_op, render_data_without_trunc6_op, \
    render_data_without_trunc10_op, render_data_without_trunc14_op, render_price_plans_with_bad_marks_op, \
    report_pdf_monthly_general


@graph()
def general_monthly_statistics_graph():
    main_data_dynamic_general_joined_monthly = main_data_dynamic_general_joined_monthly_op()
    main_data_month_dynamic_general = main_data_month_dynamic_general_op()
    main_data_dynamic_general_arpu = main_data_dynamic_general_arpu_op()
    main_data_month_dynamic_general_ivr = main_data_month_dynamic_general_ivr_op()
    main_data_month_dynamic_general_joined = main_data_month_dynamic_general_joined_op(main_data_month_dynamic_general=main_data_month_dynamic_general, main_data_month_dynamic_general_ivr=main_data_month_dynamic_general_ivr)
    price_plans_with_bad_marks = price_plans_with_bad_marks_op()
    handling_times_data_month_general = handling_times_data_month_general_op()
    handling_times_data_month_general_arpu = handling_times_data_month_general_arpu_op()
    rc_data_dynamic_main = rc_data_dynamic_main_op()
    csi_data_month_main = csi_data_month_main_op()

    topics_data = topics_data_op()
    main_index = main_index_op(main_data_month_dynamic_general=main_data_month_dynamic_general, handling_times_data_month_general=handling_times_data_month_general, rc_data_dynamic_main=rc_data_dynamic_main,
                               csi_data_month_main=csi_data_month_main)


    render_main_data_dynamic_general_joined_monthly = render_main_data_dynamic_general_joined_monthly_op(main_data_dynamic_general_joined_monthly)
    render_main_data_month_dynamic_general_joined = render_main_data_month_dynamic_general_joined_op(main_data_month_dynamic_general_joined)
    render_topics_data = render_topics_data_op(topics_data)
    # render_general_group_data = render_general_group_data_op(topics_data)

    render_main_index = render_main_index_op(main_index)
    render_service_level_arpu = render_service_level_arpu_op(main_data_dynamic_general_arpu)
    render_max_pickup_time_arpu = render_max_pickup_time_arpu_op(handling_times_data_month_general_arpu)
    render_csi_data_month_main = render_csi_data_month_main_op(csi_data_month_main)
    render_data_with_bad_marks = render_data_with_bad_marks_op()
    render_data_without_trunc8 = render_data_without_trunc8_op()
    render_data_without_trunc16 = render_data_without_trunc16_op()
    render_data_without_trunc7 = render_data_without_trunc7_op()
    render_data_without_trunc9 = render_data_without_trunc9_op()
    render_data_without_trunc6 = render_data_without_trunc6_op()
    render_data_without_trunc10 = render_data_without_trunc10_op()
    render_data_without_trunc14 = render_data_without_trunc14_op()
    render_price_plans_with_bad_marks = render_price_plans_with_bad_marks_op(price_plans_with_bad_marks)

    report_pdf_monthly_general(render_main_data_dynamic_general_joined_monthly, render_main_data_month_dynamic_general_joined,
                               render_topics_data, render_main_index, render_service_level_arpu,
                               render_max_pickup_time_arpu, render_csi_data_month_main, render_data_with_bad_marks,
                               render_data_without_trunc8, render_data_without_trunc16, render_data_without_trunc7,
                               render_data_without_trunc9, render_data_without_trunc6, render_data_without_trunc10,
                               render_data_without_trunc14, render_price_plans_with_bad_marks)

