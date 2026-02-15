# definitions.py

from dagster import Definitions

from assets.aggregator import aggregate_mp_handling_time_asset
from assets.extractor.users import load_users, load_staff_units
from assets.extractor.mp_data import load_raw_mp_data
from assets.extractor.last_aggregation_date import get_last_aggregation_date
from assets.transformer import transform_mp_data
from assets.mp_report.assets import hourly_params, hour_MPData_Messengers_Detailed, daily_params, monthly_params, \
    service_rate_all_by_month, service_rate_saima_by_month, service_rate_bank_by_month, \
    mp_data_messengers_detailed_by_hour, mp_data_messengers_enteredchats_by_hour, saima_service_rate_by_day, \
    all_service_rate_by_day, bank_service_rate_by_day, aggregated_by_day, entered_by_day, all_handled_by_day, \
    bot_handled_by_day, aggregated_by_month

from jobs.aggregate import aggregate_mp_handling_time_job
from jobs.aggregate.naumen_handling_time_data_command import naumen_handling_time_data_command
from jobs.aggregate.aggregate_mp_handing_time_data_command import aggregate_mp_handing_time_data_command
# from jobs.aggregate.naumen_repeat_call_data_command import naumen_repeat_call_data_command
from jobs.bank_daily_statistics.bank_daily_statistics_job import bank_daily_statistics_job
from jobs.chat_bot_statistics.chat_bot_statistics_job import chat_bot_statistics_job
from jobs.csf_daily_statistics.csf_daily_statistics_job import csf_daily_statistics_job
from jobs.general_daily_statistics.general_daily_statistics_job import general_daily_statistics_job
from jobs.saima_daily_statistics.saima_daily_statistics_job import saima_daily_statistics_job
from jobs.saima_second_line_daily_statistics.saima_second_line_daily_statistics_job import \
    saima_second_line_daily_statistics_job
from jobs.text_projects_handling_times_statistics.text_projects_handling_times_statistics_job import text_projects_handling_times_statistics_job
from jobs.aggregate.mp_jobs import mp_assets_jobs
from jobs.reports.text_projects_report import report_job
from jobs.aggregate.naumen_service_level_data_command import naumen_service_level_data_command
from jobs.text_projects_daily_statistics.text_projects_daily_statistics_job import text_projects_daily_statistics_job
from jobs.aggregate.replicate_naumen_tables_command import replicate_call_legs_job, replicate_call_legs_schedule
from jobs.telemarketing_daily_statistics.telemarketing_daily_statistics_job import telemarketing_daily_statistics_job
from jobs.personal_telemarketing_daily_statistics.personal_telemarketing_daily_statistics_job import personal_telemarketing_daily_statistics_job
from schedules.aggregate.saima_daily_statistics_backfill_schedule import saima_daily_statistics_backfill
from schedules.aggregate.general_daily_statistics_backfill_schedule import general_daily_statistics_backfill
from schedules.aggregate.text_projects_daily_statistics_backfill_schedule import text_projects_daily_statistics_backfill
from schedules.aggregate.naumen_handling_time_data_schedule import naumen_handling_time_data_schedule
from schedules.aggregate.aggregate_mp_handling_time_data_schedule import aggregate_mp_handling_time_data_schedule
from schedules.bank_daily_statistics_schedule import bank_daily_statistics_schedule
from schedules.chat_bot_statistics_schedule import chat_bot_statistics_schedule
from schedules.csf_daily_statistics_schedule import csf_daily_statistics_schedule
from schedules.general_daily_statistics_schedule import general_daily_statistics_schedule
from schedules.personal_telemarketing_daily_statistics_schedule import personal_telemarketing_daily_statistics_schedule
from schedules.saima_daily_statistics_lite_schedule import saima_daily_statistics_lite_schedule
from schedules.saima_daily_statistics_schedule import saima_daily_statistics_schedule
from schedules.saima_second_line_daily_schedule import saima_second_line_daily_statistics_schedule
from schedules.telemarketing_daily_statistics_schedule import telemarketing_daily_statistics_schedule
from schedules.text_projects_daily_statistics_schedule import text_projects_daily_statistics_schedule
# from schedules.aggregate.naumen_repeat_call_data_schedule import naumen_repeat_call_data_schedule
from schedules.aggregate.naumen_service_level_data_schedule import naumen_service_level_data_schedule
from schedules.aggregate.bank_daily_statistics_backfill_schedule import (
    bank_daily_statistics_backfill,
)
from schedules.aggregate.chat_bot_statistics_backfill_schedule import chat_bot_statistics_backfill
from schedules.aggregate.csf_daily_statistics_backfill_schedule import csf_daily_statistics_backfill

from resources import resources
from schedules.text_projects_handling_times_statistics_schedule import text_projects_handling_times_statistics_schedule
from jobs.saima_daily_statistics_lite.saima_daily_statistics_lite_job import saima_daily_statistics_lite_job
defs = Definitions(
    jobs=[
        aggregate_mp_handling_time_job,
        report_job,
        mp_assets_jobs,
        csf_daily_statistics_job,
        chat_bot_statistics_job,
        general_daily_statistics_job,
        saima_daily_statistics_job,
        saima_second_line_daily_statistics_job,
        text_projects_daily_statistics_job,
        bank_daily_statistics_job,
        report_job,
        naumen_handling_time_data_command,
        aggregate_mp_handing_time_data_command,
        # naumen_repeat_call_data_command,
        naumen_service_level_data_command,
        replicate_call_legs_job,
        telemarketing_daily_statistics_job,
        personal_telemarketing_daily_statistics_job,
        text_projects_handling_times_statistics_job,
        saima_daily_statistics_lite_job
    ],
    resources=resources,
    assets=[
        load_users,
        load_staff_units,
        get_last_aggregation_date,
        load_raw_mp_data,
        transform_mp_data,
        aggregate_mp_handling_time_asset,
        hourly_params,
        daily_params,
        monthly_params,
        mp_data_messengers_detailed_by_hour,
        mp_data_messengers_enteredchats_by_hour,
        hour_MPData_Messengers_Detailed,
        all_service_rate_by_day,
        saima_service_rate_by_day,
        bank_service_rate_by_day,
        service_rate_all_by_month,
        service_rate_saima_by_month,
        service_rate_bank_by_month,
        aggregated_by_day,
        entered_by_day,
        all_handled_by_day,
        bot_handled_by_day,
        aggregated_by_month,
    ],
    schedules=[
        aggregate_mp_handling_time_data_schedule,
        naumen_handling_time_data_schedule,
        replicate_call_legs_schedule,
        # naumen_repeat_call_data_schedule,
        naumen_service_level_data_schedule,
        text_projects_daily_statistics_schedule,
        text_projects_daily_statistics_backfill,
        text_projects_handling_times_statistics_schedule,
        telemarketing_daily_statistics_schedule,
        bank_daily_statistics_schedule,
        bank_daily_statistics_backfill,
        chat_bot_statistics_schedule,
        chat_bot_statistics_backfill,
        csf_daily_statistics_schedule,
        csf_daily_statistics_backfill,
        general_daily_statistics_schedule,
        general_daily_statistics_backfill,
        personal_telemarketing_daily_statistics_schedule,
        saima_daily_statistics_schedule,

        saima_daily_statistics_lite_schedule,

        saima_second_line_daily_statistics_schedule,
        saima_daily_statistics_backfill,
    ],
)