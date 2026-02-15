from dagster import define_asset_job

aggregate_mp_handling_time_job = define_asset_job(
    name="daily_mp_aggregation_job",
    selection=[
        "load_users",
        "load_staff_units",
        "get_last_aggregation_date",
        "load_raw_mp_data",
        "transform_mp_data",
        "aggregate_mp_handling_time_asset"
    ],
)