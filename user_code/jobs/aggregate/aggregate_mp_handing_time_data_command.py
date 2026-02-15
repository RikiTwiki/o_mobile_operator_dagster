from dagster import job
from resources import resources
from dags.aggregate_mp_handling_time_data import aggregate_mp_handling_time_data

@job(resource_defs=resources, description="Репликация Naumen RC")
def aggregate_mp_handing_time_data_command():
    aggregate_mp_handling_time_data()