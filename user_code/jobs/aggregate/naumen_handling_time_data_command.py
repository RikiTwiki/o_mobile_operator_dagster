from dagster import job
from resources import resources
from dags.naumen_handling_time_data import naumen_handling_time_data

@job(resource_defs=resources, description="Репликация Naumen HT")
def naumen_handling_time_data_command():
    naumen_handling_time_data()