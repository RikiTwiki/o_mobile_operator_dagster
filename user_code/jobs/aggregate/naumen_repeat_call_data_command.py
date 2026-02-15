from dagster import job
from resources import resources
from dags.naumen_repeat_call_data import naumen_repeat_call_data

@job(resource_defs=resources, description="Репликация Naumen RC")
def naumen_repeat_call_data_command():
    naumen_repeat_call_data()