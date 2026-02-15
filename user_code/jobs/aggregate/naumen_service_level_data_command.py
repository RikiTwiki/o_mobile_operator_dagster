from dagster import job
from resources import resources
from dags.naumen_service_level_data import naumen_service_level_data

@job(resource_defs=resources, description="Репликация Naumen SLD")
def naumen_service_level_data_command():
    naumen_service_level_data()