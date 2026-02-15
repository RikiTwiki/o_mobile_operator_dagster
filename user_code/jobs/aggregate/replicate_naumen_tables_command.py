from dagster import job
from resources import resources
from dags.replicate_naumen_tables import replicate_naumen_tables

from dagster import ScheduleDefinition, Definitions

@job(resource_defs=resources, description="Репликация таблиц из naumen_1/naumen_3 в CCDWH.naumen")
def replicate_call_legs_job():
    replicate_naumen_tables()  # один op

# каждые 3 минуты по Бишкеку
replicate_call_legs_schedule = ScheduleDefinition(
    job=replicate_call_legs_job,
    cron_schedule="0 */2 * * *",
    execution_timezone="Asia/Bishkek",
    run_config={
        "ops": {
            "replicate_naumen_tables": {
                "config": {
                    "batch_size": 20000,
                    "source_labels": {
                        "naumen_1": "Naumen",
                        "naumen_3": "Naumen3",
                    },
                }
            }
        }
    },
)