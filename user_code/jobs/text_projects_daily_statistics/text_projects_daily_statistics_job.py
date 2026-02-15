from dagster import fs_io_manager
from resources import resources
from graphs.text_projects_daily_statistics.text_projects_daily_statistics_graph import text_projects_daily_statistics_graph

text_projects_daily_statistics_job = text_projects_daily_statistics_graph.to_job(
    name="text_projects_daily_statistics_job",
    resource_defs={
        **resources,
        "io_manager": fs_io_manager.configured({"base_dir": "/opt/dagster/mp"}),
    },

)