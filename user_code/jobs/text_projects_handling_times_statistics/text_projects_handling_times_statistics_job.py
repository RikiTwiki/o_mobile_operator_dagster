from dagster import fs_io_manager
from resources import resources

from graphs.text_projects_handling_times_statistics.text_projects_handling_times_statistics_graph import text_projects_handling_time_graph

text_projects_handling_times_statistics_job = text_projects_handling_time_graph.to_job(
    name="text_projects_handling_times_statistics_job",
    resource_defs={
        **resources,
    },

)