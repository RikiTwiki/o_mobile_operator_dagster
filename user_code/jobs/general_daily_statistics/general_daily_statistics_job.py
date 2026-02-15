from graphs.general_daily_statistics.general_daily_statistics_graph import general_daily_statistics_graph
from resources import resources

general_daily_statistics_job = general_daily_statistics_graph.to_job(
    name="general_daily_statistics_job",
    resource_defs={
        **resources,
    },
)