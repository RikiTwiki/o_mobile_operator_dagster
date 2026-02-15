from graphs.csf_daily_statistics.csf_daily_statistics_graph import csf_daily_statistics_graph
from resources import resources

csf_daily_statistics_job = csf_daily_statistics_graph.to_job(
    name="csf_daily_statistics_job",
    resource_defs={
        **resources,
    },
)