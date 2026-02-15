from graphs.saima_daily_statistics.saima_daily_statistics_graph import saima_daily_statistics_graph
from resources import resources


saima_daily_statistics_job = saima_daily_statistics_graph.to_job(
    name="saima_daily_statistics_job",
    resource_defs={
        **resources,
    },
)