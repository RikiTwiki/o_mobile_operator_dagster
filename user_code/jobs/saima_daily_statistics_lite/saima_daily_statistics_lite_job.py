from graphs.saima_daily_statistics_lite.saima_daily_statistics_lite_graph import saima_daily_statistics_lite_graph
from resources import resources


saima_daily_statistics_lite_job = saima_daily_statistics_lite_graph.to_job(
    name="saima_daily_statistics_lite_job",
    resource_defs={
        **resources,
    },
)