from graphs.saima_second_line_daily_statistics.saima_second_line_daily_statistics_graph import saima_second_line_daily_statistics_graph
from resources import resources


saima_second_line_daily_statistics_job = saima_second_line_daily_statistics_graph.to_job(
    name="saima_second_line_daily_statistics_job",
    resource_defs={
        **resources,
    },
)