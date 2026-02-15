from graphs.bank_daily_statistics.bank_daily_statistics_graph import bank_daily_statistics_graph
from resources import resources

bank_daily_statistics_job = bank_daily_statistics_graph.to_job(
    name="bank_daily_statistics_job",
    resource_defs={
        **resources,
    },
)