from graphs.telemarketing_daily_statistics.gtm_test_graph import gtm_test_graph
from resources import resources

telemarketing_daily_statistics_job = gtm_test_graph.to_job(
    name="telemarketing_daily_statistics_job",
    resource_defs={
        **resources,
    },
)