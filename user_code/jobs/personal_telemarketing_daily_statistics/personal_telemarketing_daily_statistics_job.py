from graphs.personal_telemarketing_daily_statistics.personal_gtm_graph import (
    personal_gtm_graph,
)
from resources import resources

personal_telemarketing_daily_statistics_job = personal_gtm_graph.to_job(
    name="personal_telemarketing_daily_statistics_job",
    resource_defs={
        **resources,
    },
)