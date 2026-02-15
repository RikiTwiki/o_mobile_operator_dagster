from graphs.chat_bot_statistics.chat_bot_statistics_graph import chat_bot_statistics_graph
from resources import resources

chat_bot_statistics_job = chat_bot_statistics_graph.to_job(
    name="chat_bot_statistics_job",
    resource_defs={
        **resources,
    },
)