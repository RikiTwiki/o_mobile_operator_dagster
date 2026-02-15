SELECT TO_CHAR(chat_finished_at, 'YYYY-MM-DD') as formatted_date
FROM stat.aggregation_mp_handling_time_data
ORDER BY chat_finished_at DESC
LIMIT 1