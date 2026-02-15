SELECT
to_char(
date_trunc('{trunc}', amphtd.chat_finished_at),
'{date_format}'
) AS "{date_field}",
count(DISTINCT amphtd.chat_id) AS bot_handled_chats,
SUM(CASE WHEN amphtd.is_bot_only THEN 1
ELSE 0 END) AS bot_handled_chats
FROM stat.aggregation_mp_handling_time_data amphtd
WHERE amphtd.chat_finished_at >= %(start_date)s
AND amphtd.chat_finished_at <  %(end_date)s
AND amphtd.user_id = 129
AND amphtd.is_bot_only = true
AND amphtd.project_id = ANY(%(project_ids)s::int[])
GROUP BY 1
ORDER BY 1;