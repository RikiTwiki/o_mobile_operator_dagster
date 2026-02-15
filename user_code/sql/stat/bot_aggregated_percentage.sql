-- bot_aggregated_percentage.sql

SELECT
  TO_CHAR(
    DATE_TRUNC(%(trunc)s::text, amphtd.chat_finished_at),
    %(date_format)s
  ) AS {date_field},

  CASE
       WHEN count(DISTINCT amphtd.chat_id) = 0 THEN 0
       ELSE
           ROUND(COUNT(DISTINCT CASE WHEN amphtd.type = 'bot' AND is_bot_only = true THEN amphtd.chat_id END) * 100.0 / count(DISTINCT amphtd.chat_id),1)
  END AS bot_handled_chats_percentage,

  ROUND(COUNT(DISTINCT amphtd.chat_id)::numeric, 1) AS total_handled_chats,
  SUM(CASE WHEN amphtd.is_bot_only THEN 1 ELSE 0 END)::integer AS bot_handled_chats

FROM stat.aggregation_mp_handling_time_data AS amphtd
WHERE amphtd.chat_finished_at >= %(start_date)s
  AND amphtd.chat_finished_at <  %(end_date)s
  AND amphtd.project_id = ANY(%(project_ids)s::int[])
  {excluded_days_filter}
  {exclude_splits_filter}
GROUP BY 1
ORDER BY 1;