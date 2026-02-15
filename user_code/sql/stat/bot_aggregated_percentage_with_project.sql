-- bot_aggregated_percentage_with_project.sql

SELECT
  TO_CHAR(
    DATE_TRUNC(%(trunc)s::text, amphtd.chat_finished_at),
    %(date_format)s
  ) AS {date_field},
  amphtd.project_id,

  CASE
    WHEN COUNT(DISTINCT amphtd.chat_id) = 0 THEN 0
    ELSE ROUND(
      COUNT(DISTINCT CASE
                       WHEN amphtd.type = 'bot'
                        AND amphtd.is_bot_only = TRUE
                      THEN amphtd.chat_id
                     END) * 100.0
      / COUNT(DISTINCT amphtd.chat_id), 1
    )
  END AS bot_handled_chats_percentage

FROM stat.aggregation_mp_handling_time_data AS amphtd
WHERE amphtd.chat_finished_at >= %(start_date)s
  AND amphtd.chat_finished_at <  %(end_date)s
  AND amphtd.project_id = ANY(%(project_ids)s::int[])
  {exclude_splits_filter}
  {excluded_days_filter}
GROUP BY 1, 2
ORDER BY 1;
