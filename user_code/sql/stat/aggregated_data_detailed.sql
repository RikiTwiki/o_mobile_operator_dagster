SELECT
  TO_CHAR(
    DATE_TRUNC(%(trunc)s::text,
      CASE
        WHEN amphtd.chat_finished_at < TIMESTAMP '2024-08-21'
          THEN amphtd.chat_finished_at
        ELSE amphtd.chat_finished_at
      END
    ),
    %(date_format)s
  ) AS "{date_field}",

  COUNT(DISTINCT amphtd.chat_id) AS total,
  AVG(amphtd.user_reaction_time) AS average_reaction_time,
  MAX(amphtd.user_reaction_time) AS max_reaction_time,
  CASE
    WHEN SUM(amphtd.user_count_replies) = 0
      THEN SUM(amphtd.user_total_reaction_time)::numeric
    ELSE
      SUM(amphtd.user_total_reaction_time)::numeric
      / NULLIF(SUM(amphtd.user_count_replies)::numeric, 0)
  END AS average_speed_to_answer,
  MAX(
    CASE
      WHEN amphtd.user_count_replies = 0
        THEN amphtd.user_total_reaction_time
      ELSE
        amphtd.user_total_reaction_time / NULLIF(amphtd.user_count_replies, 0)
    END
  )::numeric AS max_speed_to_answer,
  SUM(amphtd.user_total_reaction_time)::numeric AS user_total_reaction_time,
  SUM(amphtd.user_reaction_time)::numeric       AS user_reaction_time,
  SUM(amphtd.user_count_replies)::numeric       AS user_count_replies,
  COUNT(DISTINCT CASE WHEN amphtd.type = 'bot'  AND amphtd.is_bot_only = TRUE THEN amphtd.chat_id END) AS bot_answers_count,
  COUNT(DISTINCT CASE WHEN amphtd.type = 'user' THEN amphtd.chat_id END) AS operator_answers_count,

  CASE
    WHEN COUNT(DISTINCT amphtd.chat_id) = 0 THEN 0
    ELSE ROUND(
      COUNT(DISTINCT CASE WHEN amphtd.type = 'bot' AND amphtd.is_bot_only = TRUE THEN amphtd.chat_id END) * 100.0 / COUNT(DISTINCT amphtd.chat_id), 1
    )
  END AS bssr

FROM stat.aggregation_mp_handling_time_data AS amphtd
LEFT JOIN bpm.staff_units AS su
  ON amphtd.staff_unit_id = su.id
WHERE amphtd.chat_finished_at >= %(start_date)s
  AND amphtd.chat_finished_at <  %(end_date)s
  AND (su.position_id = ANY(%(pst_positions)s::int[]) OR su.user_id = 129)
  AND amphtd.project_id = ANY(%(project_ids)s::int[])
  {excluded_chats_filter}
  {exclude_dates_filter}
  {exclude_splits_filter}
GROUP BY 1
ORDER BY 1;