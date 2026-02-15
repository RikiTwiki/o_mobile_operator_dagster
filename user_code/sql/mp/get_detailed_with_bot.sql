SELECT
  to_char(
    date_trunc(%(trunc)s::text, amphtd.chat_finished_at),
    %(date_format)s
  ) AS {date_field},
  amphtd.split_title AS project,

  -- счётчики чатов
  COUNT(DISTINCT amphtd.chat_id) AS total,
  COUNT(DISTINCT amphtd.chat_id) FILTER (WHERE amphtd.is_bot_only IS TRUE) AS bot_closed,
  (COUNT(DISTINCT amphtd.chat_id)
     - COUNT(DISTINCT amphtd.chat_id) FILTER (WHERE amphtd.is_bot_only IS TRUE)
  ) AS chats_with_agents,

  -- метрики по людям (бот исключён)
  AVG(amphtd.user_reaction_time) FILTER (WHERE amphtd.user_id != 129) AS average_reaction_time,
  CASE
    WHEN COALESCE(SUM(amphtd.user_count_replies) FILTER (WHERE amphtd.user_id != 129), 0) = 0
    THEN SUM(amphtd.user_total_reaction_time) FILTER (WHERE amphtd.user_id != 129)::numeric
    ELSE (SUM(amphtd.user_total_reaction_time) FILTER (WHERE amphtd.user_id != 129))::numeric
         / NULLIF((SUM(amphtd.user_count_replies) FILTER (WHERE amphtd.user_id != 129))::numeric, 0)
  END AS average_speed_to_answer,
  SUM(amphtd.user_total_reaction_time) FILTER (WHERE amphtd.user_id != 129)::numeric AS user_total_reaction_time,
  SUM(amphtd.user_reaction_time)        FILTER (WHERE amphtd.user_id != 129)::numeric AS user_reaction_time,
  SUM(amphtd.user_count_replies)        FILTER (WHERE amphtd.user_id != 129)::numeric AS user_count_replies

FROM stat.aggregation_mp_handling_time_data AS amphtd
LEFT JOIN bpm.staff_units AS su ON su.id = amphtd.staff_unit_id

WHERE amphtd.chat_finished_at >= %(start_date)s
  AND amphtd.chat_finished_at <  %(end_date)s
  AND amphtd.project_id = ANY(%(project_ids)s::int[])
{excluded_chats_filter}
{pst_positions_filter}

GROUP BY 1, 2
ORDER BY 1;