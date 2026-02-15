SELECT
  to_char(
    date_trunc('{trunc}',
        CASE
        WHEN amphtd.chat_finished_at < '2024-08-21'
          THEN amphtd.chat_finished_at
        ELSE amphtd.chat_finished_at
      END
    ),
    '{date_format}'
  ) AS "{date_field}",
  count(DISTINCT amphtd.chat_id) AS total,
  AVG(amphtd.user_reaction_time) AS average_reaction_time,
  CASE
    WHEN SUM(amphtd.user_count_replies) = 0
      THEN SUM(amphtd.user_total_reaction_time)::numeric
    ELSE SUM(amphtd.user_total_reaction_time)::numeric
         / NULLIF(SUM(amphtd.user_count_replies)::numeric, 0)
  END AS average_speed_to_answer,
  SUM(amphtd.user_total_reaction_time)::integer AS user_total_reaction_time,
  SUM(amphtd.user_count_replies)::integer AS user_count_replies
FROM stat.aggregation_mp_handling_time_data amphtd
LEFT JOIN bpm.staff_units su
  ON amphtd.staff_unit_id = su.id
WHERE amphtd.chat_finished_at >= %(start_date)s
  AND amphtd.chat_finished_at  < %(end_date)s
  AND amphtd.user_id != 129
  AND amphtd.is_bot_only = false
  AND amphtd.project_id = ANY(%(project_ids)s::int[])
  AND su.position_id = ANY(%(pst_positions)s::int[])
  AND NOT (amphtd.chat_id = ANY(COALESCE(%(excluded_chats)s::int[], ARRAY[]::int[])))
GROUP BY 1
ORDER BY 1;
