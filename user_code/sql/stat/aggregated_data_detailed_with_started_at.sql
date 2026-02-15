WITH base AS (
  SELECT
    amphtd.*,
    COALESCE(amphtd.chat_created_at, amphtd.chat_finished_at) AS coalesced_utc
  FROM stat.aggregation_mp_handling_time_data AS amphtd
),
coalesced AS (
  SELECT
    b.*,
    CASE
      WHEN b.coalesced_utc < TIMESTAMP '2024-08-21'
        THEN b.coalesced_utc + INTERVAL '6 hours'
      ELSE b.coalesced_utc
    END AS coalesced_local
  FROM base b
)
SELECT
  TO_CHAR(
    DATE_TRUNC(%(trunc)s::text, c.coalesced_local),
    %(date_format)s
  ) AS "{date_field}",

  COUNT(DISTINCT c.chat_id) AS total,
  AVG(c.user_reaction_time) AS average_reaction_time,
  MAX(c.user_reaction_time) AS max_reaction_time,

  CASE
    WHEN SUM(c.user_count_replies) = 0
      THEN SUM(c.user_total_reaction_time)::numeric
    ELSE
      SUM(c.user_total_reaction_time)::numeric
      / NULLIF(SUM(c.user_count_replies)::numeric, 0)
  END AS average_speed_to_answer,

  MAX(
    CASE
      WHEN c.user_count_replies = 0
        THEN c.user_total_reaction_time
      ELSE
        c.user_total_reaction_time / NULLIF(c.user_count_replies, 0)
    END
  )::numeric AS max_speed_to_answer,

  SUM(c.user_total_reaction_time)::numeric AS user_total_reaction_time,
  SUM(c.user_reaction_time)::numeric       AS user_reaction_time,
  SUM(c.user_count_replies)::numeric       AS user_count_replies,

  COUNT(DISTINCT CASE WHEN c.type = 'bot'  AND c.is_bot_only = TRUE THEN c.chat_id END) AS bot_answers_count,
  COUNT(DISTINCT CASE WHEN c.type = 'user' THEN c.chat_id END) AS operator_answers_count,

  CASE
    WHEN COUNT(DISTINCT c.chat_id) = 0 THEN 0
    ELSE ROUND(
      COUNT(DISTINCT CASE WHEN c.type = 'bot' AND c.is_bot_only = TRUE THEN c.chat_id END) * 100.0 / COUNT(DISTINCT c.chat_id), 1
    )
  END AS bssr

FROM coalesced AS c
LEFT JOIN bpm.staff_units AS su
  ON c.staff_unit_id = su.id
WHERE
  c.coalesced_local >= %(start_date)s
  AND c.coalesced_local <  %(end_date)s

  AND (
    su.position_id = ANY(%(pst_positions)s::int[])
    OR su.user_id = 129
    OR (c.staff_unit_id IS NULL AND c.user_id = 129)
  )

  AND c.project_id = ANY(%(project_ids)s::int[])
  {excluded_chats_filter}
  {exclude_dates_filter}
  {exclude_splits_filter}

GROUP BY 1
ORDER BY 1;