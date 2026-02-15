WITH total_sql AS (
    SELECT
        date_trunc(%(trunc)s, amphtd.chat_finished_at) AS bucket_ts,
        COUNT(DISTINCT amphtd.chat_id) AS total_handled_chats,
        ROUND(AVG(amphtd.user_reaction_time)::numeric / 60) AS average_reaction_time,
        ROUND(
            CASE
                WHEN SUM(amphtd.user_count_replies) = 0
                    THEN SUM(amphtd.user_total_reaction_time)::numeric / 60
                ELSE
                    (SUM(amphtd.user_total_reaction_time)::numeric
                        / NULLIF(SUM(amphtd.user_count_replies)::numeric, 0)
                    ) / 60
            END
        ) AS average_speed_to_answer,
        ROUND(SUM(amphtd.user_total_reaction_time)::numeric / 60) AS user_total_reaction_time,
        SUM(amphtd.user_count_replies)::numeric AS user_count_replies
    FROM stat.aggregation_mp_handling_time_data amphtd
    WHERE amphtd.project_id = ANY(%(project_ids)s::int[])
      AND amphtd.chat_finished_at >= %(start_date)s
      AND amphtd.chat_finished_at <  %(end_date)s
    GROUP BY 1
),
operator_sql AS (
    SELECT
        date_trunc(%(trunc)s, amphtd.chat_finished_at) AS bucket_ts,
        COUNT(DISTINCT amphtd.chat_id) AS total_by_operator
    FROM stat.aggregation_mp_handling_time_data amphtd
    WHERE amphtd.project_id = ANY(%(project_ids)s::int[])
      AND amphtd.chat_finished_at >= %(start_date)s
      AND amphtd.chat_finished_at <  %(end_date)s
      AND amphtd.user_id != 129
    GROUP BY 1
),
bot_sql AS (
    SELECT
        date_trunc(%(trunc)s, amphtd.chat_finished_at) AS bucket_ts,
        COUNT(DISTINCT amphtd.chat_id) AS bot_handled_chats
    FROM stat.aggregation_mp_handling_time_data amphtd
    WHERE amphtd.project_id = ANY(%(project_ids)s::int[])
      AND amphtd.chat_finished_at >= %(start_date)s
      AND amphtd.chat_finished_at <  %(end_date)s
      AND amphtd.user_id = 129
      AND amphtd.is_bot_only = TRUE
    GROUP BY 1
)
SELECT
    to_char(t.bucket_ts, %(date_format)s) AS {date_field},
    t.total_handled_chats,
    COALESCE(o.total_by_operator, 0) AS total_by_operator,
    COALESCE(b.bot_handled_chats, 0) AS bot_handled_chats,
    t.average_reaction_time AS average_reaction_time,
    t.average_speed_to_answer AS average_speed_to_answer,
    t.user_total_reaction_time AS user_total_reaction_time,
    t.user_count_replies AS user_count_replies
FROM total_sql t
LEFT JOIN operator_sql o USING (bucket_ts)
LEFT JOIN bot_sql b USING (bucket_ts)
ORDER BY 1;