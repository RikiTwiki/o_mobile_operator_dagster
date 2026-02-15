    SELECT
      to_char(
        date_trunc(%(trunc)s,
          CASE
            WHEN amphtd.chat_finished_at < '2024-08-21'
            THEN amphtd.chat_finished_at + interval '6 hours'
            ELSE amphtd.chat_finished_at
          END
        ),
        %(date_format)s
      ) AS {date_field},
      CASE
        WHEN u.login IS NULL THEN amphtd.user_login
        ELSE u.last_name || ' ' || u.first_name
      END AS title,
      d.abbreviation,
      COUNT(DISTINCT amphtd.chat_id) AS total,
      AVG(amphtd.user_reaction_time) AS average_reaction_time,
      CASE
        WHEN SUM(amphtd.user_count_replies) = 0
        THEN SUM(amphtd.user_total_reaction_time)::numeric
        ELSE SUM(amphtd.user_total_reaction_time)::numeric
             / NULLIF(SUM(amphtd.user_count_replies)::numeric, 0)
      END  average_speed_to_answer,
      SUM(amphtd.user_total_reaction_time)::numeric AS user_total_reaction_time,
      SUM(amphtd.user_reaction_time)::numeric AS user_reaction_time,
      SUM(amphtd.user_count_replies)::numeric AS user_count_replies
    FROM stat.aggregation_mp_handling_time_data amphtd
    LEFT JOIN bpm.staff_units su ON amphtd.staff_unit_id = su.id
    LEFT JOIN bpm.users u  ON u.id = su.user_id
    LEFT JOIN bpm.positions p  ON p.id = su.position_id
    LEFT JOIN bpm.departments d  ON d.id = p.department_id
    WHERE amphtd.chat_finished_at >= %(start_date)s
      AND amphtd.chat_finished_at < %(end_date)s
      AND amphtd.user_id != 129
--       AND amphtd.user_id != 572
      AND (su.dismissed_at >= %(end_date)s OR su.dismissed_at IS NULL)
      AND amphtd.project_id = ANY(%(project_ids)s::int[])
      {pst_positions_filter}
      {excluded_chats_filter}
    GROUP BY 1, 2, 3
    ORDER BY 1;