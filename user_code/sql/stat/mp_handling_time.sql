SELECT
  amhtd.chat_finished_at::date AS date,
  amhtd.chat_id,
  amhtd.user_login AS login,
  ROUND(amhtd.user_reaction_time::numeric)::numeric AS user_reaction_time,
  ROUND(amhtd.user_average_replies_time::numeric, 1)::numeric AS user_average_replies_time,
  amhtd.user_count_replies,
  amhtd.agent_leg_number
FROM stat.aggregation_mp_handling_time_data AS amhtd
LEFT JOIN bpm.staff_units AS su
  ON su.user_id = amhtd.user_id
LEFT JOIN bpm.positions AS p
  ON p.id = su.position_id
WHERE amhtd.chat_finished_at >= %(start_date)s
  AND amhtd.chat_finished_at <  %(end_date)s
  AND amhtd.user_reaction_time >= 900
  AND p.department_id IN (2,5,13,14,16,18,20)
GROUP BY 1,2,3,4,5,6,7
ORDER BY 1;