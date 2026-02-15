SELECT
  TO_CHAR(
    DATE_TRUNC(%(trunc)s::text, ajhtd.chat_finished_at),
    %(date_format)s
  ) AS {date_field},
  jp.title AS project,
  COUNT(ajhtd.widget_id) AS total,
  AVG(ajhtd.chat_reaction_time) AS average_reaction_time,
  SUM(ajhtd.chat_total_reaction_time)::numeric
    / NULLIF(SUM(ajhtd.chat_count_replies)::numeric, 0) AS average_speed_to_answer,
  SUM(ajhtd.chat_total_reaction_time)::numeric AS chat_total_reaction_time,
  SUM(ajhtd.chat_reaction_time)::numeric AS chat_reaction_time,
  SUM(ajhtd.chat_count_replies)::numeric AS chat_count_replies
FROM stat.aggregation_jivo_handling_time_data AS ajhtd
LEFT JOIN bpm.jivo_projects AS jp
       ON jp.widget_id = ajhtd.widget_id
WHERE ajhtd.type = 'agent'
  AND ajhtd.agent_leg_number = 1
  AND ajhtd.chat_finished_at >= %(start_date)s
  AND ajhtd.chat_finished_at <  %(end_date)s
  {responsible_filter}
  {type_filter}
  {widgets_filter}
  {groups_filter}
GROUP BY 1, 2
ORDER BY 1;
