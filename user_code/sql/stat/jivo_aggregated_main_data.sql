SELECT
  TO_CHAR(
    DATE_TRUNC(%(trunc)s::text, ajhtd.chat_finished_at),
    %(date_format)s
  ) AS {date_field},
  COUNT(*) AS total,
  SUM(CASE WHEN ajhtd.is_bot_only::bool THEN 1 ELSE 0 END)::numeric AS bot_only,
  (
    SUM(CASE WHEN ajhtd.agent_leg_number = 1 THEN 1 ELSE 0 END)::numeric
    + SUM(CASE WHEN ajhtd.is_bot_only::bool THEN 1 ELSE 0 END)::numeric
  ) AS specialist
FROM stat.aggregation_jivo_handling_time_data AS ajhtd
LEFT JOIN bpm.jivo_projects AS jp
       ON jp.widget_id = ajhtd.widget_id
WHERE ajhtd.chat_finished_at >= %(start_date)s
  AND ajhtd.chat_finished_at <  %(end_date)s
  {excluded_dates_filter}
  {responsible_filter}
  {type_filter}
  {groups_filter}
GROUP BY 1
ORDER BY 1;
