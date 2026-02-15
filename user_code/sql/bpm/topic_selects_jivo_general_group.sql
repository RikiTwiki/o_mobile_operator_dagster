SELECT
  to_char(date_trunc(%(trunc)s::text, ts.created_at), %(date_format)s) AS {date_field},
  t.general_group   AS general_group,
  t.auxiliary_group AS auxiliary_group,
  COUNT(ts.topic_id) AS count
FROM bpm.topic_selects AS ts
LEFT JOIN bpm.topics        AS t  ON t.id = ts.topic_id
LEFT JOIN bpm.jivo_projects AS jp ON jp.widget_id = ts.project_id
WHERE ts.created_at >= %(start_date)s
  AND ts.created_at <  %(end_date)s
  AND t.general_group = %(general_group)s
  AND t.active IS TRUE
  AND jp.id IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY 1;
