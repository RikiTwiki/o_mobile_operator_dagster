SELECT
  to_char(date_trunc(%(trunc)s::text, ts.created_at), %(date_format)s) AS date,
  t.general_group AS general_group,
  COUNT(ts.topic_id) AS count
FROM bpm.topic_selects AS ts
LEFT JOIN bpm.topics AS t ON t.id = ts.topic_id
WHERE ts.created_at >= %(start_date)s
  AND ts.created_at <  %(end_date)s
  AND t.active IS TRUE
  AND ts.topic_view_id = %(view_id)s
  AND ts.source_id = ANY(%(sources)s)
GROUP BY 1, 2
ORDER BY 1;
