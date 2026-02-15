SELECT
  t.general_group                         AS general_group,
  t.auxiliary_group                       AS auxiliary_group,
  t.title                                  AS topic,
  concat(t.auxiliary_group, ' - ', t.title) AS title,
  COUNT(ts.id)                             AS count
FROM bpm.topic_selects AS ts
LEFT JOIN bpm.topics AS t ON t.id = ts.topic_id
WHERE ts.created_at >= %(start_date)s
  AND ts.created_at <  %(end_date)s
  AND ts.active IS TRUE
  AND ts.topic_view_id = %(view_id)s
  AND ts.source_id = ANY(%(sources)s::int[])
GROUP BY 1, 2, 3, 4
ORDER BY 5 DESC;
