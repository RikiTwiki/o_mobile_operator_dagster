SELECT
  to_char(date_trunc(%(trunc)s::text, ts.created_at), %(date_format)s) AS date,
  t.id                                AS id,
  ts.topic_view_id                    AS project_id,
  ts.topic_view_title                 AS project_name,
  t.general_group                     AS general_group,
  t.auxiliary_group                   AS auxiliary_group,
  t.title                             AS title,
  COUNT(ts.topic_id)                  AS count
FROM bpm.topic_selects AS ts
LEFT JOIN bpm.topics       AS t  ON t.id = ts.topic_id
LEFT JOIN bpm.topic_views  AS tv ON tv.id = ts.topic_view_id
WHERE ts.created_at >= %(start_date)s
  AND ts.created_at <  %(end_date)s
  AND t.active IS TRUE
  AND ts.topic_view_id = ANY(%(view_ids)s::int[])
  AND ts.source_id     = ANY(%(sources)s::int[])
GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY 1;
