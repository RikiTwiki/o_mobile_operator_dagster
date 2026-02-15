SELECT
  to_char(date_trunc(%(trunc)s::text, ts.created_at), %(date_format)s) AS date,
  concat(t.auxiliary_group, ' - ', t.title) AS themes_joined,
  COUNT(rserd.mark) AS total
FROM bpm.topic_selects AS ts
LEFT JOIN bpm.topics AS t
  ON t.id = ts.topic_id
LEFT JOIN stat.replication_slr_employees_rating_data AS rserd
  ON rserd.session_id = ts.session_id
 AND rserd.user_login = ts.creator_login
WHERE ts.created_at >= %(start_date)s
  AND ts.created_at <  %(end_date)s
  AND ts.active IS TRUE
  AND ts.topic_view_id = %(view_id)s
  AND ts.source_id = ANY(%(sources)s::int[])
  AND rserd.mark  = ANY(%(sources)s::int[])
GROUP BY 1, 2
ORDER BY 1;
