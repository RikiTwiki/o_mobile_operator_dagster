SELECT
  TO_CHAR(DATE_TRUNC(%(trunc)s::text, s.started_at), %(date_format)s) AS "{date_field}",
  COUNT(*) AS total_chats
FROM cp.sessions AS s
WHERE s.type = 'text'
  AND s.started_at >= %(start_date)s
  AND s.started_at <  %(end_date)s
  AND s.init_project_id = ANY(%(project_ids)s::int[])
  {exclude_days_filter}
  {exclude_splits_filter}
GROUP BY 1
ORDER BY 1;