SELECT
  to_char(date_trunc('day', entered), 'YYYY-MM-DD') AS date,
  date_trunc('hour', entered)                        AS hour,
  SUM(CASE WHEN status = 'normal'  THEN duration ELSE 0 END)::bigint   AS normal,
  SUM(CASE WHEN status = 'ringing' THEN duration ELSE 0 END)::bigint   AS ringing,
  SUM(CASE WHEN status = 'speaking' THEN duration ELSE 0 END)::bigint  AS speaking,
  SUM(CASE WHEN status = 'wrapup'  THEN duration ELSE 0 END)::bigint   AS wrapup
FROM status_changes_ms
WHERE entered >= %(start_date)s
  AND entered <  %(end_date)s
  AND status IN ('normal','ringing','speaking','wrapup')
  AND login = ANY(%(logins)s::text[])
GROUP BY 1, 2
ORDER BY 1, 2;