SELECT
  to_char(date_trunc(%(trunc)s::text, repeat_calls_period_date), %(date_format)s) AS date,
  SUM(CASE WHEN resolve_status = 'resolved' THEN 1 ELSE 0 END)      AS resolved,
  SUM(CASE WHEN resolve_status = 'not_resolved' THEN 1 ELSE 0 END)  AS not_resolved
FROM {table}
WHERE start_time = %(start_time)s
  AND repeat_calls_period_date >= %(start_date)s
  AND repeat_calls_period_date <  %(end_date)s
  AND "group" = ANY(%(groups)s)
GROUP BY 1;