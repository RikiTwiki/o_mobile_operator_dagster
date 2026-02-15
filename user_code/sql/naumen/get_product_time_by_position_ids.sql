SELECT
    to_char(date_trunc(%(trunc)s::text, entered), 'YYYY-MM-DD') AS date,
    login,
    SUM(EXTRACT(EPOCH FROM (leaved - entered)))::bigint AS duration
FROM status_changes_ms
WHERE entered >= %(start_date)s
  AND entered  < %(end_date)s
  AND status = ANY(%(product_statuses)s::text[])
  AND login  = ANY(%(logins)s::text[])
GROUP BY 1, 2
ORDER BY 1, 2