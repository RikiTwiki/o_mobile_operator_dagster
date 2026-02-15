SELECT
    to_char(date_trunc(%(trunc)s::text, dc.created_at), %(date_format)s) AS date,
    COUNT(dc.has_error) AS all_listened,
    SUM(CASE WHEN dc.has_error IS TRUE THEN 1 ELSE 0 END) AS mistakes_count,
    100 - (SUM(CASE WHEN dc.has_error IS TRUE THEN 1 ELSE 0 END)::numeric
           / NULLIF(COUNT(dc.has_error), 0)::numeric) * 100
        AS critical_error_accuracy
FROM bpm.daily_control AS dc
WHERE dc.created_at >= %(start_date)s
  AND dc.created_at <  %(end_date)s
GROUP BY 1
ORDER BY 1;
