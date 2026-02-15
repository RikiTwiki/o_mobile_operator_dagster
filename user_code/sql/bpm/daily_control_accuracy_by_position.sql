SELECT
    to_char(date_trunc(%(trunc)s::text, dc.created_at), %(date_format)s) AS date,
    COUNT(dc.has_error) AS all_listened,
    SUM(CASE WHEN dc.has_error IS TRUE THEN 1 ELSE 0 END) AS mistakes_count,
    100 - (SUM(CASE WHEN dc.has_error IS TRUE THEN 1 ELSE 0 END)::numeric
           / NULLIF(COUNT(dc.has_error), 0)::numeric) * 100
        AS critical_error_accuracy
FROM bpm.daily_control AS dc
LEFT JOIN bpm.staff_units AS su
  ON dc.staff_unit_id = su.id
WHERE dc.created_at >= %(start_date)s
  AND dc.created_at <  %(end_date)s
  AND su.position_id = ANY(%(position_ids)s::int[])
GROUP BY 1
ORDER BY 1;
