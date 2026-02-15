SELECT
    sc.code,
    SUM(sc.duration) as duration
FROM state_changes as sc
LEFT JOIN users as u
    ON u.id = sc.user_id
WHERE (sc.created_at + INTERVAL '6 hours') >= %(start_date)s
  AND (sc.created_at + INTERVAL '6 hours') <  %(end_date)s
AND u.login = ANY(%(logins)s::text[])
GROUP BY 1
ORDER BY 1