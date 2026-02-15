SELECT
    to_char(date_trunc(%(trunc)s::text, sc.created_at + INTERVAL '6 hours'), %(date_format)s) AS date,
    u.login,
    sc.code,
    SUM(sc.duration) AS seconds,
    ROUND(SUM(sc.duration) / 60.0, 1) AS minutes,
    ROUND(SUM(sc.duration) / 3600.0, 3) AS hours
FROM
    state_changes sc
LEFT JOIN
    users u ON u.id = sc.user_id
WHERE
    (sc.created_at + INTERVAL '6 hours') >= %(start_date)s
    AND (sc.created_at + INTERVAL '6 hours') < %(end_date)s
    AND u.login IN %(logins)s
GROUP BY 1, 2, 3
ORDER BY  2;


