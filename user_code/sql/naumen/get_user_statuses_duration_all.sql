SELECT
    login,
    date_trunc('day', entered) AS date,
    status,
    round(sum(duration) / 60000.0, 1) AS duration
FROM status_changes_ms
WHERE entered >= %(start_date)s
  AND entered <  %(end_date)s
GROUP BY 1, 2, 3;