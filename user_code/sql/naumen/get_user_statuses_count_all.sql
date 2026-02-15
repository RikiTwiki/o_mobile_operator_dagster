SELECT
    login,
    date_trunc('day', entered) AS date,
    status,
    count(status) AS count
FROM status_changes_ms
WHERE entered >= %(start_date)s
  AND entered <  %(end_date)s
GROUP BY 1, 2, 3;
