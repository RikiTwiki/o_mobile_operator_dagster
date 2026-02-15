SELECT
    login,
    date_trunc('day', entered) AS date,
    CONCAT(status, ' - ', reason) AS status,
    ROUND(SUM(duration) / 60000.0, 1) AS duration
FROM status_changes_ms
WHERE entered >= %(start_date)s
  AND entered <  %(end_date)s
  AND status = 'away'
  AND reason IS NOT NULL
GROUP BY 1, 2, 3

UNION

SELECT
    login,
    date_trunc('day', entered) AS date,
    status,
    ROUND(SUM(duration) / 60000.0, 1) AS duration
FROM status_changes_ms
WHERE entered >= %(start_date)s
  AND entered <  %(end_date)s
GROUP BY 1, 2, 3