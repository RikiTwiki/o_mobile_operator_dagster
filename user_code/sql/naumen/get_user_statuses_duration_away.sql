SELECT
    login,
    date_trunc('day', entered) AS date,
    concat(status, ' - ', reason) AS status,
    round(sum(duration) / 60000.0, 1) AS duration
FROM status_changes_ms
WHERE entered >= %(start_date)s
  AND entered <  %(end_date)s
  AND status = 'away'
  AND reason IS NOT NULL
GROUP BY 1, 2, 3;