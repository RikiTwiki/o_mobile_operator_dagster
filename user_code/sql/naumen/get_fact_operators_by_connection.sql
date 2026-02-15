SELECT
  date_trunc('hour', entered) AS date,
  login,
  SUM(duration) AS duration
FROM status_changes_ms
WHERE entered >= %(start_date)s
  AND entered <  %(end_date)s
  AND status IN ('speaking','wrapup','ringing','standoff','normal')
GROUP BY 1, 2
ORDER BY 1;