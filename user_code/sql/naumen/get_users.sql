SELECT DISTINCT dosm.login AS login
FROM detail_outbound_sessions_ms AS dosm
WHERE dosm.attempt_start >= %(start_date)s
  AND dosm.attempt_start  < %(end_date)s
  AND dosm.login IS NOT NULL
ORDER BY dosm.login ASC;