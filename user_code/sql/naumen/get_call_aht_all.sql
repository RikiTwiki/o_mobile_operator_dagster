SELECT
  attempt_start,
  operator_pickup_time,
  speaking_time,
  wrapup_time
FROM detail_outbound_sessions
WHERE attempt_start >= %(start_date)s
  AND attempt_start <  %(end_date)s
  AND attempt_result = 'connected'
  AND speaking_time > 10;