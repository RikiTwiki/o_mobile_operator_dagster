SELECT
  attempt_start,
  attempt_result,
  number_type
FROM detail_outbound_sessions
WHERE attempt_start >= %(start_date)s
  AND attempt_start <  %(end_date)s
  AND attempt_result <> 'connected'
  AND project_id = %(project_id)s;