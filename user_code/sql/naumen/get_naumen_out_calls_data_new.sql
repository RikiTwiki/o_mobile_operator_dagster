SELECT
  dosm.attempt_start,
  dosm.attempt_result,
  dosm.speaking_time,
  dosm.number_type
FROM detail_outbound_sessions_ms AS dosm
LEFT JOIN mv_employee AS me
  ON me.login = dosm.login
WHERE dosm.attempt_start >= %(start_date)s
  AND dosm.attempt_start <  %(end_date)s
  AND dosm.project_id = %(project_id)s;