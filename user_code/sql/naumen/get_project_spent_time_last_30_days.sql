SELECT
  dosm.attempt_start,
  dosm.operator_pickup_time,
  dosm.speaking_time,
  dosm.wrapup_time,
  mocp.title AS project_title
FROM detail_outbound_sessions_ms AS dosm
LEFT JOIN mv_outcoming_call_project AS mocp
  ON dosm.project_id = mocp.uuid
WHERE dosm.attempt_start >= %(start_date)s
  AND dosm.attempt_start <  %(end_date)s
  AND dosm.attempt_result = 'connected';