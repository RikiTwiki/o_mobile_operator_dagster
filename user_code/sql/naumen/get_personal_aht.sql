SELECT
  dos.attempt_start,
  dos.operator_pickup_time,
  dos.speaking_time,
  dos.wrapup_time,
  me.title AS title
FROM detail_outbound_sessions AS dos
LEFT JOIN mv_employee AS me
  ON me.login = dos.login
WHERE dos.attempt_start >= %(start_date)s
  AND dos.attempt_start <  %(end_date)s
  AND dos.attempt_result = 'connected'
  AND dos.speaking_time > 10
  AND me.ou = %(ou)s
  AND dos.project_id = %(project_id)s;