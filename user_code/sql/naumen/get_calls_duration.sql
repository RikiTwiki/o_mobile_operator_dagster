SELECT
  dos.attempt_start,
  dos.speaking_time,
  dos.session_id,
  me.title AS title
FROM detail_outbound_sessions AS dos
LEFT JOIN mv_employee AS me
  ON me.login = dos.login
WHERE dos.attempt_start >= %(start_date)s
  AND dos.attempt_start <  %(end_date)s
  AND dos.attempt_result = 'connected'
  AND dos.project_id = %(project_id)s
  AND me.ou = %(ou)s;