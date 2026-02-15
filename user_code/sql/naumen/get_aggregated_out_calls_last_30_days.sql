SELECT
  dos.attempt_start,
  dos.number_type
FROM detail_outbound_sessions AS dos
LEFT JOIN mv_employee AS me
  ON me.login = dos.login
WHERE dos.attempt_start >= %(start_date)s
  AND dos.attempt_start <  %(end_date)s
  AND dos.project_id = %(project_id)s;