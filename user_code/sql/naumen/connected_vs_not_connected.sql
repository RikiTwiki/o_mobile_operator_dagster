SELECT
  to_char(dos.attempt_start, 'YYYY-MM-DD') AS date,
  CASE WHEN dos.attempt_result = 'connected' THEN 'Соединено' ELSE 'Недозвон' END AS row,
  COUNT(dos.number_type) AS value
FROM detail_outbound_sessions AS dos
LEFT JOIN mv_employee AS me ON me.login = dos.login
WHERE dos.attempt_start BETWEEN %(start_date)s AND %(end_date)s
  AND dos.project_id = %(project_id)s
GROUP BY 1, 2
ORDER BY 1;