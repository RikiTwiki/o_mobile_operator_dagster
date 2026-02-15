SELECT
  to_char(dos.attempt_start, 'YYYY-MM-DD') AS date,
  SUM(CASE WHEN dos.attempt_result = 'connected' AND dos.speaking_time > 5 THEN 1 ELSE 0 END) AS value
FROM detail_outbound_sessions AS dos
LEFT JOIN mv_employee AS me ON me.login = dos.login
WHERE dos.attempt_start BETWEEN %(start_date)s AND %(end_date)s
  AND NOT (dos.project_id = ANY (%(excluded)s::text[]))
GROUP BY 1
ORDER BY 1;