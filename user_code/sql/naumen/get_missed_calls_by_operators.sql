SELECT
  cl.created,
  emp.login,
  emp.firstname,
  emp.lastname,
  cl.voip_reason,
  qc.project_id
FROM mv_employee AS emp
LEFT JOIN call_legs AS cl
  ON cl.dst_abonent = emp.login
 AND cl.connected IS NULL
LEFT JOIN queued_calls_ms AS qc
  ON qc.session_id = cl.session_id
 AND cl.created >= qc.enqueued_time
 AND (qc.dequeued_time IS NULL OR cl.created < qc.dequeued_time)
WHERE cl.created >= %(start_date)s
  AND cl.created <  %(end_date)s
  AND emp.ou = %(ou)s
  AND emp.removed = false;