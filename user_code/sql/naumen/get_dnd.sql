SELECT
  sch.entered,
  me.title AS title,
  sch.duration
FROM status_changes_ms AS sch
LEFT JOIN mv_employee AS me
  ON me.login = sch.login
WHERE sch.entered >= %(start_date)s
  AND sch.entered <  %(end_date)s
  AND me.ou = %(ou)s
  AND sch.status = 'dnd';