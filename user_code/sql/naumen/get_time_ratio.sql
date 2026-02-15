SELECT
  sch.entered,
  me.login,
  sch.duration AS duration_ms
FROM status_changes_ms AS sch
JOIN mv_employee     AS me ON me.login = sch.login
WHERE sch.entered >= %(start_date)s
  AND sch.entered <  %(end_date)s
  AND me.position_id = ANY(%(position_ids)s::int[])
  AND sch.status IN ('ringing','speaking','wrapup','custom2');