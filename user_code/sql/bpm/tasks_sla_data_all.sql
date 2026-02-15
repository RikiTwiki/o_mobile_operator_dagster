SELECT
    COUNT(id)::numeric AS total,
    SUM(CASE WHEN sla_reached IS TRUE THEN 1 ELSE 0 END)::numeric AS sla_reached,
    SUM(CASE WHEN sla_reached IS FALSE THEN 1 ELSE 0 END)::numeric AS sla_not_reached
FROM bpm.tasks
WHERE finished_at >= %(start_date)s
  AND finished_at <  %(end_date)s;
