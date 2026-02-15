SELECT
    SUM(CASE WHEN dc.has_error = TRUE THEN 1 ELSE 0 END) AS mistakes,
    COUNT(dc.session_id)                                  AS all_listened
FROM bpm.daily_control AS dc
LEFT JOIN bpm.tasks AS t
  ON dc.task_id = t.id
WHERE dc.staff_unit_id = %(staff_unit_id)s
  AND dc.created_at >= %(start_date)s
  AND dc.created_at <  %(end_date)s
  AND ((t.data->>'project_id')::int) = ANY(%(project_ids)s::int[]);
