SELECT
    to_char(date_trunc(%(trunc)s::text, dc.created_at), %(date_format)s) AS date,
    t.data->>'project' AS project,
    dct.mistake,
    COUNT(dc.id) AS daily_control_count
FROM bpm.daily_control AS dc
LEFT JOIN bpm.tasks AS t
  ON dc.task_id = t.id
LEFT JOIN bpm.daily_control_templates AS dct
  ON dct.id = COALESCE(
       (
         SELECT (elem->>'id')::int
         FROM jsonb_array_elements(dc.mistakes) AS elem
         LIMIT 1
       ), 0
     )
  AND dc.has_note = TRUE
WHERE dc.created_at >= %(start_date)s
  AND dc.created_at <  %(end_date)s
  AND dc.staff_unit_id = %(staff_unit_id)s
  AND dc.has_note = TRUE
  AND (t.data->>'project') = ANY(%(projects)s::text[])
GROUP BY 1, 2, 3
ORDER BY 2;
