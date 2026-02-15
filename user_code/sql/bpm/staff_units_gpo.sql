SELECT DISTINCT su.user_id
FROM bpm.staff_units su
LEFT JOIN bpm.timetable_data_detailed tdd
  ON tdd.staff_unit_id = su.id
WHERE tdd.timetable_view_id = ANY(%(view_ids)s::int[])
  AND tdd.date = %(start_date)s::date
  AND su.accepted_at::date <= %(end_date)s::date
  AND (su.dismissed_at IS NULL OR su.dismissed_at::date > %(start_date)s::date)
ORDER BY su.user_id;
