SELECT DISTINCT u.login
FROM bpm.staff_units su
LEFT JOIN bpm.timetable_data_detailed tdd
       ON tdd.staff_unit_id = su.id
LEFT JOIN bpm.users u
       ON u.id = su.user_id
WHERE su.user_id IS NOT NULL
  AND su.position_id = ANY(%(position_ids)s::int[])
  AND su.accepted_at < %(end_date)s
  AND (su.dismissed_at >= %(start_date)s OR su.dismissed_at IS NULL)
  {extra_view_filter}
ORDER BY u.login;