SELECT DISTINCT u.login
FROM bpm.staff_units AS su
LEFT JOIN bpm.timetable_data_detailed AS tdd ON tdd.staff_unit_id = su.id
JOIN bpm.users AS u ON u.id = su.user_id
WHERE su.user_id IS NOT NULL
  AND su.position_id = ANY(%(position_ids)s::int[])
  AND su.accepted_at < %(end_date)s::date
  AND (su.dismissed_at IS NULL OR su.dismissed_at >= %(start_date)s::date)
ORDER BY u.login;