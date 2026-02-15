SELECT DISTINCT
    u.login,
    sv.login AS supervisor,
    d.abbreviation
FROM bpm.staff_units su
LEFT JOIN bpm.positions p ON p.id = su.position_id
LEFT JOIN bpm.departments d ON d.id = p.department_id
LEFT JOIN bpm.team_assigns ts ON ts.staff_unit_id = su.id AND ts.month = %(month_start)s
LEFT JOIN bpm.users u ON u.id = su.user_id
LEFT JOIN bpm.staff_units sus ON ts.staff_unit_team_lead_id = sus.id
LEFT JOIN bpm.users sv ON sv.id = sus.user_id
LEFT JOIN bpm.timetable_data_detailed tdd ON tdd.staff_unit_id = su.id
WHERE su.position_id = ANY(%(position_ids)s::int[])
  AND (
    (su.accepted_at < %(start_date)s AND su.dismissed_at >= %(end_date)s)
    OR
    (su.accepted_at < %(end_date)s AND su.dismissed_at IS NULL)
  )
  AND su.user_id IS NOT NULL
  AND tdd.date = %(start_date)s
    {excluded_shifts_filter}