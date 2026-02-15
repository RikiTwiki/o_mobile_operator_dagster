SELECT
    u.login                             AS login,
    tdd.session_start                   AS start_time,
    tdd.session_end                     AS end_time,
    tdd.timetable_shift_id              AS id
FROM bpm.timetable_data_detailed tdd
LEFT JOIN bpm.staff_units su        ON tdd.staff_unit_id       = su.id
LEFT JOIN bpm.users u               ON su.user_id              = u.id
LEFT JOIN bpm.timetable_sessions ts ON tdd.timetable_session_id = ts.id
WHERE tdd.date = %(report_day)s::date
  AND (ts.props->>'productive_time')::boolean = TRUE
  AND tdd.timetable_view_id = ANY(%(view_ids)s::int[])