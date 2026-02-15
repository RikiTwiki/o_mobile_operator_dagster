SELECT
    COALESCE(
        SUM(((tdd.day_hours + tdd.night_hours) * 3600)::bigint),
        0
    ) AS planned_seconds
FROM bpm.timetable_data_detailed AS tdd
LEFT JOIN bpm.staff_units AS su
       ON tdd.staff_unit_id = su.id
      AND su.accepted_at <= %(end_date)s
      AND (su.dismissed_at > %(start_date)s OR su.dismissed_at IS NULL)
LEFT JOIN bpm.timetable_sessions AS ts
       ON ts.id = tdd.timetable_session_id
WHERE su.position_id = ANY(%(position_ids)s::int[])
  AND tdd.month = %(start_date)s
  AND COALESCE((ts.props->>'productive_time')::boolean, FALSE) = TRUE;
