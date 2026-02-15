SELECT COALESCE(
         SUM(((COALESCE(tdd.day_hours, 0) + COALESCE(tdd.night_hours, 0)) * 3600)::bigint),
         0
       ) AS planned_seconds
FROM bpm.timetable_data_detailed AS tdd
JOIN bpm.staff_units AS su
  ON su.id = tdd.staff_unit_id
 AND su.accepted_at <= %(end_date)s
 AND (su.dismissed_at > %(start_date)s OR su.dismissed_at IS NULL)
JOIN bpm.timetable_sessions AS ts
  ON ts.id = tdd.timetable_session_id
WHERE su.position_id = ANY(%(position_ids)s::int[])
  AND tdd.date = %(start_date)s
  AND COALESCE((ts.props->>'productive_time')::boolean, FALSE) = TRUE;
