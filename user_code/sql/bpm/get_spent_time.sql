SELECT
  to_char(tdd.date, 'YYYY-MM-DD') AS date,
  u.login AS operator_login,
  SUM(tdd.total_hours) AS total_hours
FROM bpm.timetable_data_detailed AS tdd
JOIN bpm.staff_units AS su ON su.id = tdd.staff_unit_id
JOIN bpm.users       AS u  ON u.id  = su.user_id
WHERE u.login = ANY(%(logins)s)
  AND tdd.date >= %(start_date)s::date
  AND tdd.date <= %(end_date)s::date
  AND tdd.timetable_session_id IN (1, 3, 7, 11, 25, 26)
GROUP BY 1, 2
ORDER BY 1, 2;