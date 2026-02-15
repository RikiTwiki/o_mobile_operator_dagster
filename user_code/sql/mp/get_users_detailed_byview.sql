WITH max_index AS (
SELECT
    tdd.staff_unit_id,
    tdd.date,
    MAX(tdd.staff_unit_index) AS max_staff_unit_index
FROM bpm.timetable_data_detailed AS tdd
WHERE
    tdd.date >= %(start_date)s
    AND tdd.date < %(end_date)s
GROUP BY
    tdd.staff_unit_id,
    tdd.date
),

tdd_sub AS (
SELECT
    tdd.date,
    tdd.staff_unit_id,
    tdd.session_start,
    tdd.session_end,
    tdd.timetable_session_id,
    tdd.timetable_shift_id,
    tdd.session_index
FROM bpm.timetable_data_detailed AS tdd
JOIN max_index AS mi
  ON tdd.staff_unit_id = mi.staff_unit_id
AND tdd.date = mi.date
AND tdd.staff_unit_index = mi.max_staff_unit_index
)

SELECT
TO_CHAR(
  DATE_TRUNC(
    %(trunc)s,
    CASE
      WHEN ts.start IN ('23:00:00', '21:00:00', '19:00:00')
      AND amphtd.chat_finished_at::time >= '00:00:00'
      AND amphtd.chat_finished_at::time <= '08:00:00'
      THEN (amphtd.chat_finished_at - INTERVAL '1 day')::date
      ELSE amphtd.chat_finished_at::date
    END
  ),
  'YYYY-MM-DD'
) AS date,

CASE
  WHEN u.login IS NULL
  THEN amphtd.user_login
  ELSE u.last_name || ' ' || u.first_name
END AS title,

ts.start || '-' || ts.end AS shifts,

COUNT(DISTINCT amphtd.chat_id) AS total
FROM stat.aggregation_mp_handling_time_data AS amphtd
LEFT JOIN bpm.staff_units AS su
ON amphtd.staff_unit_id = su.id
LEFT JOIN bpm.users AS u
ON u.id = su.user_id
LEFT JOIN tdd_sub AS tdd
ON tdd.staff_unit_id = su.id
LEFT JOIN bpm.timetable_shifts AS ts
ON ts.id = tdd.timetable_shift_id
LEFT JOIN bpm.timetable_sessions AS tses
ON tses.id = tdd.timetable_session_id
WHERE
amphtd.chat_finished_at >= (tdd.session_start - INTERVAL '1 hour')
AND amphtd.chat_finished_at <  (tdd.session_end + INTERVAL '1 hour')
AND amphtd.user_id <> 129
AND (
  su.dismissed_at >= %(end_date)s
  OR su.dismissed_at IS NULL
)
AND amphtd.project_id = ANY(%(project_ids)s::int[])
{pst_positions_filter}
{excluded_chats_filter}
GROUP BY 1, 2, 3
ORDER BY 1;