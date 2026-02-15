 SELECT data
FROM bpm.timetable_data
WHERE timetable_view_id = ANY(%(view_ids)s::int[])
  AND month::date = %(report_month)s::date
  AND active = TRUE