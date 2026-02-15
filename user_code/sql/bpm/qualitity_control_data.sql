SELECT
    SUM(CASE WHEN has_error = TRUE THEN 1 ELSE 0 END) AS mistakes,
    COUNT(session_id) AS all_listened
FROM bpm.daily_control
WHERE staff_unit_id = %(staff_unit_id)s
  AND created_at >= %(start_date)s
  AND created_at < %(end_date)s;
