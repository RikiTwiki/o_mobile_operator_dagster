SELECT
    to_char(
        date_trunc(%(trunc)s::text, oisr.request_ended_at),
        %(date_format)s
    ) AS date,
    COUNT(oisr.session_id) AS all_contacts,
    SUM(CASE WHEN rsertd.mark IS NOT NULL THEN 1 ELSE 0 END) AS with_marks,
    SUM(CASE WHEN rsertd.mark IN (4, 5) THEN 1 ELSE 0 END)   AS good_marks,
    SUM(CASE WHEN rsertd.mark IN (3)     THEN 1 ELSE 0 END)   AS neutral_marks,
    SUM(CASE WHEN rsertd.mark IN (0,1,2) THEN 1 ELSE 0 END)   AS bad_marks
FROM stat.omni_inbound_subscribers_requests AS oisr
LEFT JOIN stat.replication_slr_employees_rating_data AS rsertd
  ON oisr.session_id      = rsertd.session_id
 AND oisr.staff_unit_login = rsertd.user_login
WHERE oisr.request_ended_at >= %(start_date)s
  AND oisr.request_ended_at <  %(end_date)s
  AND oisr.direction = ANY(%(directions)s::text[])
  AND oisr.project_id = ANY(%(selected_projects)s::int[])
GROUP BY 1;
