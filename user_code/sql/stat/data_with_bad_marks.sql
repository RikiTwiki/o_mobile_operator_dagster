SELECT
    to_char(date_trunc(%(trunc)s::text, sird.request_ended_at), %(date_format)s) AS date,
    pp.title AS price_plane_title,
    COUNT(rserd.mark) AS total
FROM stat.omni_inbound_subscribers_requests AS sird
LEFT JOIN stat.replication_slr_employees_rating_data AS rserd
  ON rserd.session_id = sird.session_id
 AND rserd.user_login = sird.staff_unit_login
LEFT JOIN stat.subscribers_data AS sd
  ON sd.session_id = sird.session_id
 AND sd.param = 'price_plan'
LEFT JOIN bpm.price_plans AS pp
  ON pp.dwh_id::text = sd.value
WHERE sird.direction = 'inbound'
  AND sird.request_ended_at >= %(start_date)s
  AND sird.request_ended_at <  %(end_date)s
  AND rserd.mark IN (0, 1, 2)
GROUP BY 1, 2
ORDER BY 1;
