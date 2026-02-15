SELECT
    to_char(date_trunc(%(trunc)s, omni_inbound_subscribers_requests.request_ended_at), %(date_format)s) as date,
    COUNT(omni_inbound_subscribers_requests.session_id) as all_contacts,
    SUM(CASE WHEN aqerd.mark::numeric IS NOT NULL THEN 1 ELSE 0 END) AS with_marks,
    SUM(CASE WHEN aqerd.mark::numeric IN (4, 5) THEN 1 ELSE 0 END) AS good_marks,
    SUM(CASE WHEN aqerd.mark::numeric IN (3) THEN 1 ELSE 0 END) AS neutral_marks,
    SUM(CASE WHEN aqerd.mark::numeric IN (0, 1, 2) THEN 1 ELSE 0 END) AS bad_marks
FROM stat.omni_inbound_subscribers_requests
LEFT JOIN stat.aggregation_questionnaire_employee_rating_data aqerd
    ON aqerd.contact_identifier = omni_inbound_subscribers_requests.session_id
    AND aqerd.user_login = omni_inbound_subscribers_requests.staff_unit_login
WHERE omni_inbound_subscribers_requests.request_ended_at >= %(start_date)s
  AND omni_inbound_subscribers_requests.request_ended_at < %(end_date)s
  AND omni_inbound_subscribers_requests.staff_unit_login = %(user_login)s
GROUP BY 1
ORDER BY 1;