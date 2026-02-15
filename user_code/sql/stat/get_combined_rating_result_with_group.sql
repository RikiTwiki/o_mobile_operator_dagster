SELECT
    to_char(date_trunc(%(trunc)s, omni_inbound_subscribers_requests.request_started_at), %(date_format)s) as {date_field},
    group_title,
    COUNT(omni_inbound_subscribers_requests.session_id) as all_contacts,
    SUM(CASE WHEN rsertd.mark::integer IS NOT NULL THEN 1 ELSE 0 END)
    + SUM(CASE WHEN aqerd.mark::integer IS NOT NULL THEN 1 ELSE 0 END) AS total_marks,
    SUM(CASE WHEN rsertd.mark::integer IN (4, 5) THEN 1 ELSE 0 END)
    + SUM(CASE WHEN aqerd.mark::integer IN (4, 5) THEN 1 ELSE 0 END) AS good_marks,
    SUM(CASE WHEN rsertd.mark::integer IN (3) THEN 1 ELSE 0 END)
    + SUM(CASE WHEN aqerd.mark::integer IN (3) THEN 1 ELSE 0 END) AS neutral_marks,
    SUM(CASE WHEN rsertd.mark::integer IN (0, 1, 2) THEN 1 ELSE 0 END)
    + SUM(CASE WHEN aqerd.mark::integer IN (0, 1, 2) THEN 1 ELSE 0 END) AS bad_marks
FROM stat.omni_inbound_subscribers_requests
LEFT JOIN stat.replication_slr_employees_rating_data rsertd
    ON rsertd.session_id = omni_inbound_subscribers_requests.session_id
    AND rsertd.user_login = omni_inbound_subscribers_requests.staff_unit_login
LEFT JOIN stat.aggregation_questionnaire_employee_rating_data aqerd
    ON aqerd.contact_identifier = omni_inbound_subscribers_requests.session_id
    AND aqerd.user_login = omni_inbound_subscribers_requests.staff_unit_login
WHERE request_ended_at BETWEEN %(start_date)s AND %(end_date)s
  AND group_title = ANY(%(group)s::text[])
  AND channel_title = ANY(%(channel)s::text[])
  AND direction IN ('inbound', 'in')
GROUP BY 1, 2
ORDER BY 1, 2;