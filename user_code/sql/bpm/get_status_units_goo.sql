SELECT u.login, d.name AS department
FROM staff_units su
JOIN users u ON u.id = su.user_id
JOIN positions p ON p.id = su.position_id
JOIN departments d ON d.id = p.department_id
WHERE su.position_id = ANY(%(position_ids)s)
AND (
    (su.accepted_at < %(start_date)s AND su.dismissed_at >= %(end_date)s)
 OR (su.accepted_at < %(end_date)s   AND su.dismissed_at IS NULL)
)
