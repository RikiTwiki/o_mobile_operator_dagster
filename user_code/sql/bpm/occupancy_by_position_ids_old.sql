SELECT DISTINCT u.login
FROM bpm.staff_units AS su
JOIN bpm.users       AS u ON u.id = su.user_id
WHERE su.user_id IS NOT NULL
  AND su.position_id = ANY(%(position_ids)s::int[])
  AND su.accepted_at < %(end_date)s::date
  AND (su.dismissed_at >= %(end_date)s::date OR su.dismissed_at IS NULL)
ORDER BY u.login;