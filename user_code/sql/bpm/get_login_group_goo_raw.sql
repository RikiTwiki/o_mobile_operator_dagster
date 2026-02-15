SELECT DISTINCT ON (u.login)
       u.login,
       d.id            AS department_id,
       d.title         AS department_title,
       d.abbreviation  AS department_abbreviation
FROM bpm.staff_units su
JOIN bpm.users       u ON u.id = su.user_id
JOIN bpm.positions   p ON p.id = su.position_id
JOIN bpm.departments d ON d.id = p.department_id
WHERE su.position_id = ANY(%(position_ids)s::int[])
  AND (
        (su.accepted_at < %(start_date)s::date AND su.dismissed_at >= %(end_date)s::date)
        OR
        (su.accepted_at < %(end_date)s::date AND su.dismissed_at IS NULL)
      )
ORDER BY u.login, su.accepted_at DESC, su.dismissed_at NULLS LAST;