SELECT DISTINCT u.login
FROM bpm.staff_units AS su
JOIN bpm.users AS u ON u.id = su.user_id
WHERE
  su.accepted_at::date <= %(end_date)s
  AND (su.dismissed_at IS NULL OR su.dismissed_at::date > %(start_date)s)
  AND (
        (su.accepted_at::date < %(start_date)s AND su.dismissed_at::date >= %(end_date)s)
     OR (su.accepted_at::date < %(end_date)s  AND su.dismissed_at IS NULL)
      )
  AND su.position_id = ANY(ARRAY[35,36,37,39]::int[])
ORDER BY u.login;