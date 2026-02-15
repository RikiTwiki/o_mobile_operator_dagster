SELECT
  usc.user_id,
  usc.started_at  + INTERVAL '6 HOUR' AS entered,
  usc.finished_at + INTERVAL '6 HOUR' AS leaved,
  COALESCE(us.params->>'code', us.title) AS status,
  SUM(
    COALESCE(
      usc.duration,
      EXTRACT(EPOCH FROM (COALESCE(usc.finished_at, NOW()) - COALESCE(usc.started_at, NOW())))
    )
  ) AS duration
FROM cp.user_status_changes AS usc
JOIN cp.user_statuses        AS us ON us.id = usc.status_id
WHERE (usc.started_at + INTERVAL '6 HOUR') >= %(start_date)s
  AND (usc.started_at + INTERVAL '6 HOUR') <  %(end_date)s
  AND (
    us.title IN ('normal','eshop','pre_coffee','social_media')
    OR (us.params->>'code') IN ('normal','eshop','pre_coffee','social_media')
  )
GROUP BY 1,2,3,4
ORDER BY 1;