WITH bounds AS (
    SELECT
        %(date)s::date                           AS day_start,
        (%(date)s::date + INTERVAL '1 day')      AS day_end
),
raw AS (
    SELECT
        u.login,
        CASE
            WHEN usc.status_id = 1  THEN 'normal'
            WHEN usc.status_id = 2  THEN 'offline'
            WHEN usc.status_id = 3  THEN 'absent'
            WHEN usc.status_id = 5  THEN 'lunch'
            WHEN usc.status_id = 6  THEN 'coffee'
            WHEN usc.status_id = 11 THEN 'pre_coffee'
            WHEN usc.status_id = 12 THEN 'social_media'
            WHEN usc.status_id = 13 THEN 'eshop'
            WHEN usc.status_id = 10 THEN 'dnd'
            WHEN usc.status_id = 7  THEN 'ringing'
            WHEN usc.status_id = 8  THEN 'speaking'
            WHEN usc.status_id = 9  THEN 'accident'
            ELSE LOWER(regexp_replace(us.title, '\s+', '_', 'g'))
        END AS code,
        COALESCE(usc.started_at, usc.created_at)                 AS s_local,
        COALESCE(usc.finished_at, b.day_end)                     AS f_local,
        b.day_start,
        b.day_end
    FROM cp.user_status_changes AS usc
    JOIN cp.users            AS u  ON u.id = usc.user_id
    LEFT JOIN cp.user_statuses AS us ON us.id = usc.status_id
    CROSS JOIN bounds     AS b
    WHERE u.login = %(login)s
      AND usc.status_id <> 2                         -- исключаем offline
      -- есть пересечение интервала статуса с сутками:
      AND COALESCE(usc.started_at, usc.created_at) < b.day_end
      AND COALESCE(usc.finished_at, b.day_end) > b.day_start
),
agg AS (
    SELECT
        r.day_start::date AS date,
        r.login,
        r.code,
        GREATEST(
            0,
            EXTRACT(
                EPOCH FROM (
                    LEAST(r.f_local, r.day_end)
                    - GREATEST(r.s_local, r.day_start)
                )
            )
        ) AS seconds_piece
    FROM raw r
)
SELECT
    date,
    login,
    code,
    SUM(seconds_piece)::bigint                                   AS seconds,
    ROUND(SUM(seconds_piece) / 60.0, 2)                          AS minutes,
    ROUND(SUM(seconds_piece) / 3600.0, 3)                        AS hours
FROM agg
GROUP BY 1, 2, 3
ORDER BY 1;