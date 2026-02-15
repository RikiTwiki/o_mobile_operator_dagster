WITH base AS (
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
        COALESCE(usc.started_at, usc.created_at)                AS s_local,
        COALESCE(usc.finished_at, %(end_date)s::timestamp)      AS f_local
    FROM cp.user_status_changes AS usc
    JOIN cp.users           AS u  ON u.id = usc.user_id
    LEFT JOIN cp.user_statuses AS us ON us.id = usc.status_id
    WHERE COALESCE(usc.started_at, usc.created_at) < %(end_date)s::timestamp
      AND COALESCE(usc.finished_at, %(end_date)s::timestamp) > %(start_date)s::timestamp
      AND u.login = ANY(%(logins)s::text[])
      AND usc.status_id <> 2  -- исключаем offline
),
days AS (
    SELECT
        gs::date AS day
    FROM generate_series(
        date_trunc('day', %(start_date)s::timestamp),
        date_trunc('day', %(end_date)s::timestamp) - INTERVAL '1 day',
        INTERVAL '1 day'
    ) AS gs
)
SELECT
    t.date,
    t.login,
    t.code,
    SUM(t.seconds_piece)::bigint              AS seconds,
    ROUND(SUM(t.seconds_piece) / 60.0, 2)     AS minutes,
    ROUND(SUM(t.seconds_piece) / 3600.0, 3)   AS hours
FROM (
    SELECT
        date_trunc('day', d.day)::date AS date,
        b.login,
        b.code,
        GREATEST(
            0,
            EXTRACT(
                EPOCH FROM (
                    LEAST(b.f_local, d.day + INTERVAL '1 day', %(end_date)s::timestamp)
                    - GREATEST(b.s_local, d.day, %(start_date)s::timestamp)
                )
            )
        ) AS seconds_piece
    FROM base b
    CROSS JOIN days d
    WHERE LEAST(b.f_local, d.day + INTERVAL '1 day', %(end_date)s::timestamp)
          > GREATEST(b.s_local, d.day, %(start_date)s::timestamp)
) AS t
GROUP BY 1, 2, 3
ORDER BY 2, 1;