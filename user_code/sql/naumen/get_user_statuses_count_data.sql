WITH base AS (
    SELECT
        login,
        date_trunc('day', entered) AS date,
        status,
        COUNT(status) AS count
    FROM status_changes_ms
    WHERE entered >= %(start_date)s
      AND entered <  %(end_date)s
    GROUP BY 1, 2, 3
),
away_reason AS (
    SELECT
        login,
        date_trunc('day', entered) AS date,
        CONCAT(status, ' - ', reason) AS status,
        COUNT(status) AS count
    FROM status_changes_ms
    WHERE entered >= %(start_date)s
      AND entered <  %(end_date)s
      AND status = 'away'
      AND reason IS NOT NULL
    GROUP BY 1, 2, 3
)
SELECT login, date, status, count FROM base
UNION
SELECT login, date, status, count FROM away_reason;