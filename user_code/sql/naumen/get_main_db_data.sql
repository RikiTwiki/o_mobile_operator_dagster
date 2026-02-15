WITH status_changes_agg AS (
    SELECT
        to_char(date_trunc(%(trunc)s::text, entered), %(date_format)s) AS date,
        CASE
            WHEN entered >= '2025-11-26 20:30:00'::timestamp
             AND entered <= '2025-11-27 17:00:00'::timestamp
             AND duration <= 120000
             AND status = 'dnd' THEN 'wrapup'
            ELSE status
        END AS status,
        SUM(intervaltosec(leaved - entered)) AS duration
    FROM status_changes_ms
    WHERE entered >= %(start_date)s
      AND entered <  %(end_date)s
      {excluded_days_filter}
      AND login = ANY(%(logins)s::text[])
      AND status IN (
          'normal','ringing','speaking','wrapup','standoff','away',
          'dnd','accident','custom1','custom2','custom3'
      )
    GROUP BY 1, 2
),
call_status_agg AS (
    SELECT
        to_char(date_trunc(%(trunc)s::text, entered), %(date_format)s) AS date,
        state AS status,
        SUM(intervaltosec(ended - entered)) AS duration
    FROM call_status
    WHERE entered >= %(start_date)s
      AND entered <  %(end_date)s
      {excluded_days_filter}
      AND initiator_id = ANY(%(logins)s::text[])
    GROUP BY 1, 2
)
SELECT date, status, duration FROM call_status_agg
UNION
SELECT date, status, duration FROM status_changes_agg
ORDER BY 1, 2;