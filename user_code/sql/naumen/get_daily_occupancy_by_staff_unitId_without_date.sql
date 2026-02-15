WITH sc AS (
    SELECT
        status,
        SUM(EXTRACT(EPOCH FROM (leaved - entered)))::bigint AS duration
    FROM status_changes_ms
    WHERE entered >= %(start_date)s
      AND entered  < %(end_date)s
      AND login = %(login)s
      {excluded_clause}
      AND status = ANY(%(all_statuses)s::text[])
    GROUP BY status
),
cs AS (
    SELECT
        state AS status,
        SUM(EXTRACT(EPOCH FROM (ended - entered)))::bigint AS duration
    FROM call_status
    WHERE entered >= %(start_date)s
      AND entered  < %(end_date)s
      AND initiator_id = %(login)s
      {excluded_clause}
    GROUP BY state
)
SELECT status, duration FROM sc
UNION ALL
SELECT status, duration FROM cs