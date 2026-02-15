SELECT
    tws.title AS service,
    tw.description AS description,
    CASE
        WHEN tw.ended_at IS NULL
            THEN TO_CHAR(NOW() - tw.started_at, 'DDд. HH24ч. MIм.')
        ELSE TO_CHAR(tw.ended_at - tw.started_at, 'DDд. HH24ч. MIм.')
    END AS downtime,
    tw.started_at AS date_begin,
    tw.ended_at AS date_end
FROM bpm.technical_works AS tw
LEFT JOIN bpm.technical_work_services tws
       ON tw.service_id = tws.id
WHERE (tw.started_at >= %(start_date)s AND tw.started_at < %(end_date)s)
  AND tws.group_id = ANY(%(groups)s::int[])
   OR (tw.ended_at IS NULL AND tws.group_id = ANY(%(groups)s::int[]))
ORDER BY tw.started_at;
