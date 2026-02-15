SELECT
    tws.title AS service,
    tw.description AS description,
    COUNT(twi.technical_work_id) AS technical_work_count,
    CASE
        WHEN tw.ended_at IS NULL
            THEN TO_CHAR(NOW() - tw.started_at, 'DDд. HH24ч. MIм.')
        ELSE TO_CHAR(tw.ended_at - tw.started_at, 'DDд. HH24ч. MIм.')
    END AS downtime,
    tw.started_at AS date_begin,
    tw.ended_at AS date_end
FROM bpm.technical_works AS tw
LEFT JOIN bpm.technical_work_impacts twi
       ON tw.id = twi.technical_work_id
LEFT JOIN bpm.technical_work_services tws
       ON tw.service_id = tws.id
WHERE (tw.started_at >= %(start_date)s AND tw.started_at < %(end_date)s)
  AND tw.service_id = ANY(%(services)s::int[])
   OR (tw.ended_at IS NULL AND tw.service_id = ANY(%(services)s::int[]))
GROUP BY 1, 2, 5, 6
ORDER BY tw.started_at;
