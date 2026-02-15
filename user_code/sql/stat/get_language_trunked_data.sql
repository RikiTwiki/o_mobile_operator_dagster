SELECT
  "date",
  "language" AS language,
  SUM(total)                               AS total,
  CASE
    WHEN SUM(sla_total::numeric) = 0 THEN NULL
    ELSE ROUND(SUM(sla_answered::numeric) / SUM(sla_total::numeric) * 100)
  END                                      AS service_level,
  CASE
    WHEN SUM(total_to_operators::numeric) = 0 THEN NULL
    ELSE ROUND(SUM(answered::numeric) / SUM(total_to_operators::numeric) * 100)
  END                                      AS answered_calls_rate
FROM stat.replication_naumen_service_level_data
WHERE "date" >= %(start_date)s::timestamp
  AND "date"  < %(end_date)s::timestamp
  AND project_id::text = ANY(%(project_ids)s::text[])
GROUP BY 1, 2
ORDER BY 1;