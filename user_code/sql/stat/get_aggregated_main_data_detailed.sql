SELECT to_char(date_trunc(%(trunc)s::text, hour), %(date_format)s) AS date,
CASE
    WHEN "group" IN ('Money','Bank') THEN 'O!Bank'
    ELSE "group"
  END AS project_key,
  SUM(total)              AS total,
  SUM(ivr)                AS ivr,
  SUM(sla_answered)       AS sla_answered,
  SUM(sla_total)          AS sla_total,
  SUM(total_to_operators) AS total_to_operators,
  SUM(answered)           AS answered
FROM stat.replication_naumen_service_level_data
WHERE hour >= %(start_date)s::timestamp
      AND hour <  %(end_date)s::timestamp
{selected_filter}
{excluded_days_filter}
GROUP BY 1, 2
ORDER BY 1, 2;

