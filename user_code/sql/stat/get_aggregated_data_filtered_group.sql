SELECT
  to_char(date_trunc(%(trunc)s::text, hour), %(date_format)s) AS date,
  "group" AS project,
  SUM(total)                AS total,
  SUM(ivr)                  AS ivr,
  SUM(redirected)           AS redirected,
  SUM(call_project_changed) AS call_project_changed,
  SUM(queue)                AS queue,
  SUM(threshold_queue)      AS threshold_queue,
  (SUM(threshold_queue) + SUM(queue)) AS total_queue,
  SUM(lost)                 AS lost,
  SUM(callback_success)     AS callback_success,
  SUM(callback_disabled)    AS callback_disabled,
  SUM(callback_unsuccessful) AS callback_unsuccessful,
  SUM(total_to_operators)   AS total_to_operators,
  SUM(answered)             AS answered,
  SUM(sla_answered)         AS sla_answered,
  SUM(sla_total)            AS sla_total,
  SUM(summary_waiting_time)::numeric AS summary_waiting_time,
  MIN(minimum_waiting_time)::numeric AS minimum_waiting_time,
  MAX(maximum_waiting_time)::numeric AS maximum_waiting_time
FROM stat.replication_naumen_service_level_data
WHERE hour >= %(start_date)s::timestamp
  AND hour  < %(end_date)s::timestamp
{selected_filter}
{excluded_days_filter}
GROUP BY 1, 2
ORDER BY 1, 2;