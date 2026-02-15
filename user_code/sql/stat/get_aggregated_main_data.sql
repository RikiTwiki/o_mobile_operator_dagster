SELECT
    to_char(date_trunc(%(trunc)s::text, hour), %(date_format)s) AS date,
    sum(total)                          AS total,
    sum(ivr)                            AS ivr,
    sum(redirected)                     AS redirected,
    sum(call_project_changed)           AS call_project_changed,
    sum(queue)                          AS queue,
    sum(threshold_queue)                AS threshold_queue,
    sum(threshold_queue) + sum(queue)   AS total_queue,
    sum(lost)                           AS lost,
    sum(callback_success)               AS callback_success,
    sum(callback_disabled)              AS callback_disabled,
    sum(callback_unsuccessful)          AS callback_unsuccessful,
    sum(total_to_operators)             AS total_to_operators,
    sum(answered)                       AS answered,
    sum(sla_answered)                   AS sla_answered,
    sum(sla_total)                      AS sla_total,
    sum(summary_waiting_time)::numeric  AS summary_waiting_time,
    min(minimum_waiting_time)::numeric  AS minimum_waiting_time,
    max(maximum_waiting_time)::numeric  AS maximum_waiting_time
FROM stat.replication_naumen_service_level_data
WHERE
    hour >= %(start_date)s::timestamp
    AND hour <  %(end_date)s::timestamp
{selected_filter}
{excluded_days_filter}
GROUP BY 1
ORDER BY 1;
