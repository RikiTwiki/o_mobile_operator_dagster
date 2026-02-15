-- Main AHT aggregation per period
SELECT
    to_char(date_trunc(%s::text, hour), %s)                      AS date,
    SUM(quantity)                                                AS quantity,
    SUM(sum_pickup_time)                                         AS sum_pickup_time,
    MIN(min_pickup_time)                                         AS min_pickup_time,
    MAX(max_pickup_time)                                         AS max_pickup_time,
    SUM(sum_speaking_time)                                       AS sum_speaking_time,
    MIN(min_speaking_time)                                       AS min_speaking_time,
    MAX(max_speaking_time)                                       AS max_speaking_time,
    SUM(sum_wrapup_time)                                         AS sum_wrapup_time,
    MIN(min_wrapup_time)                                         AS min_wrapup_time,
    MAX(max_wrapup_time)                                         AS max_wrapup_time,
    SUM(quantity_hold)                                           AS quantity_hold,
    SUM(sum_holding_time)                                        AS sum_holding_time,
    MIN(min_holding_time)                                        AS min_holding_time,
    MAX(max_holding_time)                                        AS max_holding_time,
    (SUM(sum_pickup_time) + SUM(sum_speaking_time) + SUM(sum_wrapup_time)) AS sum_handling_time,
    (SUM(sum_pickup_time)::numeric / NULLIF(SUM(quantity),0))    AS average_ringing_time,
    (SUM(sum_holding_time)::numeric / NULLIF(SUM(quantity_hold),0)) AS average_holding_time,
    ((SUM(sum_pickup_time) + SUM(sum_speaking_time) + SUM(sum_wrapup_time))::numeric
        / NULLIF(SUM(quantity),0))                               AS average_handling_time,
    (SUM(sum_speaking_time)::numeric / NULLIF(SUM(quantity),0))  AS average_speaking_time
FROM stat.replication_naumen_handling_time_data
WHERE hour >= %s::timestamp
  AND hour <  %s::timestamp
  /*WHERE_PROJECTS_CLAUSE*/
GROUP BY 1;