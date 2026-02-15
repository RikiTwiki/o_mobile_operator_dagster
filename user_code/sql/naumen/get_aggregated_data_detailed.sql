-- AHT per period and project (Money/Bank collapsed to O!Bank)
SELECT
    to_char(date_trunc(%s::text, hour), %s) AS date,
    CASE WHEN "group" IN ('Money','Bank') THEN 'O!Bank' ELSE "group" END AS project_key,
    (SUM(sum_pickup_time) + SUM(sum_speaking_time) + SUM(sum_wrapup_time))::numeric
        / NULLIF(SUM(quantity),0) AS average_handling_time
FROM stat.replication_naumen_handling_time_data
WHERE hour >= %s::timestamp
  AND hour <  %s::timestamp
  /*WHERE_PROJECTS_CLAUSE*/
GROUP BY 1, 2;