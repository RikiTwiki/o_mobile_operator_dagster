-- ARPU, avg(max_pickup_time) by date_trunc(trunc, "date")
SELECT
    date_trunc(%s, "date") AS date,
    arpu,
    AVG(max_pickup_time)    AS max_pickup_time
FROM stat.replication_naumen_handling_time_data
WHERE "date" >= %s
  AND "date" <  %s
  /*WHERE_PROJECTS_CLAUSE*/
GROUP BY 1, 2
ORDER BY 1;