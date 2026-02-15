-- Average handling-time by staff unit with project exclusions (<> ALL)
SELECT
    staff_unit_id,
    COALESCE(user_login, '') AS user_login,
    SUM(quantity)                 AS quantity,
    SUM(sum_holding_time)         AS sum_holding_time,
    SUM(sum_pickup_time)          AS sum_pickup_time,
    SUM(sum_speaking_time)        AS sum_speaking_time,
    SUM(sum_wrapup_time)          AS sum_wrapup_time,
    MAX(max_pickup_time)          AS max_pickup_time,
    MAX(max_speaking_time)        AS max_speaking_time,
    MAX(max_wrapup_time)          AS max_wrapup_time,
    MAX(max_holding_time)         AS max_holding_time
FROM stat.replication_naumen_handling_time_data
WHERE hour >= %s
  AND hour <  %s
  AND language <> 'UZ'
  AND "group" <> ALL(%s)  -- pass array: ['Telesales','Money','Saima']
  AND quantity IS NOT NULL
GROUP BY staff_unit_id, user_login;