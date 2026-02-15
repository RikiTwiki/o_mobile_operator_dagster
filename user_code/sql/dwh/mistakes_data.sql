WITH d AS (
    SELECT
        TO_CHAR(TRUNC(ACTION_DATE), 'YYYY-MM-DD') AS "day",
        CASE
            WHEN USED_IN_REPORT = 13 THEN 'Абоненту подключили услугу с пониженной или равной стоимостью(приоритетом)'
            WHEN USED_IN_REPORT = 23 THEN 'Нет оплат после подключения услуги'
            ELSE 'Иные ошибки'
        END AS "name",
        COUNT(USED_IN_REPORT) AS "value"
    FROM KPI.COM_ORW_REWARD_DETAILED_GTM_MW
    WHERE TRUNC(ACTION_DATE) >= TO_DATE(:start_date, 'YYYY-MM-DD')
      AND TRUNC(ACTION_DATE) <  TO_DATE(:end_date,   'YYYY-MM-DD')
    GROUP BY
        TO_CHAR(TRUNC(ACTION_DATE), 'YYYY-MM-DD'),
        CASE
            WHEN USED_IN_REPORT = 13 THEN 'Абоненту подключили услугу с пониженной или равной стоимостью(приоритетом)'
            WHEN USED_IN_REPORT = 23 THEN 'Нет оплат после подключения услуги'
            ELSE 'Иные ошибки'
        END
)
SELECT d."day", d."name", SUM(d."value") AS "value"
FROM (
    SELECT "day", "name", "value" FROM d
    UNION ALL
    SELECT dd."day", nn."name", NULL AS "value"
    FROM (SELECT "day"  FROM d GROUP BY "day")  dd,
         (SELECT "name" FROM d GROUP BY "name") nn
) d
GROUP BY d."day", d."name"
ORDER BY 1