WITH d AS (
    SELECT
        TO_CHAR(TRUNC(ACTION_DATE), 'YYYY-MM-DD') AS "day",
        CASE
            WHEN TOTAL_CHARGE_30_DAYS_BFR >= 0   AND TOTAL_CHARGE_30_DAYS_BFR <= 15  THEN 'VERY LOW'
            WHEN TOTAL_CHARGE_30_DAYS_BFR > 15   AND TOTAL_CHARGE_30_DAYS_BFR <= 100 THEN 'LOW'
            WHEN TOTAL_CHARGE_30_DAYS_BFR > 100  AND TOTAL_CHARGE_30_DAYS_BFR <= 250 THEN 'MIDDLE'
            WHEN TOTAL_CHARGE_30_DAYS_BFR > 250  AND TOTAL_CHARGE_30_DAYS_BFR <= 500 THEN 'HIGH'
            WHEN TOTAL_CHARGE_30_DAYS_BFR > 500                                        THEN 'VERY HIGH'
            ELSE 'НЕИЗВЕСТНО'
        END AS "name",
        ROUND(SUM(RENEWAL_QNT)) AS "value"
    FROM KPI.COM_ORW_REWARD_DETAILED_GTM_MW
    WHERE TRUNC(ACTION_DATE) >= TO_DATE(:start_date, 'YYYY-MM-DD')
      AND TRUNC(ACTION_DATE) <  TO_DATE(:end_date,   'YYYY-MM-DD')
    GROUP BY
        TO_CHAR(TRUNC(ACTION_DATE), 'YYYY-MM-DD'),
        CASE
            WHEN TOTAL_CHARGE_30_DAYS_BFR >= 0   AND TOTAL_CHARGE_30_DAYS_BFR <= 15  THEN 'VERY LOW'
            WHEN TOTAL_CHARGE_30_DAYS_BFR > 15   AND TOTAL_CHARGE_30_DAYS_BFR <= 100 THEN 'LOW'
            WHEN TOTAL_CHARGE_30_DAYS_BFR > 100  AND TOTAL_CHARGE_30_DAYS_BFR <= 250 THEN 'MIDDLE'
            WHEN TOTAL_CHARGE_30_DAYS_BFR > 250  AND TOTAL_CHARGE_30_DAYS_BFR <= 500 THEN 'HIGH'
            WHEN TOTAL_CHARGE_30_DAYS_BFR > 500                                        THEN 'VERY HIGH'
            ELSE 'НЕИЗВЕСТНО'
        END
)
SELECT d."day", d."name", SUM(d."value") AS "value"
FROM (
    SELECT "day", "name", "value"
    FROM d
    UNION ALL
    -- генерируем полную матрицу (все day × name), для отсутствующих пар value будет NULL
    SELECT dd."day", nn."name", NULL AS "value"
    FROM (SELECT "day" FROM d GROUP BY "day") dd,
         (SELECT "name" FROM d GROUP BY "name") nn
) d
GROUP BY d."day", d."name"
ORDER BY 1