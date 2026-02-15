WITH _p AS (
  SELECT %s::timestamptz AS start_date, %s::timestamptz AS end_date
),
base AS (
  SELECT
    CASE
      WHEN f.jsondata->'telesales_upsales'->>'imp_tariff' ILIKE '%%540 +%%'    THEN '540+'
      WHEN f.jsondata->'telesales_upsales'->>'imp_tariff' ILIKE '%%490 +%%'    THEN '490+'
      WHEN f.jsondata->'telesales_upsales'->>'imp_tariff' ILIKE '%%395 +%%'    THEN '395+'
      WHEN f.jsondata->'telesales_upsales'->>'imp_tariff' ILIKE '%%195 PRO%%'  THEN '195PRO'
      WHEN f.jsondata->'telesales_upsales'->>'imp_tariff' ILIKE '%%160 PRO+%%' THEN '160PRO+'
      WHEN f.jsondata->'telesales_upsales'->>'imp_tariff' ILIKE '%%160 PRO%%'  THEN '160PRO'
      WHEN f.jsondata->'telesales_upsales'->>'imp_tariff' ILIKE '%%145 +%%'    THEN '145+'
      WHEN f.jsondata->'telesales_upsales'->>'imp_tariff' ILIKE '%%Комбо%%'    THEN 'Комбо Макс'
      WHEN f.jsondata->'telesales_upsales'->>'imp_tariff' ILIKE '%%Premium%%'  THEN 'Premium'
      WHEN f.jsondata->'telesales_upsales'->>'imp_tariff' ILIKE '%%VIP%%'      THEN 'VIP'
      WHEN f.jsondata->'telesales_upsales'->>'imp_tariff' ILIKE 'ПнО! Регион + %%' THEN 'Регион+'
      WHEN f.jsondata->'telesales_upsales'->>'imp_tariff' ILIKE 'ПнО! Регион +%%'  THEN 'Регион+'
      WHEN f.jsondata->'telesales_upsales'->>'imp_tariff' ILIKE 'ПнО! Регион'     THEN 'Регион'
      WHEN f.jsondata->'telesales_upsales'->>'imp_tariff' ILIKE 'ПнО! Талас + %%'  THEN 'Талас+'
      WHEN f.jsondata->'telesales_upsales'->>'imp_tariff' ILIKE 'ПнО! Талас%%'     THEN 'Талас'
      WHEN f.jsondata->'telesales_upsales'->>'imp_tariff' ILIKE 'ПнО! Юг +%%'      THEN 'Юг+'
      WHEN f.jsondata->'telesales_upsales'->>'imp_tariff' ILIKE 'ПнО! Юг'         THEN 'Юг'
      WHEN f.jsondata->'telesales_upsales'->>'imp_tariff' ILIKE '%%Специальный%%' THEN 'Специальный'
      WHEN f.jsondata->'telesales_upsales'->>'imp_tariff' ILIKE '%%Samiy Perviy%%' THEN 'Samiy Perviy'
      WHEN f.jsondata->'telesales_upsales'->>'imp_tariff' ILIKE 'ПнО! Нарын +%%'  THEN 'Нарын+'
      WHEN f.jsondata->'telesales_upsales'->>'imp_tariff' ILIKE 'ПнО! Нарын'     THEN 'Нарын'
      WHEN f.jsondata->'telesales_upsales'->>'imp_tariff' ILIKE 'ПнО! Иссык-Куль + %%' THEN 'Иссык-Куль+'
      WHEN f.jsondata->'telesales_upsales'->>'imp_tariff' ILIKE 'ПнО! Иссык-Куль' THEN 'Иссык-Куль'
      WHEN f.jsondata->'telesales_upsales'->>'imp_tariff' ILIKE '%%Интернет%%'   THEN 'Интернет'
      WHEN f.jsondata->'telesales_upsales'->>'imp_tariff' ILIKE 'Переходи на О! Регион 135 + O!TV' THEN 'Регион135+'
      ELSE 'Other'
    END AS "KEY",
    (f.jsondata->'telesales_upsales'->>'imp_tariff')      AS imp_tariff,
    (f.jsondata->'telesales_upsales'->>'qwes_recomendet') AS qwes_recomendet,
    COALESCE(dos.speaking_time, 0) AS speaking_time
  FROM detail_outbound_sessions_ms AS dos
  JOIN mv_call_case AS c ON c.uuid = dos.case_uuid
  LEFT JOIN mv_custom_form AS f ON f.owneruuid = dos.case_uuid AND f.removed = false
  WHERE dos.project_id = 'corebo00000000000n9e9ma1906197mg'
    AND (f.jsondata->'telesales_upsales'->>'qwes_recomendet') IS NOT NULL
    AND dos.attempt_start >= (SELECT start_date FROM _p)
    AND dos.attempt_start <  (SELECT end_date   FROM _p)
),
norm AS (
  SELECT
    "KEY",
    imp_tariff,
    qwes_recomendet,
    REGEXP_REPLACE(
      REGEXP_REPLACE(qwes_recomendet, E'\\\\([0-9]+).*$', ' (\\1)'),
      E'\\s+и выше\\s*$', ''
    ) AS qwes_normalized,
    speaking_time
  FROM base
),
agg AS (  -- агрегат по варианту рекомендации
  SELECT
    "KEY",
    imp_tariff,
    qwes_recomendet,
    qwes_normalized,
    SUM(CASE WHEN speaking_time > 5000 THEN 1 ELSE 0 END) AS more5_sec,
    COUNT(*) AS total
  FROM norm
  GROUP BY 1,2,3,4
),
scored AS (  -- приоритеты "как в PHP-дампе"
  SELECT
    a.*,
    CASE
      WHEN a."KEY" IN ('540+','490+','395+') THEN
        CASE
          WHEN a.qwes_recomendet ILIKE '%%Комбо Макс%%и выше%%' THEN 0
          WHEN a.qwes_recomendet ILIKE '%%Комбо Макс%%'        THEN 1
          ELSE 2
        END
      WHEN a."KEY" IN ('145+','160PRO','160PRO+') THEN
        CASE
          WHEN a.qwes_recomendet ILIKE '%%195 + O!TV%%' THEN 0
          WHEN a.qwes_recomendet ILIKE '%%160 + O!TV%%' THEN 1
          ELSE 2
        END
      WHEN a."KEY" IN ('Регион','Регион+','Регион135+','Юг','Юг+','Нарын','Нарын+','Иссык-Куль','Иссык-Куль+','Талас','Талас+') THEN
        CASE
          WHEN a.qwes_recomendet ILIKE '%%195 + O!TV%%' THEN 0
          WHEN a.qwes_recomendet ILIKE '%%160 + O!TV%%' THEN 1
          ELSE 2
        END
      ELSE 2
    END AS pri
  FROM agg a
),
ranked AS (
  SELECT s.*,
         ROW_NUMBER() OVER (PARTITION BY "KEY" ORDER BY pri, qwes_recomendet) AS rn
  FROM scored s
)
SELECT
  "KEY",
  imp_tariff,
  qwes_recomendet,
  qwes_normalized,
  more5_sec,
  total
FROM ranked
WHERE rn = 1;