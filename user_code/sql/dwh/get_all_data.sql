SELECT
  CASE
    WHEN REGEXP_LIKE(PREVIOUS_SERVICE_NAME, 'АнтиАОН', 'i') THEN PREVIOUS_SERVICE_NAME
    ELSE SUBSTR(PREVIOUS_SERVICE_NAME, 4)
  END AS OLD_NAME,
  CASE
    WHEN REGEXP_LIKE(PROV_SERVICE_NAME, 'АнтиАОН', 'i') THEN PROV_SERVICE_NAME
    ELSE SUBSTR(PROV_SERVICE_NAME, 4)
  END AS NEW_NAME,
  CASE
    WHEN REGEXP_LIKE(PREVIOUS_SERVICE_NAME, 'АнтиАОН', 'i') THEN PREVIOUS_SERVICE_NAME
    WHEN UPPER(PREVIOUS_SERVICE_NAME) LIKE '%О!+ VIP%'                THEN 'VIP'
    WHEN UPPER(PREVIOUS_SERVICE_NAME) LIKE '%БЕЗЛИМИТ НА ВСЁ!_4369%'  THEN 'БЕЗЛИМИТ НА ВСЁ!_4369'
    WHEN REGEXP_LIKE(PREVIOUS_SERVICE_NAME, 'Иссык-Куль', 'i')        THEN 'Иссык-Куль'
    WHEN REGEXP_LIKE(PREVIOUS_SERVICE_NAME, 'Иссык-Куль\+', 'i')      THEN 'Иссык-Куль+'
    WHEN REGEXP_LIKE(PREVIOUS_SERVICE_NAME, '145\+', 'i')             THEN '145+'
    WHEN REGEXP_LIKE(PREVIOUS_SERVICE_NAME, 'Комбо Макс', 'i')        THEN 'Комбо Макс'
    WHEN REGEXP_LIKE(PREVIOUS_SERVICE_NAME, '160 PRO\+', 'i')         THEN '160PRO+'
    WHEN REGEXP_LIKE(PREVIOUS_SERVICE_NAME, '195 PRO', 'i')           THEN '195PRO'
    WHEN REGEXP_LIKE(PREVIOUS_SERVICE_NAME, '195\+', 'i')             THEN '195PRO+'
    WHEN REGEXP_LIKE(PREVIOUS_SERVICE_NAME, 'Нарын\+', 'i')           THEN 'Нарын+'
    WHEN REGEXP_LIKE(PREVIOUS_SERVICE_NAME, 'Комбо Безлимит', 'i')    THEN 'Комбо Безлимит'
    WHEN REGEXP_LIKE(PREVIOUS_SERVICE_NAME, 'Комбо', 'i')             THEN 'Комбо'
    WHEN REGEXP_LIKE(PREVIOUS_SERVICE_NAME, 'Регион', 'i')            THEN 'Регион'
    WHEN REGEXP_LIKE(PREVIOUS_SERVICE_NAME, 'Юг', 'i')                THEN 'Юг'
    WHEN REGEXP_LIKE(PREVIOUS_SERVICE_NAME, 'Premium', 'i')           THEN 'Premium'
    WHEN REGEXP_LIKE(PREVIOUS_SERVICE_NAME, 'Твой ноль Стандарт', 'i') THEN 'Твой ноль Стандарт 4 недели'
    WHEN REGEXP_LIKE(PREVIOUS_SERVICE_NAME, 'Переходи на О! Талас\+ \(4255\)', 'i') THEN 'Талас+'
    WHEN REGEXP_LIKE(PREVIOUS_SERVICE_NAME, 'Переходи на О! 490 \+ O!TV \(4334\)', 'i') THEN '490+'
    WHEN REGEXP_LIKE(PREVIOUS_SERVICE_NAME, 'Переходи на О! 540 \+ O!TV \(4371\)', 'i') THEN '540+'
    WHEN REGEXP_LIKE(PREVIOUS_SERVICE_NAME, 'Переходи на О! 160 PRO \(4290\)', 'i') THEN '160PRO'
    WHEN REGEXP_LIKE(PREVIOUS_SERVICE_NAME, 'Твой Суперноль 4 недели \(4345\)', 'i') THEN 'Суперноль'
    WHEN REGEXP_LIKE(PREVIOUS_SERVICE_NAME, 'Переходи на О! 145 \+ О!TV \(4366\)', 'i') THEN '145+'
    WHEN REGEXP_LIKE(PREVIOUS_SERVICE_NAME, 'Переходи на О! Талас \(4185\)', 'i') THEN 'Талас'
    WHEN REGEXP_LIKE(PREVIOUS_SERVICE_NAME, 'Переходи на О! Нарын \(4187\)', 'i') THEN 'Нарын'
    WHEN REGEXP_LIKE(PREVIOUS_SERVICE_NAME, 'БЕЗЛИМИТ НА ВСЁ!_4369', 'i') THEN 'БЕЗЛИМИТ'
    ELSE SUBSTR(PREVIOUS_SERVICE_NAME, 4)
  END AS "KEY",
  COUNT(*) AS "VALUE"
FROM KPI.COM_ORW_REWARD_DETAILED_GTM_MW
WHERE
  ACTION_DATE >= TO_DATE(:start_date, 'YYYY-MM-DD')
  AND ACTION_DATE <  TO_DATE(:end_date,   'YYYY-MM-DD')
GROUP BY
  CASE
    WHEN REGEXP_LIKE(PREVIOUS_SERVICE_NAME, 'АнтиАОН', 'i') THEN PREVIOUS_SERVICE_NAME
    ELSE SUBSTR(PREVIOUS_SERVICE_NAME, 4)
  END,
  CASE
    WHEN REGEXP_LIKE(PROV_SERVICE_NAME, 'АнтиАОН', 'i') THEN PROV_SERVICE_NAME
    ELSE SUBSTR(PROV_SERVICE_NAME, 4)
  END,
  CASE
    WHEN REGEXP_LIKE(PREVIOUS_SERVICE_NAME, 'АнтиАОН', 'i') THEN PREVIOUS_SERVICE_NAME
    WHEN UPPER(PREVIOUS_SERVICE_NAME) LIKE '%О!+ VIP%'                THEN 'VIP'
    WHEN UPPER(PREVIOUS_SERVICE_NAME) LIKE '%БЕЗЛИМИТ НА ВСЁ!_4369%'  THEN 'БЕЗЛИМИТ НА ВСЁ!_4369'
    WHEN REGEXP_LIKE(PREVIOUS_SERVICE_NAME, 'Иссык-Куль', 'i')        THEN 'Иссык-Куль'
    WHEN REGEXP_LIKE(PREVIOUS_SERVICE_NAME, 'Иссык-Куль\+', 'i')      THEN 'Иссык-Куль+'
    WHEN REGEXP_LIKE(PREVIOUS_SERVICE_NAME, '145\+', 'i')             THEN '145+'
    WHEN REGEXP_LIKE(PREVIOUS_SERVICE_NAME, 'Комбо Макс', 'i')        THEN 'Комбо Макс'
    WHEN REGEXP_LIKE(PREVIOUS_SERVICE_NAME, '160 PRO\+', 'i')         THEN '160PRO+'
    WHEN REGEXP_LIKE(PREVIOUS_SERVICE_NAME, '195 PRO', 'i')           THEN '195PRO'
    WHEN REGEXP_LIKE(PREVIOUS_SERVICE_NAME, '195\+', 'i')             THEN '195PRO+'
    WHEN REGEXP_LIKE(PREVIOUS_SERVICE_NAME, 'Нарын\+', 'i')           THEN 'Нарын+'
    WHEN REGEXP_LIKE(PREVIOUS_SERVICE_NAME, 'Комбо Безлимит', 'i')    THEN 'Комбо Безлимит'
    WHEN REGEXP_LIKE(PREVIOUS_SERVICE_NAME, 'Комбо', 'i')             THEN 'Комбо'
    WHEN REGEXP_LIKE(PREVIOUS_SERVICE_NAME, 'Регион', 'i')            THEN 'Регион'
    WHEN REGEXP_LIKE(PREVIOUS_SERVICE_NAME, 'Юг', 'i')                THEN 'Юг'
    WHEN REGEXP_LIKE(PREVIOUS_SERVICE_NAME, 'Premium', 'i')           THEN 'Premium'
    WHEN REGEXP_LIKE(PREVIOUS_SERVICE_NAME, 'Твой ноль Стандарт', 'i') THEN 'Твой ноль Стандарт 4 недели'
    WHEN REGEXP_LIKE(PREVIOUS_SERVICE_NAME, 'Переходи на О! Талас\+ \(4255\)', 'i') THEN 'Талас+'
    WHEN REGEXP_LIKE(PREVIOUS_SERVICE_NAME, 'Переходи на О! 490 \+ O!TV \(4334\)', 'i') THEN '490+'
    WHEN REGEXP_LIKE(PREVIOUS_SERVICE_NAME, 'Переходи на О! 540 \+ O!TV \(4371\)', 'i') THEN '540+'
    WHEN REGEXP_LIKE(PREVIOUS_SERVICE_NAME, 'Переходи на О! 160 PRO \(4290\)', 'i') THEN '160PRO'
    WHEN REGEXP_LIKE(PREVIOUS_SERVICE_NAME, 'Твой Суперноль 4 недели \(4345\)', 'i') THEN 'Суперноль'
    WHEN REGEXP_LIKE(PREVIOUS_SERVICE_NAME, 'Переходи на О! 145 \+ О!TV \(4366\)', 'i') THEN '145+'
    WHEN REGEXP_LIKE(PREVIOUS_SERVICE_NAME, 'Переходи на О! Талас \(4185\)', 'i') THEN 'Талас'
    WHEN REGEXP_LIKE(PREVIOUS_SERVICE_NAME, 'Переходи на О! Нарын \(4187\)', 'i') THEN 'Нарын'
    WHEN REGEXP_LIKE(PREVIOUS_SERVICE_NAME, 'БЕЗЛИМИТ НА ВСЁ!_4369', 'i') THEN 'БЕЗЛИМИТ'
    ELSE SUBSTR(PREVIOUS_SERVICE_NAME, 4)
  END