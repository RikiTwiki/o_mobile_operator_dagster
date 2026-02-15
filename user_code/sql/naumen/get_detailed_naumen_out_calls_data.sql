SELECT
    to_char(dos.attempt_start, 'YYYY-MM-DD') AS date,
    CASE
        WHEN dos.attempt_result = 'busy'               THEN 'Абонент занят'
        WHEN dos.attempt_result = 'no_answer'          THEN 'Абонент не принял вызов'
        WHEN dos.attempt_result = 'rejected'           THEN 'Абонент отклонил вызов'
        WHEN dos.attempt_result = 'operator_busy'      THEN 'Оператор занят'
        WHEN dos.attempt_result = 'operator_no_answer' THEN 'Оператор не принял вызов'
        WHEN dos.attempt_result = 'operator_rejected'  THEN 'Оператор отклонил вызов'
        WHEN dos.attempt_result = 'not_found'          THEN 'Вызываемый номер не существует'
        WHEN dos.attempt_result = 'UNKNOWN_ERROR'      THEN 'Неизвестный код отбоя'
        WHEN dos.attempt_result = 'abandoned'          THEN 'Потерянный вызов'
        WHEN dos.attempt_result = 'amd'                THEN 'Автоответчик'
        WHEN dos.attempt_result = 'CRR_DISCONNECT'     THEN 'Обрыв связи'
        WHEN dos.attempt_result = 'CRR_INVALID'        THEN 'Неправильный номер'
        WHEN dos.attempt_result = 'message_not_played' THEN 'Абонент завершил вызов во время IVR'
        WHEN dos.attempt_result = 'CRR_UNAVAILABLE'    THEN 'Не берут трубку'
        ELSE 'Неизвестно'
    END AS row,
    COUNT(dos.number_type) AS value
FROM detail_outbound_sessions AS dos
LEFT JOIN mv_employee AS me
       ON me.login = dos.login
WHERE dos.attempt_start >= %(start_date)s
  AND dos.attempt_start  < %(end_date)s
  AND dos.attempt_result <> 'connected'
  AND dos.project_id = %(project_id)s
GROUP BY 1, 2
ORDER BY 1, 2;