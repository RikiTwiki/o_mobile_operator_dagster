--   Возвращает список chat_id, которые нужно исключить из агрегации,
--    по аналогии с PHP-методом getExcludedChats().
--    Фильтрация по user_id = 572, диапазону дат и списку проектов.

SELECT amphtd.chat_id
FROM stat.aggregation_mp_handling_time_data amphtd
WHERE amphtd.user_id = 572
AND amphtd.chat_finished_at >= %(start_date)s
AND amphtd.chat_finished_at < %(end_date)s
AND amphtd.project_id = ANY(%(project_ids)s)