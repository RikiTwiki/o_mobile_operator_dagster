SELECT
  to_char(
    date_trunc(%(trunc)s, amphtd.chat_finished_at), 'YYYY-MM-DD') AS date,
  count(DISTINCT amphtd.chat_id) AS total_handled_chats
FROM stat.aggregation_mp_handling_time_data amphtd
LEFT JOIN bpm.staff_units su
  ON amphtd.staff_unit_id = su.id
WHERE amphtd.chat_finished_at >= %(start_date)s
  AND amphtd.chat_finished_at < %(end_date)s
  AND amphtd.project_id = ANY(%(project_ids)s::int[])
  AND su.position_id = ANY(%(position_ids)s::int[])
GROUP BY date_trunc(%(trunc)s, amphtd.chat_finished_at)
ORDER BY date_trunc(%(trunc)s, amphtd.chat_finished_at);