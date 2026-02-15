SELECT
  to_char(
    date_trunc('{trunc}', amphtd.chat_finished_at), '{date_format}') AS "{date_field}",
  count(DISTINCT amphtd.chat_id) AS total_handled_chats
FROM stat.aggregation_mp_handling_time_data amphtd
LEFT JOIN bpm.staff_units su
  ON amphtd.staff_unit_id = su.id
WHERE amphtd.chat_finished_at >= %(start_date)s
  AND amphtd.chat_finished_at < %(end_date)s
  AND amphtd.project_id = ANY(%(project_ids)s::int[])
  {pst_positions_filter}
  {excluded_chats_filter}
GROUP BY 1
ORDER BY 1;