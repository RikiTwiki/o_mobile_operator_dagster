SELECT
    to_char(date_trunc(%(trunc)s::text, rserd.first_answer_received_at), %(date_format)s) AS date,
    SUM(CASE WHEN rserd.mark IN (4, 5) THEN 1 ELSE 0 END) AS good_marks,
    SUM(CASE WHEN rserd.mark IN (3)     THEN 1 ELSE 0 END) AS neutral_marks,
    SUM(CASE WHEN rserd.mark IN (0,1,2) THEN 1 ELSE 0 END) AS bad_marks
FROM stat.replication_slr_employees_rating_data AS rserd
WHERE rserd.first_answer_received_at >= %(start_date)s
  AND rserd.first_answer_received_at <  %(end_date)s
  AND rserd.channel_id = ANY(%(channel_ids)s::int[])
  {excluded_filter}
GROUP BY 1;
