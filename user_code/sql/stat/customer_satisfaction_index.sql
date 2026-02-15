SELECT rserd.*
FROM stat.replication_slr_employees_rating_data AS rserd
WHERE rserd.first_answer_received_at >= %(start_date)s
  AND rserd.first_answer_received_at <  %(end_date)s
  AND rserd.channel_id = 205
  {excluded_filter};
