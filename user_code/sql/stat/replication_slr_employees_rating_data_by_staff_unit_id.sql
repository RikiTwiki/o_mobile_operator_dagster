SELECT rserd.*
FROM stat.replication_slr_employees_rating_data AS rserd
WHERE rserd.staff_unit_id = %(staff_unit_id)s
  AND rserd.first_answer_received_at >= %(start_date)s
  AND rserd.first_answer_received_at <  %(end_date)s
  {excluded_filter};
