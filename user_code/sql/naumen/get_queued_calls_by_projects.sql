SELECT
  enqueued_time,
  final_stage,
  session_id
FROM queued_calls_ms
WHERE enqueued_time >= %(start_date)s
  AND enqueued_time <  %(end_date)s
  AND project_id = ANY(%(projects)s::text[]);