SELECT
  TO_CHAR(
    DATE_TRUNC(%(trunc)s::text, amphtd.chat_finished_at),
    %(date_format)s
  ) AS "{date_field}",

  amphtd.project_id,
  SUM(amphtd.user_count_replies) AS total

FROM stat.aggregation_mp_handling_time_data AS amphtd

WHERE amphtd.chat_finished_at >= %(start_date)s
  AND amphtd.chat_finished_at <  %(end_date)s
  AND amphtd.user_id = 129
  AND amphtd.project_id = ANY(%(project_ids)s::int[])
  {exclude_days_filter}
  {exclude_splits_filter}

GROUP BY 1, 2
ORDER BY 1;
