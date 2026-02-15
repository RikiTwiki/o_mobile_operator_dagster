SELECT
    to_char(date_trunc(%(trunc)s::text, ivr_transitions.created_at), %(date_format)s) AS {date_field},
    ipd.id AS id,
    ipd.parent_id AS parent_id,
    ipd.title AS title,
    ipd.description AS description,
    COUNT(ivr_transitions.id) AS count
FROM ivr_transitions
LEFT JOIN ivr_prompt_descriptions ipd
    ON ipd.id = ivr_transitions.prompt_id
   AND ipd.removed = FALSE
WHERE ivr_transitions.code NOT IN ('MAIN', 'M0')
  AND ipd.removed = FALSE
  AND ivr_transitions.created_at >= %(start_date)s
  AND ivr_transitions.created_at <  %(end_date)s
  AND ipd.id <> ALL(%(execs)s::int[])
  AND ipd.parent_id = ANY(%(parents)s::int[])
  {callers_filter}
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1;
