SELECT
  {head_fields}
  COUNT(*) AS {count_alias},
  ROUND(SUM(t.duration) / 60.0, 1) AS {total_alias},
  ROUND(AVG(t.duration) / 60.0, 1) AS {field_alias}
FROM (
  SELECT
    inner_q.issue_id,
    SUM(EXTRACT(EPOCH FROM (inner_q.left_at - inner_q.entered_at))) AS duration,
    date_trunc(%(trunc)s::text, inner_q.entered_at) AS day
  FROM (
    SELECT
      ji.id AS issue_id,
      cg.created AS entered_at,
      LEAD(cg.created) OVER (PARTITION BY ji.id ORDER BY cg.created) AS left_at,
      ci.newvalue
    FROM public.jiraissue ji
    JOIN public.project     p  ON ji.project = p.id
    JOIN public.changegroup cg ON cg.issueid = ji.id
    JOIN public.changeitem  ci ON ci.groupid = cg.id
    WHERE ci.field = %(field)s
      AND ji.created >= %(start_date)s
      AND ji.created <  %(end_date)s
      AND p.pkey = %(pkey)s
      AND ji.issuetype = %(issue_type)s
      AND ji.issuenum::text <> ALL(%(excluded_issuenum)s::text[])
  ) inner_q
  WHERE inner_q.newvalue = ANY(%(statuses)s::text[])
    AND inner_q.left_at IS NOT NULL
  GROUP BY inner_q.issue_id, date_trunc(%(trunc)s::text, inner_q.entered_at)
) t
{group_by_clause}
{order_by_clause};