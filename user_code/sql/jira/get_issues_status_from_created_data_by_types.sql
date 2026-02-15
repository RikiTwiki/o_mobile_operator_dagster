SELECT
  {head_fields}
  COUNT(t.issue_id) AS open_close_count,
  ROUND(SUM(EXTRACT(EPOCH FROM (t.last_status_change - t.created_at))) / 60.0, 1) AS sum_open_close,
  ROUND(AVG(EXTRACT(EPOCH FROM (t.last_status_change - t.created_at))) / 60.0, 1) AS avg_open_close_min
FROM (
  SELECT
    ji.id AS issue_id,
    ji.created AS created_at,
    MAX(cg.created) AS last_status_change
  FROM public.jiraissue ji
  JOIN public.project p       ON ji.project = p.id
  JOIN public.changegroup cg  ON cg.issueid = ji.id
  JOIN public.changeitem ci   ON ci.groupid = cg.id
  WHERE ci.field = %(field)s
    AND ji.created >= %(start_date)s
    AND ji.created <  %(end_date)s
    AND p.pkey = %(pkey)s
    AND ji.issuetype = %(issue_type)s
    AND ji.issuenum::text <> ALL(%(excluded_issuenum)s::text[])
  GROUP BY ji.id, ji.created
) t
{group_by_clause}
{order_by_clause};