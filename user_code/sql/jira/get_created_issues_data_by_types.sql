SELECT
  {head_fields}
  COUNT(ji.issuetype) AS total,
  SUM(CASE WHEN issuestatus.pname <> %(closed_issue_title)s THEN 1 ELSE 0 END) AS not_resolved,
  COUNT(ji.issuetype) - SUM(CASE WHEN issuestatus.pname <> %(closed_issue_title)s THEN 1 ELSE 0 END) AS resolved,
  ROUND(
    (COUNT(ji.issuetype) - SUM(CASE WHEN issuestatus.pname <> %(closed_issue_title)s THEN 1 ELSE 0 END))::numeric
    / NULLIF(COUNT(ji.issuetype), 0) * 100, 2
  ) AS resolved_percent,
  ROUND(
    (SUM(CASE WHEN issuestatus.pname <> %(closed_issue_title)s THEN 1 ELSE 0 END))::numeric
    / NULLIF(COUNT(ji.issuetype), 0) * 100, 2
  ) AS not_resolved_percent
FROM jiraissue AS ji
LEFT JOIN project     ON project.id = ji.project
LEFT JOIN issuestatus ON issuestatus.id = ji.issuestatus
LEFT JOIN issuetype   ON issuetype.id = ji.issuetype
WHERE ji.created >= %(start_date)s
  AND ji.created <  %(end_date)s
  AND issuetype.id = %(issue_type)s
  AND project.pkey = ANY(%(pkey)s::text[])
{group_by_clause}
{order_by_clause};