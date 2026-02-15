SELECT
  to_char(date_trunc('day', ji_parent.created), 'YYYY-MM-DD') AS issue_date,
  it_parent.pname AS parent_issue_type,
  COUNT(*) AS issues_count
FROM jiraissue ji_parent
JOIN project p
  ON p.id = ji_parent.project
 AND p.pkey = ANY(%(project_keys)s::text[])
JOIN issuetype it_parent
  ON it_parent.id = ji_parent.issuetype
WHERE ji_parent.created >= %(start_date)s::date
  AND ji_parent.created <  %(end_date)s::date
  AND it_parent.pname NOT ILIKE '%%SUB%%'
GROUP BY
  issue_date, parent_issue_type
ORDER BY
  issue_date, parent_issue_type;
