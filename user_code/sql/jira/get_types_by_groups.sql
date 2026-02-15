SELECT
  id,
  issue_type_id,
  project_key,
  (project_name || ' - ' || issue_type_name) AS title
FROM jira_issue_type_catalog_data
WHERE group_id IS NOT NULL
  AND {group_col} = ANY(%(groups)s::{group_cast}[])
ORDER BY project_key, issue_type_id;