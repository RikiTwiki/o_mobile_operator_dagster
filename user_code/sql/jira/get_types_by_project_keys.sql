SELECT
  id,
  issue_type_id,
  project_key,
  (project_name || ' - ' || issue_type_name) AS title
FROM jira_issue_type_catalog_data
WHERE group_id IS NOT NULL
  AND project_key = ANY(%(project_keys)s::text[]);
