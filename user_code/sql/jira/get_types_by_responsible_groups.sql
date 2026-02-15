SELECT
  id,
  issue_type_id,
  project_key,
  (project_name || ' - ' || issue_type_name) AS title
FROM stat.jira_issue_type_catalog_data
WHERE group_id IS NOT NULL
  AND issue_responsible_group = ANY(%(responsible_groups)s::text[]);
