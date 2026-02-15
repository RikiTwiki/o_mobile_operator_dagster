SELECT
  {first_field}
  {group_fields}
  {title_field}
  , COUNT(jiraissue.issuetype) AS count
  , SUM(CASE WHEN issuestatus.pname <> %(closed_issue_title)s THEN 1 ELSE 0 END) AS not_resolved
  , COUNT(jiraissue.issuetype) - SUM(CASE WHEN issuestatus.pname <> %(closed_issue_title)s THEN 1 ELSE 0 END) AS resolved
FROM jiraissue
LEFT JOIN project    ON project.id = jiraissue.project
LEFT JOIN issuestatus ON issuestatus.id = jiraissue.issuestatus
LEFT JOIN issuetype  ON issuetype.id = jiraissue.issuetype
WHERE jiraissue.created >= %(start_date)s
  AND jiraissue.created <  %(end_date)s
  /*__EXTRA_FILTER__*/
GROUP BY {group_by}
ORDER BY 1;
