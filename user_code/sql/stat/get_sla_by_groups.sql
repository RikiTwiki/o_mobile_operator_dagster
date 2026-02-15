SELECT
  to_char(date_trunc(%(trunc)s::text, sjisd.resolution_date), %(date_format)s) AS date
{project_fields}
  , SUM(CASE WHEN sjisd.general_sla_reached_status IS TRUE THEN 1 ELSE 0 END)::numeric AS sla_reached
  , COUNT(sjisd.general_sla_reached_status)::numeric AS all_resolved_issues
FROM stat.replication_jira_service_level_data sjisd
LEFT JOIN stat.jira_issue_type_catalog_data jitcd
  ON sjisd.issue_type_id = jitcd.issue_type_id
 AND sjisd.project_key  = jitcd.project_key
WHERE sjisd.resolution_date >= %(start_date)s
  AND sjisd.resolution_date <  %(end_date)s
  AND jitcd.group_id = ANY(%(groups)s::int[])
  /*__EXTRA_FILTER__*/
GROUP BY {group_by}
ORDER BY 1;