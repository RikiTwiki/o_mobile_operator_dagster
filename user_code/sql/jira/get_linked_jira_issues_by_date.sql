SELECT
  {head_fields}
  COUNT(*) AS linked_jira_issue,
  COUNT(*) AS linked_jira_issue
FROM jiraissue AS ji
JOIN issuetype AS it
  ON ji.issuetype = it.id
JOIN customfieldvalue AS cfv_grp
  ON cfv_grp.issue = ji.id
 AND cfv_grp.customfield = %(grp_customfield)s
JOIN customfieldoption AS cfo_grp
  ON cfv_grp.stringvalue = cfo_grp.id::text
 AND cfo_grp.customfield = %(grp_customfield)s
WHERE ji.project = ANY(%(projects)s::int[])
  AND it.pname = ANY(%(issue_types)s::text[])
  AND cfo_grp.customvalue = %(group_customvalue)s
  AND ji.created >= %(start_date)s
  AND ji.created <  %(end_date)s
{group_by_clause}
{order_by_clause};
