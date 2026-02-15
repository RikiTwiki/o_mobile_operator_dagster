SELECT
    COUNT(cfvp.stringvalue) AS confirmed_jira_mistakes
FROM jiraissue ji
LEFT JOIN customfieldvalue cfvp
    ON cfvp.issue = ji.id
   AND cfvp.customfield = 10553
LEFT JOIN customfieldvalue cfvr
    ON cfvr.issue = ji.id
   AND cfvr.customfield = 12723
WHERE ji.created >= %(start_date)s
  AND ji.created < %(end_date)s
  AND ji.issuestatus = '10202'
  AND cfvr.stringvalue = '20405'
  AND cfvp.stringvalue = %(login)s;