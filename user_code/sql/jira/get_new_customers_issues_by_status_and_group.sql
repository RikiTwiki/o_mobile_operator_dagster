SELECT
  to_char(date_trunc(%(trunc)s::text, ji.created), %(date_format)s) AS date,
  cv.stringvalue AS id,
  co.customvalue AS title,
  COUNT(cv.stringvalue) AS total
FROM jiraissue AS ji
LEFT JOIN customfieldvalue AS cv
  ON (cv.issue)::numeric = ji.id
 AND cv.customfield = %(customfield)s
LEFT JOIN customfieldoption AS co
  ON (cv.stringvalue)::numeric = co.id
LEFT JOIN issuestatus AS i
  ON i.id = ji.issuestatus
WHERE ji.created >= %(start_date)s
  AND ji.created <  %(end_date)s
  AND i.id = %(status_id)s
  AND ji.issuetype = ANY(%(issue_types)s::int[])
GROUP BY 1, 2, 3
ORDER BY 1;
