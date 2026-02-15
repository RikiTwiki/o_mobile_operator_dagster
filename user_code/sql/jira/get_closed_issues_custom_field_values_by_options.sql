SELECT
  to_char(date_trunc(%(trunc)s::text, ji.resolutiondate), %(date_format)s) AS date,
  cv.stringvalue AS id,
  co.customvalue AS title,
  SUM(CASE WHEN i.pname = %(closed_issue_title)s THEN 1 ELSE 0 END) AS total
FROM jiraissue AS ji
LEFT JOIN customfieldvalue AS cv
  ON (cv.issue)::numeric = ji.id
 AND cv.customfield = %(customfield)s
LEFT JOIN customfieldoption AS co
  ON (cv.stringvalue)::numeric = co.id
LEFT JOIN issuestatus AS i
  ON i.id = ji.issuestatus
WHERE ji.resolutiondate >= %(start_date)s
  AND ji.resolutiondate <  %(end_date)s
  AND ji.issuetype = ANY(%(issue_types)s::int[])
  AND cv.stringvalue IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY 1;
