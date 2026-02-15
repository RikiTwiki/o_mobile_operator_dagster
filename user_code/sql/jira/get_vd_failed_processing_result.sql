SELECT
  to_char(date_trunc(%(trunc)s::text, ji.resolutiondate), %(date_format)s) AS date,
  cv.stringvalue AS id,
  co.customvalue AS title,
  COUNT(cv.stringvalue) AS total
FROM jiraissue AS ji
LEFT JOIN customfieldvalue AS cv
  ON (cv.issue)::numeric = (ji.id)::numeric
 AND cv.customfield = '12442'
LEFT JOIN customfieldoption AS co
  ON (cv.stringvalue)::numeric = co.id
LEFT JOIN issuestatus AS i
  ON i.id = ji.issuestatus
WHERE ji.resolutiondate >= %(start_date)s
  AND ji.resolutiondate <  %(end_date)s
  AND i.pname = %(closed_issue_title)s
  AND ji.issuetype = ANY(ARRAY['12206']::int[])
  AND cv.stringvalue IS NOT NULL
  AND cv.stringvalue <> ALL(%(excluded_values)s::text[])
GROUP BY 1, 2, 3
ORDER BY 1, 3;
