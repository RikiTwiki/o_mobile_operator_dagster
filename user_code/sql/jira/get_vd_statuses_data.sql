SELECT
  to_char(date_trunc(%(trunc)s::text, ji.created), %(date_format)s) AS date,
  cv.stringvalue AS id,
  co.customvalue AS title,
  COUNT(cv.stringvalue) AS total
FROM jiraissue AS ji
LEFT JOIN customfieldvalue AS cv
  ON (cv.issue)::numeric = (ji.id)::numeric
 AND cv.customfield = '10500'
LEFT JOIN customfieldvalue AS cvv
  ON (cvv.issue)::numeric = (ji.id)::numeric
 AND cvv.customfield = '10209'
LEFT JOIN customfieldoption AS co
  ON (cv.stringvalue)::numeric = co.id
LEFT JOIN issuestatus AS i
  ON i.id = ji.issuestatus
WHERE ji.created >= %(start_date)s
  AND ji.created <  %(end_date)s
  AND cvv.stringvalue = '10800'
  AND ji.issuetype = ANY(%(issuetypes)s::int[])
  AND cv.stringvalue IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY 1;
