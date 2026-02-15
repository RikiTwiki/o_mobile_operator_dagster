SELECT
  to_char(date_trunc(%(trunc)s::text, ji.created), %(date_format)s) AS date,
  COUNT(*) AS total
FROM jiraissue AS ji
LEFT JOIN customfieldvalue AS cfv
  ON cfv.issue = ji.id
 AND cfv.customfield = 10223      -- как в PHP
 AND cfv.parentkey   = 13289      -- как в PHP
WHERE ji.created >= %(start_date)s
  AND ji.created <  %(end_date)s
  AND ji.issuetype = ANY(%(issuetypes)s::int[])
  AND cfv.stringvalue = ANY(%(results)s::text[])
GROUP BY 1
ORDER BY 1;
