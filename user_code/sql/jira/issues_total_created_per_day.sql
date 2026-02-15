SELECT
  to_char(date_trunc(%(trunc)s::text, created), %(date_format)s) AS date,
  COUNT(*) AS total
FROM jiraissue
WHERE created >= %(start_date)s
  AND created <  %(end_date)s
  AND issuetype = ANY(%(issuetypes)s::int[])
GROUP BY 1
ORDER BY 1;
