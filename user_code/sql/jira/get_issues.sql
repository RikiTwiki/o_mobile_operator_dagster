SELECT
  TO_CHAR(DATE_TRUNC(%(trunc)s, ji.created), %(date_format)s) AS date,
  cfo.customvalue AS title,
  SUM(CASE WHEN iss.pname <> %(closed_issue_title)s THEN 1 ELSE 0 END) AS not_resolved,
  COUNT(ji.id) AS count,
  COUNT(ji.issuetype)
    - SUM(CASE WHEN iss.pname <> %(closed_issue_title)s THEN 1 ELSE 0 END) AS resolved
FROM jiraissue AS ji
LEFT JOIN project           AS p   ON ji.project    = p.id
LEFT JOIN issuestatus       AS iss ON ji.issuestatus = iss.id
LEFT JOIN issuetype         AS it  ON ji.issuetype  = it.id
LEFT JOIN customfieldvalue  AS cfv ON ji.id         = cfv.issue
LEFT JOIN customfieldoption AS cfo ON cfo.id::varchar = cfv.stringvalue
LEFT JOIN label             AS l   ON ji.id         = l.issue
WHERE ji.created >= %(start_date)s
  AND ji.created <  %(end_date)s
  AND p.pkey = %(pkey)s
  AND cfv.customfield = %(customfield)s
  AND it.id = %(issue_type)s
  AND cfo.id IS NOT NULL
GROUP BY 1, 2
ORDER BY 1;
