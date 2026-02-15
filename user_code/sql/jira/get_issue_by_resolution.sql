SELECT
  to_char(date_trunc(%(trunc)s::text, ji.created), %(date_format)s) AS date,
  cfp.customvalue AS status,
  cfo.customvalue AS title,
  COUNT(ji.issuetype) AS count,
  SUM(CASE WHEN i.pname <> %(closed_issue_title)s THEN 1 ELSE 0 END) AS not_resolved,
  COUNT(ji.issuetype) - SUM(CASE WHEN i.pname <> %(closed_issue_title)s THEN 1 ELSE 0 END) AS resolved
FROM jiraissue AS ji
LEFT JOIN project          AS p   ON ji.project     = p.id
LEFT JOIN issuestatus      AS i   ON ji.issuestatus = i.id
LEFT JOIN issuetype        AS it  ON ji.issuetype   = it.id
LEFT JOIN customfieldvalue AS cfv ON ji.id          = cfv.issue
LEFT JOIN customfieldoption AS cfo
  ON cfv.customfield = cfo.customfield
 AND cfv.stringvalue = cfo.id::text
 AND cfv.parentkey::int = cfo.parentoptionid
LEFT JOIN customfieldoption AS cfp
  ON cfo.customfield = cfp.customfield
 AND cfp.id = cfo.parentoptionid
 AND cfp.parentoptionid IS NULL
LEFT JOIN label            AS l   ON ji.id          = l.issue
WHERE ji.created >= %(start_date)s
  AND ji.created <  %(end_date)s
  AND p.pkey = %(pkey)s
  AND it.id = ANY(%(issue_types)s::text[])
  AND cfv.customfield = %(customfield)s
  AND cfo.id IS NOT NULL
  AND cfp.id IS NOT NULL
GROUP BY
  to_char(date_trunc(%(trunc)s::text, ji.created), %(date_format)s),
  cfp.customvalue,
  cfo.customvalue
ORDER BY 1;
