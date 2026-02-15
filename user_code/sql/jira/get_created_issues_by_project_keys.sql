SELECT
  to_char(date_trunc(%(trunc)s::text, ji.created), %(date_format)s) AS date
{group_fields}
  , COUNT(ji.issuetype) AS created
  , SUM(CASE WHEN isstat.pname <> %(closed_issue_title)s THEN 1 ELSE 0 END) AS not_resolved
  , COUNT(ji.issuetype) - SUM(CASE WHEN isstat.pname <> %(closed_issue_title)s THEN 1 ELSE 0 END) AS resolved
FROM jiraissue AS ji
LEFT JOIN label      AS lbl    ON lbl.issue   = ji.id
LEFT JOIN project    AS prj    ON prj.id      = ji.project
LEFT JOIN issuestatus AS isstat ON isstat.id  = ji.issuestatus
LEFT JOIN issuetype   AS itype  ON itype.id   = ji.issuetype
WHERE ji.created >= %(start_date)s
  AND ji.created <  %(end_date)s
  AND prj.pkey = ANY(%(project_keys)s::text[])
  /*__EXTRA_FILTER__*/
GROUP BY {group_by}
ORDER BY 1;
