SELECT
  to_char(date_trunc(%(trunc)s::text, ji.resolutiondate), %(date_format)s) AS date
{group_fields}
  , SUM(CASE WHEN isstat.pname = %(closed_issue_title)s THEN 1 ELSE 0 END) AS closed
FROM jiraissue AS ji
LEFT JOIN project     AS prj    ON prj.id = ji.project
LEFT JOIN issuestatus AS isstat ON isstat.id = ji.issuestatus
LEFT JOIN issuetype   AS itype  ON itype.id = ji.issuetype
WHERE ji.resolutiondate >= %(start_date)s
  AND ji.resolutiondate <  %(end_date)s
  AND ji.issuetype = ANY(%(types)s::int[])
  /*__EXTRA_FILTER__*/
GROUP BY {group_by}
ORDER BY 1;
