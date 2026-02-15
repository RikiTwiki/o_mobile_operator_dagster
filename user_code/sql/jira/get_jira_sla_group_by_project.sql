SELECT
  it.pname AS title,
  SUM(
    CASE
      WHEN (json_extract_path_text(cva.textvalue::json, 'completeSLAData')::jsonb) IS NOT NULL THEN
        CASE WHEN (json_extract_path_text(cva.textvalue::json, 'completeSLAData')::jsonb -> 0 ->> 'succeeded') = 'true' THEN 1 ELSE 0 END
      ELSE
        CASE WHEN (json_extract_path_text(cvg.textvalue::json, 'completeSLAData')::jsonb -> 0 ->> 'succeeded') = 'true' THEN 1 ELSE 0 END
    END
  ) AS sla_reached,
  SUM(
    CASE
      WHEN (json_extract_path_text(cva.textvalue::json, 'completeSLAData')::jsonb) IS NOT NULL THEN
        CASE WHEN (json_extract_path_text(cva.textvalue::json, 'completeSLAData')::jsonb -> 0 ->> 'succeeded') = 'false' THEN 1 ELSE 0 END
      ELSE
        CASE WHEN (json_extract_path_text(cvg.textvalue::json, 'completeSLAData')::jsonb -> 0 ->> 'succeeded') = 'false' THEN 1 ELSE 0 END
    END
  ) AS sla_not_reached,
  SUM(
    CASE
      WHEN (json_extract_path_text(cva.textvalue::json, 'completeSLAData')::jsonb IS NULL
            OR json_extract_path_text(cva.textvalue::json, 'completeSLAData')::jsonb = '[]'::jsonb)
       AND (json_extract_path_text(cvg.textvalue::json, 'completeSLAData')::jsonb IS NULL
            OR json_extract_path_text(cvg.textvalue::json, 'completeSLAData')::jsonb = '[]'::jsonb)
      THEN 1 ELSE 0
    END
  ) AS without_sla,
  SUM(
    CASE
      WHEN (json_extract_path_text(cva.textvalue::json, 'completeSLAData')::jsonb) IS NOT NULL
        OR (json_extract_path_text(cvg.textvalue::json, 'completeSLAData')::jsonb) IS NOT NULL
      THEN 1 ELSE 0
    END
  ) AS all_resolved_issues
FROM jiraissue AS ji
LEFT JOIN project          AS p   ON ji.project     = p.id
LEFT JOIN issuestatus      AS i   ON ji.issuestatus = i.id
LEFT JOIN issuetype        AS it  ON ji.issuetype   = it.id
LEFT JOIN customfieldvalue AS cva ON cva.issue      = ji.id AND cva.customfield = %(sla_custom_field_general)s
LEFT JOIN customfieldvalue AS cvg ON cvg.issue      = ji.id AND cvg.customfield = %(sla_custom_field_secondary)s
LEFT JOIN label            AS l   ON ji.id          = l.issue
WHERE ji.resolutiondate >= %(start_date)s
  AND ji.resolutiondate <  %(end_date)s
  AND i.pname = ANY(%(closed_issue_titles)s::text[])
  AND p.pkey = ANY(%(project_keys)s::text[])
  /*__EXTRA_FILTER__*/
GROUP BY 1
ORDER BY 1;
