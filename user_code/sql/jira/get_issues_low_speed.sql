SELECT
  concat(p.pkey, '-', ji.issuenum) AS issue_key,
  ji.summary::text                 AS summary,
  ji.description                   AS description,
  to_char(ji.created, %(created_format)s) AS created,
  it.pname                         AS issue_type,
  iss.pname                        AS issue_status,

  MAX(CASE WHEN cf.id = 10553 THEN cfv.stringvalue END)        AS login,
  MAX(CASE WHEN cf.id = 10553 THEN cwdu.display_name END)       AS user_title,
  MAX(CASE WHEN cf.id = 10520 THEN cfo.customvalue END)         AS region,
  MAX(CASE WHEN cf.id = 10616 THEN cfo.customvalue END)         AS district,
  MAX(CASE WHEN cf.id = 10513 THEN cfv.stringvalue END)         AS phone_model,
  MAX(CASE WHEN cf.id = 10711 THEN cfo.customvalue END)         AS group_title,
  concat(
    MAX(CASE WHEN cf.id = 10302 AND cfv.parentkey IS NULL THEN cfo.customvalue END),
    MAX(CASE WHEN cf.id = 10302 AND cfo.parentoptionid::varchar = cfv.parentkey
             THEN ' - ' || cfo.customvalue END)
  )                                                             AS result

FROM jiraissue AS ji
LEFT JOIN project          AS p    ON p.id  = ji.project
LEFT JOIN issuetype        AS it   ON it.id = ji.issuetype
LEFT JOIN issuestatus      AS iss  ON iss.id = ji.issuestatus
LEFT JOIN label            AS l    ON l.issue = ji.id
LEFT JOIN customfieldvalue AS cfv  ON cfv.issue = ji.id
LEFT JOIN customfield      AS cf   ON cf.id = cfv.customfield
LEFT JOIN customfieldoption AS cfo ON cfo.id::varchar = cfv.stringvalue
LEFT JOIN cwd_user         AS cwdu ON cwdu.user_name = cfv.stringvalue

WHERE ji.created >= %(start_date)s
  AND ji.created <  %(end_date)s
  AND (
        ( p.pkey = ANY(%(pkeys1)s::text[])
          AND it.pname = ANY(%(issue_names)s::text[]) )
        OR
        ( p.pkey = 'MP' AND it.pname = 'Bug' AND l.label = 'core' )
      )

GROUP BY 1,2,3,4,5,6
ORDER BY 1;
