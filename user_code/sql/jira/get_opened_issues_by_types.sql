(
  SELECT
      to_char(date_trunc('month', fi.created), 'YYYY-MM-DD') AS month,
      CASE WHEN dji.id IS NOT NULL THEN dis.pname ELSE iss.pname END AS status,
      (p.pname || ' ' || it.pname)                           AS title,
      CASE
        WHEN dji.id IS NOT NULL THEN (dp.pname || ' ' || dit.pname)
        ELSE NULL
      END                                                    AS subtask_title
  FROM (
    SELECT
        ji.id,
        ji.created,
        ji.project,
        ji.issuetype,
        ji.issuenum,
        ji.issuestatus
    FROM jiraissue AS ji
    LEFT JOIN customfieldvalue AS cv ON cv.issue = ji.id
    LEFT JOIN customfield     AS cf ON cv.customfield = cf.id
    WHERE ji.created >= %(start_date)s
      AND ji.created <  %(end_date)s
      AND ji.archived = 'N'
      AND cf.id = 15607
      AND NOT EXISTS (
            SELECT 1
            FROM issuelink AS ilp
            WHERE ilp.destination = ji.id
          )
      AND (ji.resolution IS NULL OR ji.resolutiondate IS NULL)
  ) AS fi
  LEFT JOIN project     AS p   ON p.id  = fi.project
  LEFT JOIN issuestatus AS iss ON iss.id = fi.issuestatus
  LEFT JOIN issuetype   AS it  ON it.id  = fi.issuetype
  LEFT JOIN issuelink   AS il  ON il.source = fi.id
  LEFT JOIN jiraissue   AS dji ON dji.id = il.destination
  LEFT JOIN issuetype   AS dit ON dit.id = dji.issuetype
  LEFT JOIN project     AS dp  ON dp.id  = dji.project
  LEFT JOIN issuestatus AS dis ON dis.id = dji.issuestatus
  WHERE iss.id <> ALL(%(excluded_status_ids)s::int[])
    AND p.pkey = ANY(%(project_keys)s::text[])
    AND (dji.id IS NULL OR dis.id <> ALL(%(excluded_status_ids)s::int[]))

  UNION ALL

  SELECT
      to_char(date_trunc('month', fi.created), 'YYYY-MM-DD') AS month,
      iss.pname                                             AS status,
      (p.pname || ' ' || it.pname)                          AS title,
      NULL                                                  AS subtask_title
  FROM (
    SELECT
        ji.id,
        ji.created,
        ji.project,
        ji.issuetype,
        ji.issuenum,
        ji.issuestatus
    FROM jiraissue AS ji
    LEFT JOIN customfieldvalue AS cv ON cv.issue = ji.id
    LEFT JOIN customfield     AS cf ON cv.customfield = cf.id
    WHERE ji.created >= %(start_date)s
      AND ji.created <  %(end_date)s
      AND ji.archived = 'N'
      AND cf.id = 15607
      AND NOT EXISTS (
            SELECT 1
            FROM issuelink AS ilp
            WHERE ilp.destination = ji.id
          )
      AND (ji.resolution IS NULL OR ji.resolutiondate IS NULL)
  ) AS fi
  LEFT JOIN project     AS p   ON p.id  = fi.project
  LEFT JOIN issuestatus AS iss ON iss.id = fi.issuestatus
  LEFT JOIN issuetype   AS it  ON it.id  = fi.issuetype
  LEFT JOIN issuelink   AS il  ON il.source = fi.id
  LEFT JOIN jiraissue   AS dji ON dji.id = il.destination
  LEFT JOIN issuestatus AS dis ON dis.id = dji.issuestatus
  WHERE iss.id <> ALL(%(excluded_status_ids)s::int[])
    AND p.pkey = ANY(%(project_keys)s::text[])
    AND dji.id IS NOT NULL
)
ORDER BY month;