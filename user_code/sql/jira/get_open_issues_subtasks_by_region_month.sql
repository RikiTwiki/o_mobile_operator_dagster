SELECT
  title,
  oblast AS "область",
  month,
  jsonb_agg(
    jsonb_build_object(
      'month',         month,
      'status',        status,
      'subtask_title', subtask_title,
      'count',         cnt
    )
    ORDER BY subtask_title
  ) AS subtasks
FROM (
  SELECT
    to_char(date_trunc('month', ji.created), 'YYYY-MM-DD') AS month,
    cfo.customvalue                                        AS oblast,
    p.pname || ' ' || it.pname                             AS title,
    CASE
      WHEN dis.id::integer NOT IN (SELECT unnest(%(excluded_status_ids)s::int[]))
        THEN dis.pname
      ELSE iss.pname
    END                                                    AS status,
    CASE
      WHEN dis.id::integer NOT IN (SELECT unnest(%(excluded_status_ids)s::int[]))
        THEN dp.pname || ' ' || dit.pname
      ELSE NULL
    END                                                    AS subtask_title,
    COUNT(*)                                               AS cnt
  FROM jiraissue ji
    LEFT JOIN customfieldvalue cv15607 ON cv15607.issue = ji.id AND cv15607.customfield = 15607
    LEFT JOIN customfieldvalue cv10907 ON cv10907.issue = ji.id AND cv10907.customfield = 10907
    LEFT JOIN customfieldoption  cfo   ON cv10907.stringvalue = cfo.id::text
                                       AND cfo.customfield    = 10907
    JOIN project        p   ON ji.project     = p.id
    JOIN issuetype      it  ON ji.issuetype   = it.id
    JOIN issuestatus    iss ON ji.issuestatus = iss.id
    LEFT JOIN issuelink     il  ON il.source      = ji.id
    LEFT JOIN jiraissue     dji ON dji.id         = il.destination
    LEFT JOIN issuetype     dit ON dji.issuetype  = dit.id
    LEFT JOIN project       dp  ON dji.project    = dp.id
    LEFT JOIN issuestatus   dis ON dji.issuestatus= dis.id
  WHERE ji.created >= %(start_date)s::date
    AND ji.created <  %(end_date)s::date
    AND ji.archived = 'N'
    AND NOT EXISTS (SELECT 1 FROM issuelink ilp WHERE ilp.destination = ji.id)
    AND (ji.resolution IS NULL OR ji.resolutiondate IS NULL)
    AND p.pkey = ANY(%(project_keys)s::text[])
    AND iss.id::integer NOT IN (SELECT unnest(%(excluded_status_ids)s::int[]))
    AND (dji.id IS NULL OR dis.id::integer NOT IN (SELECT unnest(%(excluded_status_ids)s::int[])))
  GROUP BY month, oblast, title, status, subtask_title
) AS grouped
GROUP BY title, oblast, month
ORDER BY title, oblast, month;
