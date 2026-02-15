SELECT
  sub.month,
  sub.область,
  sub.title,
  COUNT(*) AS count
FROM (
  SELECT DISTINCT
    ji.id,
    to_char(date_trunc('month', ji.created), 'YYYY-MM-DD') AS month,
    cfo.customvalue                                        AS область,
    p.pname || ' ' || it.pname                             AS title
  FROM jiraissue ji
  -- область
  LEFT JOIN customfieldvalue cv10907
         ON cv10907.issue = ji.id
        AND cv10907.customfield = 10907
  LEFT JOIN customfieldoption cfo
         ON cfo.id::text = cv10907.stringvalue
        AND cfo.customfield = 10907
  -- проект / тип / статус
  LEFT JOIN project     p   ON p.id  = ji.project
  LEFT JOIN issuetype   it  ON it.id = ji.issuetype
  LEFT JOIN issuestatus iss ON iss.id = ji.issuestatus
  WHERE ji.created >= %(start_date)s::date
    AND ji.created <  %(end_date)s::date
    AND ji.archived = 'N'
    AND p.pkey = ANY(%(project_keys)s::text[])
    AND ji.resolution IS NULL
    -- только «родительские»
    AND NOT EXISTS (
      SELECT 1
      FROM issuelink ilp2
      WHERE ilp2.destination = ji.id
    )
    -- исключаем родителей с «запрещёнными» потомками
    AND NOT EXISTS (
      SELECT 1
      FROM issuelink il
      JOIN jiraissue   dji ON dji.id = il.destination
      JOIN issuestatus dis ON dis.id = dji.issuestatus
      WHERE il.source = ji.id
        AND dis.id::integer = ANY(%(excluded_status_ids)s::int[])
    )
) AS sub
GROUP BY sub.month, sub.область, sub.title
ORDER BY sub.month, sub.область, sub.title;
