SELECT
  it_parent.pname AS parent_issue_type,
  (
    SELECT COUNT(*)
    FROM jiraissue ji2
    JOIN project p2 ON ji2.project = p2.id AND p2.pkey = ANY(%(project_keys)s::text[])
    JOIN issuetype it2 ON ji2.issuetype = it2.id
    WHERE ji2.created >= %(start_date)s::date
      AND ji2.created <  %(end_date)s::date
      AND it2.id = it_parent.id
      AND NOT (it2.pname LIKE '[SUB] %%' OR it2.pname LIKE 'SUB:%%')
  ) AS total_count,
  it_sub.pname AS subtask_type,
  COUNT(DISTINCT ji_sub.id) AS subtasks_count,
  SUM(
    CASE
      WHEN ist_sub.pname = ANY(ARRAY['Closed','Resolved','Done','Закрыто','Решено','Выполнено','Закрытый'])
      THEN 0 ELSE 1
    END
  ) AS open_subtasks_count,
  SUM(
    CASE
      WHEN ist_sub.pname = ANY(ARRAY['Closed','Resolved','Done','Закрыто','Решено','Выполнено','Закрытый'])
      THEN 1 ELSE 0
    END
  ) AS closed_subtasks_count,
  SUM(
    CASE
      WHEN ist_sub.pname = ANY(ARRAY['Closed','Resolved','Done','Закрыто','Решено','Выполнено','Закрытый'])
       AND ((s.cycle->>'elapsedTime')::bigint <= (s.cycle->>'goalTime')::bigint)
      THEN 1 ELSE 0
    END
  ) AS ontime_closed_subtasks_count,
  COALESCE(
    ROUND(AVG(
      CASE
        WHEN ist_sub.pname = ANY(ARRAY['Closed','Resolved','Done','Закрыто','Решено','Выполнено','Закрытый'])
         AND ((s.cycle->>'elapsedTime')::bigint <= (s.cycle->>'goalTime')::bigint)
        THEN ((s.cycle->>'goalTime')::numeric / 3600000.0)
      END
    ), 2), 0
  ) AS sla_plan_days,
  COALESCE(
    ROUND(AVG(
      CASE
        WHEN ist_sub.pname = ANY(ARRAY['Closed','Resolved','Done','Закрыто','Решено','Выполнено','Закрытый'])
         AND ((s.cycle->>'elapsedTime')::bigint <= (s.cycle->>'goalTime')::bigint)
        THEN ((s.cycle->>'elapsedTime')::numeric / 3600000.0)
      END
    ), 2), 0
  ) AS sla_fact_days
FROM jiraissue ji_parent
JOIN issuetype it_parent ON ji_parent.issuetype = it_parent.id
JOIN project p           ON ji_parent.project   = p.id AND p.pkey = ANY(%(project_keys)s::text[])
JOIN issuestatus ist_parent ON ji_parent.issuestatus = ist_parent.id
LEFT JOIN issuelink il   ON il.source = ji_parent.id
LEFT JOIN jiraissue ji_sub
  ON ji_sub.id = il.destination
 AND ji_sub.created >= %(start_date)s::date
 AND ji_sub.created <  %(end_date)s::date
LEFT JOIN issuetype it_sub ON ji_sub.issuetype = it_sub.id
JOIN issuestatus ist_sub   ON ji_sub.issuestatus = ist_sub.id
JOIN customfieldvalue cv   ON cv.issue = ji_sub.id
JOIN customfield cf        ON cf.id = cv.customfield AND cf.id = 15607
LEFT JOIN LATERAL jsonb_array_elements(cv.textvalue::jsonb -> 'completeSLAData') AS s(cycle) ON TRUE
WHERE ji_parent.created >= %(start_date)s::date
  AND ji_parent.created <  %(end_date)s::date
  AND NOT (it_parent.pname LIKE '[SUB] %%' OR it_parent.pname LIKE 'SUB:%%')
GROUP BY it_parent.id, it_parent.pname, it_sub.pname

UNION ALL

-- ЧАСТЬ 2: родители без сабтасков
SELECT
  it_parent.pname AS parent_issue_type,
  (
    SELECT COUNT(*)
    FROM jiraissue ji2
    JOIN project p2 ON ji2.project = p2.id AND p2.pkey = ANY(%(project_keys)s::text[])
    JOIN issuetype it2 ON ji2.issuetype = it2.id
    WHERE ji2.created >= %(start_date)s::date
      AND ji2.created <  %(end_date)s::date
      AND it2.id = it_parent.id
      AND NOT (it2.pname LIKE '[SUB] %%' OR it2.pname LIKE 'SUB:%%')
  ) AS total_count,
  NULL::text AS subtask_type,
  COUNT(DISTINCT ji_parent.id) AS subtasks_count,
  SUM(
    CASE
      WHEN ist_parent.pname = ANY(ARRAY['Closed','Resolved','Done','Закрыто','Решено','Выполнено','Закрытый'])
      THEN 0 ELSE 1
    END
  ) AS open_subtasks_count,
  SUM(
    CASE
      WHEN ist_parent.pname = ANY(ARRAY['Closed','Resolved','Done','Закрыто','Решено','Выполнено','Закрытый'])
      THEN 1 ELSE 0
    END
  ) AS closed_subtasks_count,
  SUM(
    CASE
      WHEN ist_parent.pname = ANY(ARRAY['Closed','Resolved','Done','Закрыто','Решено','Выполнено','Закрытый'])
       AND ((s.cycle->>'elapsedTime')::bigint <= (s.cycle->>'goalTime')::bigint)
      THEN 1 ELSE 0
    END
  ) AS ontime_closed_subtasks_count,
  COALESCE(
    ROUND(AVG(
      CASE
        WHEN ist_parent.pname = ANY(ARRAY['Closed','Resolved','Done','Закрыто','Решено','Выполнено','Закрытый'])
         AND ((s.cycle->>'elapsedTime')::bigint <= (s.cycle->>'goalTime')::bigint)
        THEN ((s.cycle->>'goalTime')::numeric / 3600000.0)
      END
    ), 2), 0
  ) AS sla_plan_days,
  COALESCE(
    ROUND(AVG(
      CASE
        WHEN ist_parent.pname = ANY(ARRAY['Closed','Resolved','Done','Закрыто','Решено','Выполнено','Закрытый'])
         AND ((s.cycle->>'elapsedTime')::bigint <= (s.cycle->>'goalTime')::bigint)
        THEN ((s.cycle->>'elapsedTime')::numeric / 3600000.0)
      END
    ), 2), 0
  ) AS sla_fact_days
FROM jiraissue ji_parent
JOIN issuetype it_parent ON ji_parent.issuetype = it_parent.id
JOIN project p           ON ji_parent.project   = p.id AND p.pkey = ANY(%(project_keys)s::text[])
JOIN issuestatus ist_parent ON ji_parent.issuestatus = ist_parent.id
JOIN customfieldvalue cv   ON cv.issue = ji_parent.id
JOIN customfield cf        ON cf.id = cv.customfield AND cf.id = 15607
LEFT JOIN LATERAL jsonb_array_elements(cv.textvalue::jsonb -> 'completeSLAData') AS s(cycle) ON TRUE
LEFT JOIN issuelink il     ON il.source = ji_parent.id
LEFT JOIN jiraissue ji_sub ON ji_sub.id = il.destination
 AND ji_sub.created >= %(start_date)s::date
 AND ji_sub.created <  %(end_date)s::date
LEFT JOIN issuetype it_sub ON ji_sub.issuetype = it_sub.id
WHERE ji_parent.created >= %(start_date)s::date
  AND ji_parent.created <  %(end_date)s::date
  AND NOT (it_parent.pname LIKE '[SUB] %%' OR it_parent.pname LIKE 'SUB:%%')
  AND ji_sub.id IS NULL
GROUP BY it_parent.id, it_parent.pname
ORDER BY parent_issue_type, subtasks_count DESC;