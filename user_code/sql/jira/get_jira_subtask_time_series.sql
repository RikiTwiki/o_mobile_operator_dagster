SELECT
  json_build_object(
    'parent_issue_type', parents.parent_issue_type,
    'subtitles',
      COALESCE(
        json_agg(
          json_build_object(
            'date',         subs.date,
            'subtask_type', subs.subtask_type,
            'count',        subs.subtasks_count
          )
          ORDER BY subs.date DESC, subs.subtasks_count DESC NULLS LAST
        ),
        '[]'::json
      )
  ) AS result
FROM (
  SELECT DISTINCT it_parent.pname AS parent_issue_type
  FROM jiraissue ji_parent
  JOIN project p
    ON ji_parent.project = p.id
   AND p.pkey = ANY(%(project_keys)s::text[])
  JOIN issuetype it_parent
    ON ji_parent.issuetype = it_parent.id
  WHERE ji_parent.created >= %(start_date)s::date
    AND ji_parent.created <  %(end_date)s::date
    AND it_parent.pname NOT ILIKE '%%SUB%%'
) AS parents
LEFT JOIN (
  SELECT
     it_parent.pname AS parent_issue_type,
     to_char(date_trunc('day', COALESCE(ji_sub.created, ji_parent.created)), 'YYYY-MM-DD') AS date,
     it_sub.pname AS subtask_type,
     COUNT(*) AS subtasks_count
  FROM jiraissue ji_parent
  JOIN project p
    ON p.id = ji_parent.project
   AND p.pkey = ANY(%(project_keys)s::text[])
  JOIN issuetype it_parent
    ON it_parent.id = ji_parent.issuetype
  LEFT JOIN issuelink il
    ON il.source = ji_parent.id
  LEFT JOIN jiraissue ji_sub
    ON ji_sub.id = il.destination
   AND ji_sub.created >= %(start_date)s::date
   AND ji_sub.created <  %(end_date)s::date
  LEFT JOIN issuetype it_sub
    ON it_sub.id = ji_sub.issuetype
   AND (it_sub.pname LIKE '[SUB] %%' OR it_sub.pname LIKE 'SUB:%%')
  WHERE ji_parent.created >= %(start_date)s::date
    AND ji_parent.created <  %(end_date)s::date
  GROUP BY
     it_parent.pname,
     date_trunc('day', COALESCE(ji_sub.created, ji_parent.created)),
     it_sub.pname
) AS subs
  ON subs.parent_issue_type = parents.parent_issue_type
GROUP BY parents.parent_issue_type
ORDER BY parents.parent_issue_type;
