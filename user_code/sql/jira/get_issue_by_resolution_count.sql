SELECT
  {head_fields}
  COUNT(ji.issuetype) AS count,
  SUM(CASE WHEN cfp.id = ANY(%(resolved_set)s::int[]) THEN 1 ELSE 0 END)::numeric AS resolved_count,
  SUM(CASE WHEN cfp.id = %(ID_HELPED)s THEN 1 ELSE 0 END) AS resolved,
  SUM(CASE WHEN cfp.id = %(ID_NOT_HELPED)s THEN 1 ELSE 0 END) AS not_resolved,
  SUM(CASE WHEN cfp.id = %(ID_CLIENT_SIDE_REASON)s THEN 1 ELSE 0 END) AS client_side_reason,
  SUM(CASE WHEN cfp.id = %(ID_INFRA_HARDWARE_ISSUE)s THEN 1 ELSE 0 END) AS infrastructure_hardware_issue,
  SUM(CASE WHEN cfp.id = %(ID_TICKET_CANCEL)s THEN 1 ELSE 0 END) AS ticket_cancel,
  ROUND(
    SUM(CASE WHEN cfp.id = %(ID_HELPED)s THEN 1 ELSE 0 END)::numeric
    / NULLIF(SUM(CASE WHEN cfp.id = ANY(%(resolved_set)s::int[]) THEN 1 ELSE 0 END)::numeric, 0) * 100, 2
  ) AS resolved_percent,
  ROUND(
    SUM(CASE WHEN cfp.id = %(ID_NOT_HELPED)s THEN 1 ELSE 0 END)::numeric
    / NULLIF(SUM(CASE WHEN cfp.id = ANY(%(resolved_set)s::int[]) THEN 1 ELSE 0 END)::numeric, 0) * 100, 2
  ) AS not_resolved_percent
FROM jiraissue AS ji
LEFT JOIN project           AS p   ON ji.project     = p.id
LEFT JOIN issuestatus       AS i   ON ji.issuestatus = i.id
LEFT JOIN issuetype         AS it  ON ji.issuetype   = it.id
LEFT JOIN customfieldvalue  AS cfv ON ji.id          = cfv.issue
LEFT JOIN customfieldoption AS cfo
  ON cfv.customfield = cfo.customfield
 AND cfv.stringvalue = cfo.id::text
 AND cfv.parentkey::int = cfo.parentoptionid
LEFT JOIN customfieldoption AS cfp
  ON cfo.customfield = cfp.customfield
 AND cfp.id = cfo.parentoptionid
 AND cfp.parentoptionid IS NULL
LEFT JOIN label             AS l   ON ji.id          = l.issue
WHERE ji.created >= %(start_date)s
  AND ji.created <  %(end_date)s
  AND p.pkey = %(pkey)s
  AND it.id = ANY(%(issue_types)s::text[])
  AND cfv.customfield = ANY(%(customfields)s::int[])
  AND cfo.id IS NOT NULL
  AND cfp.id IS NOT NULL
{group_by_clause}
{order_by_clause};
