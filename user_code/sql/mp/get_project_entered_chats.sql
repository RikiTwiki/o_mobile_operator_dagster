SELECT
  to_char(
    date_trunc(%(trunc)s, c.created_at),
      %(date_format)s
  ) AS "date",
  count(c.id) AS total_chats,
  s.title as project
FROM mp.chats c
LEFT JOIN mp.splits s
  ON s.id = c.split_id
WHERE (c.created_at) >= %(start_date)s
  AND (c.created_at) <  %(end_date)s
  AND s.project_id = ANY(%(project_ids)s::int[])
GROUP BY 1, 3
ORDER BY 1