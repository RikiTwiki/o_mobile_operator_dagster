SELECT
    SUM(CASE WHEN ct.type = 'Подтверждается' THEN 1 ELSE 0 END) AS confirmed_complaints,
    SUM(CASE WHEN ct.type = 'Не подтверждается' THEN 1 ELSE 0 END) AS not_confirmed_complaints
FROM bpm.complaints c
LEFT JOIN bpm.complaint_templates ct
       ON c.template_id = ct.id
WHERE c.staff_unit_id = %(staff_unit_id)s
  AND c.created_at >= %(start_date)s
  AND c.created_at <  %(end_date)s;
