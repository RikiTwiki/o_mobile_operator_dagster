SELECT
    to_char(date_trunc(%(trunc)s::text, created_at), %(date_format)s) AS date,
    SUM(CASE WHEN data IS NULL THEN 0 ELSE 1 END) as filled,
    SUM(CASE WHEN data IS NULL THEN 1 ELSE 0 END) as not_filled,
    COUNT(*) as total
FROM bpm.questionnaire_results
WHERE questionnaire_id = %(questionnaire_id)s
    AND created_at >= %(start_date)s
    AND created_at < %(end_date)s
GROUP BY 1
ORDER BY 1;