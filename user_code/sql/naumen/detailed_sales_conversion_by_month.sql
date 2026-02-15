-- naumen/detailed_sales_conversion_by_month.sql

SELECT
    TO_CHAR(date_trunc('month', attempt_start), 'YYYY-MM-DD') AS date,
    COUNT(number_type)::numeric AS calls
FROM detail_outbound_sessions dos
LEFT JOIN mv_employee me
       ON dos.login = me.login
WHERE attempt_start >= %(start_date)s
  AND attempt_start <  %(end_date)s
  AND attempt_result = 'connected'
  AND project_id = 'corebo00000000000n9e9ma1906197mg'
GROUP BY 1
ORDER BY 1;
