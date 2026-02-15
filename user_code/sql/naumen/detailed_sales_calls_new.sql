-- naumen/detailed_sales_calls_new.sql
SELECT
    TO_CHAR(attempt_start, 'YYYY-MM-DD') AS DAY,
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
