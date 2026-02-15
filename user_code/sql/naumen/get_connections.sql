SELECT
  date,
  SUM(value) AS value
FROM connections
WHERE date BETWEEN %(start_date)s AND %(end_date)s
GROUP BY date
ORDER BY date;