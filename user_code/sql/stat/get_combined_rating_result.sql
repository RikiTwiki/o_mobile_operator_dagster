WITH base_raw AS (
  SELECT
      date_trunc(%(trunc)s::text, o.request_ended_at) AS period,  -- было started_at
      o.session_id,
      o.staff_unit_login
  FROM stat.omni_inbound_subscribers_requests o
  WHERE o.request_ended_at >= %(start_date)s
    AND o.request_ended_at <  %(end_date)s
    AND o.group_title   = ANY(%(group)s::text[])
    AND o.channel_title = ANY(%(channel)s::text[])
    AND o.direction IN ('inbound','in')
),
base AS (
  -- ровно одна строка на (period, session_id, login)
  SELECT DISTINCT period, session_id, staff_unit_login
  FROM base_raw
),

/* rsertd.mark — числовой (INTEGER), без регексов */
rsertd_agg AS (
  SELECT
      r.session_id,
      r.user_login,
      COUNT(*) FILTER (WHERE r.mark BETWEEN 0 AND 5) AS total_marks,
      COUNT(*) FILTER (WHERE r.mark IN (4,5))        AS good,
      COUNT(*) FILTER (WHERE r.mark = 3)             AS neutral,
      COUNT(*) FILTER (WHERE r.mark IN (0,1,2))      AS bad
  FROM stat.replication_slr_employees_rating_data r
  GROUP BY r.session_id, r.user_login
),

/* aqerd.mark — текст, приводим безопасно */
aqerd_agg AS (
  SELECT
      a.contact_identifier AS session_id,
      a.user_login,
      COUNT(*) FILTER (WHERE trim(a.mark) ~ '^[0-9]+$')                               AS total_marks,
      COUNT(*) FILTER (WHERE trim(a.mark) ~ '^[0-9]+$' AND (trim(a.mark))::int IN (4,5)) AS good,
      COUNT(*) FILTER (WHERE trim(a.mark) ~ '^[0-9]+$' AND (trim(a.mark))::int = 3)      AS neutral,
      COUNT(*) FILTER (WHERE trim(a.mark) ~ '^[0-9]+$' AND (trim(a.mark))::int IN (0,1,2)) AS bad
  FROM stat.aggregation_questionnaire_employee_rating_data a
  GROUP BY a.contact_identifier, a.user_login
),

ratings AS (
  -- объединяем источники на уровне (session_id, login)
  SELECT
      b.period,
      b.session_id,
      b.staff_unit_login,
      COALESCE(r.good,0)    + COALESCE(a.good,0)    AS good,
      COALESCE(r.neutral,0) + COALESCE(a.neutral,0) AS neutral,
      COALESCE(r.bad,0)     + COALESCE(a.bad,0)     AS bad
  FROM base b
  LEFT JOIN rsertd_agg r
    ON r.session_id = b.session_id AND r.user_login = b.staff_unit_login
  LEFT JOIN aqerd_agg a
    ON a.session_id = b.session_id AND a.user_login = b.staff_unit_login
),

per AS (
  SELECT
      period,
      COUNT(DISTINCT session_id)                      AS all_contacts,
      COALESCE(SUM(good + neutral + bad), 0)          AS total_marks,
      COALESCE(SUM(good), 0)                          AS good_marks,
      COALESCE(SUM(neutral), 0)                       AS neutral_marks,
      COALESCE(SUM(bad), 0)                           AS bad_marks
  FROM ratings
  GROUP BY period
)

SELECT
  to_char(period, %(date_format)s)                    AS "{date_field}",
  all_contacts,
  total_marks,
  good_marks,
  neutral_marks,
  bad_marks
FROM per
ORDER BY period;