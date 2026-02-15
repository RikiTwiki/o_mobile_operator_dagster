WITH RECURSIVE session_times AS (
  SELECT
    cl.src_id       AS subscriber_number,
    cl.session_id,
    MIN(cl.created) AS call_start,
    MAX(cl.ended)   AS call_end
  FROM call_legs cl
  WHERE cl.created >= %(start)s
    AND cl.created <  %(end)s
  GROUP BY cl.src_id, cl.session_id
),

session_status AS (
  SELECT
    st.subscriber_number,
    st.session_id,
    st.call_start,
    st.call_end,
    MAX(
      CASE q.final_stage
        WHEN 'operator' THEN 3
        WHEN 'queue'    THEN 2
        WHEN 'ivr'      THEN 1
      END
    ) AS status_code
  FROM session_times st
  JOIN queued_calls_ms q
    ON q.session_id = st.session_id
  WHERE q.final_stage IN ('ivr','queue','operator')
    AND q.project_id IN (
              'corebo00000000000ni3d5tgr2iffv24',
              'corebo00000000000nha1om5shqidq98',
              'corebo00000000000nnrdrlklmt48gjk',
              'corebo00000000000nha1ii9e4fddmu0',
              'corebo00000000000nhd5elip10bnp68',
              'corebo00000000000nhrnevpmhdmpib0',
              'corebo00000000000o6dk0apqn5m6hcc',
              'corebo00000000000nhrmqtr14nggsfk',
              'corebo00000000000nhrna8c54omdru4',
              'corebo00000000000ni3d75k61bti2es',
              'corebo00000000000oedipngl7u45t8o',
              'corebo00000000000nha1flctieef5hc',
              'corebo00000000000nhrncfjsmfkv0do',
              'corebo00000000000nhrnfi0c5bm20us',
              'corebo00000000000nk61tt6l743cl34',
              'corebo00000000000nha1i167lv182gs',
              'corebo00000000000nha1j4r316jk6u0',
              'corebo00000000000npqh4o5fiq36qb4',
              'corebo00000000000npqh8r5q4n36570',
              'corebo00000000000npqh6nt8js1fg2s',
              'corebo00000000000npqh1gid4hpm25o'
            )
  GROUP BY st.subscriber_number, st.session_id, st.call_start, st.call_end
),

final_sessions AS (
  SELECT
    subscriber_number,
    session_id,
    call_start,
    call_end,
    CASE status_code
      WHEN 3 THEN 'operator'
      WHEN 2 THEN 'queue'
      WHEN 1 THEN 'ivr'
    END AS completion_status,
    DATE(call_start) AS call_date
  FROM session_status
),

ordered AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY subscriber_number, call_date
      ORDER BY call_start
    ) AS rn
  FROM final_sessions
),

mark AS (
  SELECT
    subscriber_number,
    session_id,
    completion_status,
    call_start,
    call_end,
    call_date,
    rn,
    CASE WHEN completion_status = 'ivr' THEN 1 ELSE 0 END AS ivr_balance,
    0 AS is_repeated
  FROM ordered
  WHERE rn = 1

  UNION ALL

  SELECT
    o.subscriber_number,
    o.session_id,
    o.completion_status,
    o.call_start,
    o.call_end,
    o.call_date,
    o.rn,
    CASE
      WHEN o.completion_status = 'ivr' THEN m.ivr_balance + 1
      WHEN o.completion_status IN ('operator','queue') AND m.ivr_balance > 0 THEN 0
      ELSE m.ivr_balance
    END AS ivr_balance,
    CASE
      WHEN o.completion_status IN ('operator','queue') AND m.ivr_balance > 0 THEN 1
      ELSE 0
    END AS is_repeated
  FROM mark m
  JOIN ordered o
    ON o.subscriber_number = m.subscriber_number
   AND o.call_date        = m.call_date
   AND o.rn               = m.rn + 1
)

SELECT
  call_date AS day,
  ROUND(
    (SUM(is_repeated)::numeric / NULLIF(COUNT(*), 0)) * 100
  , 1) AS repeat_calls
FROM mark
GROUP BY call_date
ORDER BY call_date;