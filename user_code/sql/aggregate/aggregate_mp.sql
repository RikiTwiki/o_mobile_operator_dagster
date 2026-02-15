WITH filtered_chats AS (SELECT c.id, c.created_at, c.finished_at, c.split_id, c.user_id
                        FROM mp.chats c
                                 JOIN mp.splits s ON c.split_id = s.id
                        WHERE (c.finished_at + INTERVAL '6 hours') >= %(start_date)s
                            AND (c.finished_at + INTERVAL '6 hours') < %(end_date)s
                            AND s.project_id IN ({project_ids_str})),
     chat_messages AS (SELECT m.chat_id,
                              json_agg(
                                      json_build_object(
                                              'text', m.text,
                                              'side', m.side,
                                              'user_id', m.user_id,
                                              'created_at', m.created_at + INTERVAL '6 HOUR',
                                              'a_created_at', a.created_at + INTERVAL '6 HOUR'
                                      ) ORDER BY m.created_at
                              ) AS messages
                       FROM mp.messages m
                                JOIN filtered_chats fc ON m.chat_id = fc.id
                                LEFT JOIN mp.assignies a ON m.chat_id = a.chat_id
                           AND m.created_at >= a.created_at
                           AND m.user_id = a.user_id
                           AND m.created_at < a.last_action_at
                       GROUP BY m.chat_id),
     chat_assignees AS (SELECT a.chat_id,
                               json_agg(
                                       json_build_object(
                                               'chat_id', a.chat_id,
                                               'created_at', a.created_at + INTERVAL '6 HOUR',
                                               'last_action_at', a.last_action_at + INTERVAL '6 HOUR',
                                               'unblocked_at', a.unblocked_at + INTERVAL '6 HOUR',
                                               'chat_finished_at', fc.finished_at + INTERVAL '6 HOUR',
                                               'user_id', a.user_id,
                                               'project_id', s.project_id,
                                               'split_title', s.title,
                                               'name', u.first_name || ' ' || u.last_name,
                                               'login', u.login,
                                               'closed_by_timeout', a.closed_by_timeout,
                                               'user_holding_time', COALESCE(hold_durations.total_duration, 0)
                                       ) ORDER BY a.created_at
                               ) AS assignies
                        FROM mp.assignies a
                                 JOIN filtered_chats fc ON a.chat_id = fc.id
                                 JOIN mp.users u ON a.user_id = u.id
                                 JOIN mp.splits s ON fc.split_id = s.id
                                 LEFT JOIN (SELECT h.chat_id,
                                                   h.user_id,
                                                   SUM(EXTRACT(epoch FROM h.finished_at) - EXTRACT(epoch FROM h.created_at)) as total_duration
                                            FROM mp.holds h
                                                     JOIN filtered_chats fc ON h.chat_id = fc.id
                                                     JOIN mp.assignies a ON h.chat_id = a.chat_id AND h.user_id = a.user_id
                                                AND h.user_id = a.user_id
                                                AND h.created_at >= a.created_at
                                                AND h.finished_at <= a.last_action_at
                                            WHERE a.user_id != 129
                                              AND a.last_action_at IS NOT NULL
                                            GROUP BY h.chat_id, h.user_id) hold_durations
                                           ON a.chat_id = hold_durations.chat_id AND a.user_id = hold_durations.user_id
                        WHERE a.user_id != 129
                          AND a.last_action_at IS NOT NULL
                        GROUP BY a.chat_id),
     chat_bots AS (SELECT m.chat_id,
                          json_agg(
                                  json_build_object(
                                          'user_id', u.id,
                                          'name', u.first_name || ' ' || u.last_name,
                                          'project_id', s.project_id,
                                          'split_title', s.title,
                                          'chat_finished_at', fc.finished_at + INTERVAL '6 HOUR',
                                          'login', u.login
                                  ) ORDER BY u.created_at
                          ) AS bots
                   FROM mp.messages m
                            JOIN filtered_chats fc ON m.chat_id = fc.id
                            JOIN mp.users u ON m.user_id = u.id AND u.id = 129
                            JOIN mp.splits s ON fc.split_id = s.id
                   GROUP BY m.chat_id),
     chat_holds AS (SELECT h.chat_id,
                           SUM(EXTRACT(epoch FROM h.finished_at) - EXTRACT(epoch FROM h.created_at)) as hold_duration
                    FROM mp.holds h
                             JOIN filtered_chats fc ON h.chat_id = fc.id
                    WHERE h.user_id = fc.user_id
                    GROUP BY h.chat_id)

SELECT fc.id,
       cm.messages,
       ca.assignies,
       cb.bots,
       fc.finished_at + INTERVAL '6 HOUR' AS chat_finished_at,
       fc.created_at + INTERVAL '6 HOUR' AS chat_created_at,
       fc.created_at + INTERVAL '6 HOUR',
       s.project_id,
       s.title                            as split_title,
       CASE
           WHEN ca.assignies IS NOT NULL
               THEN (ca.assignies -> 0 ->> 'login')
           END                            as user_login,
       COALESCE(ch.hold_duration, 0)      as chat_holding_time
FROM filtered_chats fc
         JOIN mp.splits s ON fc.split_id = s.id
         LEFT JOIN chat_messages cm ON cm.chat_id = fc.id
         LEFT JOIN chat_assignees ca ON ca.chat_id = fc.id
         LEFT JOIN chat_bots cb ON cb.chat_id = fc.id
         LEFT JOIN chat_holds ch ON ch.chat_id = fc.id
ORDER BY fc.id;
