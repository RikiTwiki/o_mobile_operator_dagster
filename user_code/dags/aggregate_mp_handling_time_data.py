# aggregate_mp_handling_time_data.py
# Python 3.11, Dagster ≥ 1.8, psycopg2-binary
# Replicator for Messaging Platform handling-time data
# Переписано с PHP AggregateMPHandlingTimeDataCommand по тому же принципу,
# что и replicate_naumen_repeat_calls.py: дневные окна, таймзона Asia/Bishkek,
# батч-вставка в DWH, резолв staff_unit_id по логину.

from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pytz
from dagster import Field, get_dagster_logger, op
from psycopg2.extras import execute_values

from utils.models.staff_unit import StaffUnit

from resources import resources

BOT_USER_ID = 129
TZ = pytz.timezone("Asia/Bishkek")
DEFAULT_START = date(2025, 7, 1)

def get_staff_unit_id_as_of(login: Optional[str], as_of_dt: str, conn) -> Optional[int]:
    """Возвращает staff_unit_id, актуальный на дату/время as_of_dt (строка 'YYYY-MM-DD HH:MM:SS')."""
    if not login:
        return None
    with conn.cursor() as cur:
        cur.execute("""
            SELECT su.id
            FROM bpm.staff_units su
            JOIN bpm.users u ON u.id = su.user_id
            WHERE u.login = %s
              AND (su.accepted_at IS NULL OR su.accepted_at < %s::timestamp)
              AND (su.dismissed_at IS NULL OR su.dismissed_at > %s::timestamp)
            ORDER BY su.accepted_at DESC NULLS LAST
            LIMIT 1
        """, (login, as_of_dt, as_of_dt))
        row = cur.fetchone()
        if row:
            return row[0]

        # Фолбэк: самый свежий по accepted_at (как в PHP "first()")
        cur.execute("""
            SELECT su.id
            FROM bpm.staff_units su
            JOIN bpm.users u ON u.id = su.user_id
            WHERE u.login = %s
            ORDER BY su.accepted_at DESC NULLS LAST
            LIMIT 1
        """, (login,))
        row = cur.fetchone()
        return row[0] if row else None

# ---------------------------------------------------------------------------
# SQL: выборка чатов за сутки с сообщениями, назначениями и ботами
# Используем (c.finished_at + INTERVAL '6 hours') для локального дня
# ---------------------------------------------------------------------------

SELECT_DAY_SQL = """
WITH chats AS (
  SELECT m.chat_id,
         json_agg(
           json_build_object(
             'text', m.text,
             'side', m.side,
             'user_id', m.user_id,
             'created_at', to_char(m.created_at, 'YYYY-MM-DD HH24:MI:SS'),
             'a_created_at', to_char(a.created_at, 'YYYY-MM-DD HH24:MI:SS')
           )
           ORDER BY m.created_at
         ) AS messages
    FROM mp.messages m
    LEFT JOIN mp.chats c ON c.id = m.chat_id
    LEFT JOIN mp.assignies a
      ON a.chat_id = m.chat_id
     AND m.created_at >= a.created_at
     AND m.user_id    = a.user_id
     AND m.created_at <  a.last_action_at
    LEFT JOIN mp.splits ON splits.id = c.split_id
   WHERE c.finished_at >= (%(start)s::timestamp - interval '6 hours')
     AND c.finished_at <  (%(end)s::timestamp   - interval '6 hours')
     AND splits.project_id = ANY(%(project_ids)s::int[])
   GROUP BY m.chat_id
),
assignies AS (
  SELECT chat_id,
         json_agg(
           json_build_object(
             'chat_id', chat_id,
             'created_at',      to_char(created_at,      'YYYY-MM-DD HH24:MI:SS'),
             'unblocked_at',    to_char(unblocked_at,    'YYYY-MM-DD HH24:MI:SS'),
             'updated_at',      to_char(updated_at,      'YYYY-MM-DD HH24:MI:SS'),
             'last_action_at',  to_char(last_action_at,  'YYYY-MM-DD HH24:MI:SS'),
             'chat_finished_at',to_char(finished_at,     'YYYY-MM-DD HH24:MI:SS'),
             'user_id', user_id,
             'project_id', project_id,
             'split_title', split_title,
             'name', name,
             'login', login,
             'closed_by_timeout', closed_by_timeout
           ) ORDER BY created_at
         ) AS assignies
    FROM (
      SELECT DISTINCT
             c.id AS chat_id,
             (c.finished_at + interval '6 hours') AS finished_at,
             a.created_at,
             a.unblocked_at,
             a.updated_at,
             a.last_action_at,
             a.closed_by_timeout,
             u.first_name || ' ' || u.last_name AS name,
             u.login AS login,
             splits.project_id,
             splits.title AS split_title,
             a.user_id
        FROM mp.assignies a
        LEFT JOIN mp.chats  c  ON a.chat_id = c.id
        LEFT JOIN mp.users  u  ON a.user_id = u.id
        LEFT JOIN mp.splits    ON c.split_id = splits.id
       WHERE c.finished_at >= (%(start)s::timestamp - interval '6 hours')
         AND c.finished_at <  (%(end)s::timestamp   - interval '6 hours')
         AND splits.project_id = ANY(%(project_ids)s::int[])
         AND a.last_action_at IS NOT NULL
         AND a.user_id != %(bot_user_id)s
    ) t
   GROUP BY chat_id
),
bots AS (
  SELECT chat_id,
         json_agg(
           json_build_object(
             'user_id', user_id,
             'name', name,
             'project_id', project_id,
             'split_title', split_title,
             'chat_finished_at', to_char(chat_finished_at, 'YYYY-MM-DD HH24:MI:SS'),
             'login', login
           )
           ORDER BY created_at
         ) AS bots
  FROM (
    SELECT DISTINCT
      c.id AS chat_id,
      u.id AS user_id,
      u.first_name || ' ' || u.last_name AS name,
      u.login,
      splits.project_id,
      splits.title AS split_title,
      c.finished_at AS chat_finished_at,
      u.created_at AS created_at
    FROM mp.messages m
    LEFT JOIN mp.users  u ON m.user_id = u.id
    LEFT JOIN mp.chats  c ON m.chat_id = c.id
    LEFT JOIN mp.splits   ON c.split_id = splits.id
    WHERE c.finished_at >= (%(start)s::timestamp - interval '6 hours')
      AND c.finished_at <  (%(end)s::timestamp   - interval '6 hours')
      AND u.id = %(bot_user_id)s
      AND splits.project_id = ANY(%(project_ids)s::int[])
  ) t
  GROUP BY chat_id
),
holds AS (
  SELECT h.chat_id,
         SUM(EXTRACT(EPOCH FROM (h.finished_at - h.created_at)))::bigint AS user_holding_time
    FROM mp.holds h
    JOIN mp.chats c ON c.id = h.chat_id AND c.user_id = h.user_id
    LEFT JOIN mp.splits s ON c.split_id = s.id
   WHERE c.finished_at >= (%(start)s::timestamp - interval '6 hours')
     AND c.finished_at <  (%(end)s::timestamp   - interval '6 hours')
     AND s.project_id = ANY(%(project_ids)s::int[])
   GROUP BY h.chat_id
)
SELECT c.id,
       chats.messages,
       assignies.assignies,
       bots.bots,
       to_char((c.finished_at + interval '6 hours'), 'YYYY-MM-DD HH24:MI:SS') AS finished_at,
       to_char((c.created_at + interval '6 hours'),  'YYYY-MM-DD HH24:MI:SS') AS chat_created_at,
       holds.user_holding_time,
       c.created_at,
       splits.project_id,
       splits.title AS split_title
  FROM mp.chats c
  LEFT JOIN mp.splits   ON c.split_id = splits.id
  LEFT JOIN chats       ON chats.chat_id = c.id
  LEFT JOIN assignies   ON assignies.chat_id = c.id
  LEFT JOIN bots        ON bots.chat_id = c.id
  LEFT JOIN holds       ON holds.chat_id = c.id
 WHERE c.finished_at >= (%(start)s::timestamp - interval '6 hours')
   AND c.finished_at <  (%(end)s::timestamp   - interval '6 hours')
   AND splits.project_id = ANY(%(project_ids)s::int[])
 ORDER BY c.id;
"""

# ---------------------------------------------------------------------------
# Алгоритм вычисления метрик (порт PHP-логики)
# ---------------------------------------------------------------------------

def _ts(v: Any) -> int:
    if isinstance(v, (int, float)):
        return int(v)
    if isinstance(v, str):
        return int(datetime.strptime(v[:19], "%Y-%m-%d %H:%M:%S").timestamp())
    if isinstance(v, datetime):
        return int(v.timestamp())
    raise ValueError(f"Unsupported timestamp value: {v!r}")

def _as_py_json(v):
    if v is None:
        return []
    if isinstance(v, (list, dict)):
        return v
    if isinstance(v, memoryview):  # на всякий случай
        v = v.tobytes().decode()
    if isinstance(v, str):
        return json.loads(v)
    return v

def _add6(ts: Optional[str]) -> Optional[str]:
    if not ts:
        return ts
    dt = datetime.strptime(ts[:19], "%Y-%m-%d %H:%M:%S")
    return (dt + timedelta(hours=6)).strftime("%Y-%m-%d %H:%M:%S")


def compute_records_for_chat(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    messages = _as_py_json(row.get("messages"))
    assignies = _as_py_json(row.get("assignies"))
    bots_arr = _as_py_json(row.get("bots"))

    chat_created_at = row.get("chat_created_at")  # 'YYYY-MM-DD HH:MM:SS'
    user_holding_time = row.get("user_holding_time")

    start: Optional[str] = None
    answered = False
    bots: List[Dict[str, Any]] = []
    agents: List[Dict[str, Any]] = []
    legs: List[int] = []

    total_replies = 0
    total_reaction = 0
    chat_first_reaction = 0

    found = True
    has_question = False

    if assignies:
        for assigny in assignies:
            assigny_key = f"{assigny['created_at']}{assigny['user_id']}"
            user_reactions: Dict[str, bool] = {}

            last_before = None
            if messages:
                for lm in reversed(messages):
                    if lm["created_at"] < assigny["created_at"]:
                        last_before = lm
                        break
                for idx, m in enumerate(messages):
                    if idx == 0:
                        start = m["created_at"]
                        if m["side"] == "user":
                            continue
                    if last_before and last_before["side"] == "user" and m["side"] == "user":
                        user_reactions[assigny_key] = True
                    if idx > 0 and messages[idx-1]["side"] == "user" and m["side"] == "user":
                        if assigny["user_id"] == m["user_id"] and assigny["created_at"] == m.get("a_created_at"):
                            user_reactions[assigny_key] = True
                        continue
                    if m["side"] == "user":
                        if assigny["user_id"] == m["user_id"] and assigny["created_at"] == m.get("a_created_at"):
                            if assigny_key not in user_reactions:
                                start = assigny["created_at"]
                            if not answered:
                                reaction = _ts(m["created_at"]) - _ts(start)

                                if int(m["user_id"]) not in legs:
                                    legs.append(int(m["user_id"]))
                                agents.append({
                                    "agent_id": int(m["user_id"]),
                                    "reply": 1,
                                    "reaction": reaction,
                                    "created_at": m.get("a_created_at"),
                                    "message_created": m["created_at"],
                                })
                                total_replies += 1
                                total_reaction += reaction
                                found = False
                                answered = True
                            user_reactions[assigny_key] = True
                    if m["side"] == "client":
                        start = m["created_at"]
                        has_question = True
                        answered = False
            if found and has_question:
                reaction = _ts(assigny["last_action_at"]) - _ts(assigny["created_at"])
                agents.append({
                    "agent_id": int(assigny["user_id"]),
                    "reply": 0,
                    "reaction": reaction,
                    "created_at": assigny["created_at"],
                })

    if messages:
        for idx, m in enumerate(messages):
            if idx == 0:
                start = m["created_at"]
            if m["side"] == "client" and answered:
                start = m["created_at"]
                answered = False
            if m["side"] == "user" and not answered:
                answered = True
            if m["side"] == "bot" and not answered:
                reaction = _ts(m["created_at"]) - _ts(start)
                bots.append({
                    "agent_id": int(m["user_id"]),
                    "created_at": m["created_at"],
                    "reply": 1,
                    "reaction": reaction,
                })
                total_replies += 1
                total_reaction += reaction
                answered = True

    avg_avg_reaction = (total_reaction / total_replies) if total_replies else total_reaction

    out: List[Dict[str, Any]] = []

    # Боты
    for bot in bots_arr or []:
        bot_replies = 0
        bot_total = 0
        bot_reaction_first = 0
        for i, rep in enumerate(bots):
            if rep["agent_id"] == int(bot["user_id"]):
                bot_replies += 1
                bot_total += rep["reaction"]
                if i == 0:
                    chat_first_reaction = bot_reaction_first = rep["reaction"]
        out.append({
            "chat_id": row["id"],
            "chat_finished_at": row["finished_at"],
            "chat_reaction_time": chat_first_reaction,
            "chat_total_reaction_time": total_reaction,
            "chat_count_replies": total_replies,
            "chat_average_replies_time": avg_avg_reaction,
            "type": "bot",
            "project_id": row["project_id"],
            "split_title": row["split_title"],
            "is_bot_only": len(agents) == 0,
            "agent_leg_number": None,
            "user_id": int(bot["user_id"]),
            "user_login": bot.get("login"),
            "user_reaction_time": bot_reaction_first,
            "user_total_reaction_time": bot_total,
            "user_count_replies": bot_replies,
            "user_average_replies_time": (bot_total / bot_replies) if bot_replies else 0,
            "rate": None,

            "user_holding_time": user_holding_time,  # для бота скорее всего None, но оставим значение чата
            "assignee_created_at": None,
            "assignee_unblocked_at": None,
            "assignee_finished_at": None,
            "chat_created_at": chat_created_at,
            "closed_by_timeout": None,

        })

    # Операторы (по (agent_id, created_at))
    if assignies:
        grouped: Dict[Tuple[int, str], List[Dict[str, Any]]] = {}
        for a in agents:
            grouped.setdefault((a["agent_id"], a["created_at"]), []).append(a)

        for assigny in assignies:
            key = (int(assigny["user_id"]), assigny["created_at"])
            data = grouped.get(key)
            if not data:
                continue
            agent_replies = 0
            agent_total = 0
            agent_reaction_first = 0
            for idx, entry in enumerate(data):
                if idx == 0:
                    agent_reaction_first = entry["reaction"]
                agent_total += entry["reaction"]
                if entry["reply"] != 0:
                    agent_replies += 1
            agent_avg = (agent_total / agent_replies) if agent_replies else agent_total
            leg_num = (legs.index(int(assigny["user_id"])) + 1) if int(assigny["user_id"]) in legs else 1

            out.append({
                "chat_id": assigny["chat_id"],
                "chat_finished_at": assigny["chat_finished_at"],
                "chat_reaction_time": chat_first_reaction,
                "chat_total_reaction_time": total_reaction,
                "chat_count_replies": total_replies,
                "chat_average_replies_time": avg_avg_reaction,
                "type": "user",
                "project_id": assigny["project_id"],
                "split_title": assigny["split_title"],
                "is_bot_only": False,
                "agent_leg_number": leg_num,
                "user_id": int(assigny["user_id"]),
                "user_login": assigny.get("login"),
                "user_reaction_time": agent_reaction_first,
                "user_total_reaction_time": agent_total,
                "user_count_replies": agent_replies,
                "user_average_replies_time": agent_avg,
                "rate": None,

                "user_holding_time": user_holding_time,
                "assignee_created_at": assigny.get("created_at"),
                "assignee_unblocked_at": assigny.get("unblocked_at"),
                "assignee_finished_at": assigny.get("updated_at"),
                "chat_created_at": chat_created_at,
                "closed_by_timeout": assigny.get("closed_by_timeout"),

            })

    return out


# ---------------------------------------------------------------------------
# Dagster op
# ---------------------------------------------------------------------------

@op(
    required_resource_keys={"source_mp", "source_dwh"},
    config_schema={
        "start": Field(str, is_required=False, description="YYYY-MM-DD (local), default: last+1 or 2024-07-15"),
        "end": Field(str, is_required=False, description="YYYY-MM-DD (local), exclusive; default: today"),
        "project_ids": Field(list, is_required=False, description="Override project id list"),
    },
)
def aggregate_mp_handling_time_data(context) -> None:
    mp = context.resources.source_mp
    dwh = context.resources.source_dwh
    cfg = context.op_config

    # 1) Окно дат [start, end)
    if cfg.get("start"):
        start_day = datetime.strptime(cfg["start"], "%Y-%m-%d").date()
        context.log.info(f"Start передан в конфиге: {start_day}")
    else:
        sql = "SELECT MAX(chat_finished_at)::date FROM stat.aggregation_mp_handling_time_data;"
        context.log.info(f"Запрашиваю дату последней репликации:\n{sql}")
        with dwh.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()

        db_max_date = row[0] if row else None
        value_type = type(db_max_date).__name__ if db_max_date is not None else "None"
        context.log.info(f"MAX(chat_finished_at)::date вернулось: {db_max_date!r} (type={value_type})")

        start_day = (db_max_date + timedelta(days=1)) if db_max_date else DEFAULT_START
        source = "db_max_date + 1 день" if db_max_date else f"DEFAULT_START ({DEFAULT_START})"
        context.log.info(f"Итоговый start_day: {start_day} (источник: {source})")

    if cfg.get("end"):
        end_day = datetime.strptime(cfg["end"], "%Y-%m-%d").date()
        context.log.info(f"End передан в конфиге: {end_day}")
    else:
        end_day = datetime.now(TZ).date()
        context.log.info(f"End по умолчанию (сегодня в Asia/Bishkek): {end_day}")

    project_ids = cfg.get("project_ids") or [1, 3, 4, 5, 6, 7, 8, 9]
    context.log.info(f"Project IDs: {project_ids}")
    context.log.info(f"Окно репликации [start_day, end_day): [{start_day}, {end_day})")

    started_at = datetime.now(TZ)
    context.log.info(
        f"[MP-HT] REPLICATION STARTED: {started_at.isoformat()} | "
        f"window=[{start_day:%Y-%m-%d}, {end_day:%Y-%m-%d})"
    )

    try:

        # 2) Цикл по дням
        current = start_day
        while current < end_day:
            next_day = current + timedelta(days=1)
            context.log.info(f"Replicating date: {current:%Y-%m-%d}")

            # 3) SELECT с агрегацией JSON
            with mp.cursor() as cur:
                cur.execute(
                    SELECT_DAY_SQL,
                    {
                        "start": datetime(current.year, current.month, current.day),
                        "end": datetime(next_day.year, next_day.month, next_day.day),
                        "project_ids": project_ids,
                        "bot_user_id": BOT_USER_ID,
                    },
                )
                cols = [d[0] for d in cur.description]
                rows = [dict(zip(cols, r)) for r in cur.fetchall()]

            # 4) Вычисление метрик
            records: List[Dict[str, Any]] = []
            for r in rows:
                records.extend(compute_records_for_chat(r))

            context.log.info(f"Found {len(records)} rows for {current:%Y-%m-%d}")

            # 5) staff_unit_id для всех записей (и бота, и оператора)
            now_str = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")

            as_of_str = f"{current:%Y-%m-%d} 00:00:00"

            for rec in records:
                as_of_str = f"{current:%Y-%m-%d} 00:00:00"

                # сдвиг +6ч для assignies-полей перед записью
                for key in ("assignee_created_at", "assignee_unblocked_at", "assignee_finished_at"):
                    if rec.get(key):
                        rec[key] = _add6(rec[key])

                login = rec.get("user_login")
                rec["staff_unit_id"] = get_staff_unit_id_as_of(login, as_of_str, dwh) if login else None
                now_str = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
                rec["created_at"] = now_str
                rec["updated_at"] = now_str

            # 6) Вставка батчем
            if records:
                keys = [
                    "chat_id",
                    "chat_finished_at",
                    "chat_reaction_time",
                    "chat_total_reaction_time",
                    "chat_count_replies",
                    "chat_average_replies_time",
                    "type",
                    "project_id",
                    "split_title",
                    "is_bot_only",
                    "agent_leg_number",
                    "user_id",
                    "user_login",
                    "user_reaction_time",
                    "user_total_reaction_time",
                    "user_count_replies",
                    "user_average_replies_time",
                    "rate",
                    "staff_unit_id",

                    "user_holding_time",  # int (секунды) или NULL
                    "assignee_created_at",  # 'YYYY-MM-DD HH:MM:SS' или NULL
                    "assignee_unblocked_at",  # 'YYYY-MM-DD HH:MM:SS' или NULL
                    "assignee_finished_at",  # 'YYYY-MM-DD HH:MM:SS' или NULL
                    "chat_created_at",  # 'YYYY-MM-DD HH:MM:SS'
                    "closed_by_timeout",

                    "created_at",
                    "updated_at",
                ]
                cols_sql = ", ".join(f'"{k}"' for k in keys)
                template = "(" + ",".join(["%s"] * len(keys)) + ")"
                values = [tuple(rec.get(k) for k in keys) for rec in records]
                with dwh.cursor() as cur:
                    execute_values(
                        cur,
                        f"INSERT INTO stat.aggregation_mp_handling_time_data ({cols_sql}) VALUES %s",
                        values,
                        template=template,
                    )
                    dwh.commit()
            else:
                context.log.info("No records to insert (empty day).")

            current = next_day

    finally:
        finished_at = datetime.now(TZ)
        duration = finished_at - started_at
        context.log.info(
            f"[MP-HT] REPLICATION FINISHED: {finished_at.isoformat()} | "
            f"window=[{start_day:%Y-%m-%d}, {end_day:%Y-%m-%d}) | duration={duration}"
        )