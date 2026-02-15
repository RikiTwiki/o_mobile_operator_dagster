# replicate_naumen_ht.py
# Python 3.11, Dagster ≥ 1.8, psycopg2-binary
# Replicator for Naumen handling-time (AHT) data
#
# Порт PHP App\Console\Commands\Actions\NaumenHandlingTimeDataCommand
# Логика:
# - По каждому проекту из NaumenProjectsGetter
# - Игнорируем archived и replicate=false
# - Окно репликации — почасовые интервалы [start, end), где end по умолчанию — сегодня 00:00 Asia/Bishkek
# - start: из конфига, или последняя часовая запись (hour) в DWH + 1 день (00:00), или project['replicate_start_at'] 00:00
# - Для каждого часового окна: выборка AHT из Naumen, резолв staff_unit_id по login, батч-вставка в DWH
# - Если нет данных за час — вставляем пустую строку (как в PHP)

from __future__ import annotations

import json
from datetime import datetime, timedelta, time, date
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pytz
from dagster import Field, get_dagster_logger, op
from psycopg2.extras import execute_values

# Ваши локальные помощники/Models
from utils.models.staff_unit import StaffUnit
from utils.getters.naumen_projects_getter import NaumenProjectsGetter  # ожидается подобный API

TZ = pytz.timezone("Asia/Bishkek")
TARGET_TABLE = "stat.replication_naumen_handling_time_data"

def get_staff_unit_id_current(conn, login: Optional[str]) -> Optional[int]:
    if not login:
        return None
    with conn.cursor() as cur:
        cur.execute("""
            SELECT su.id
            FROM bpm.staff_units su
            JOIN bpm.users u ON u.id = su.user_id
            WHERE u.login = %s
              AND (su.accepted_at IS NULL OR su.accepted_at <= now())
              AND (su.dismissed_at IS NULL OR now() < su.dismissed_at)
            ORDER BY su.accepted_at DESC NULLS LAST
            LIMIT 1
        """, (login,))
        row = cur.fetchone()
        return row[0] if row else None

def _json_default(o):
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    return str(o)


# ---------------------------------------------------------------------------
# SQL: AHT за час с доп.агрегатами (холды)
# ---------------------------------------------------------------------------

AHT_SQL = """
WITH holds AS (
  SELECT
    session_id,
    initiator_id AS login,
    COUNT(session_id) AS quantity_hold,
    SUM(intervaltosec(date_trunc('second', ended) - date_trunc('second', entered))) AS sum_holding_time,
    MIN(intervaltosec(date_trunc('second', ended) - date_trunc('second', entered))) AS min_holding_time,
    MAX(intervaltosec(date_trunc('second', ended) - date_trunc('second', entered))) AS max_holding_time
  FROM call_status
  WHERE state = 'hold'
    AND entered >= %(start)s
    AND entered <  %(end)s
  GROUP BY 1, 2
)
SELECT
  to_char(date_trunc('hour', qc.enqueued_time), 'YYYY-MM-DD HH24:MI:SS') AS date,
  cl.dst_abonent AS login,
  COUNT(qc.session_id) AS quantity,
  SUM(INTERVALTOSEC(date_trunc('second', cl.connected) - date_trunc('second', cl.created))::integer) AS sum_pickup_time,
  MIN(INTERVALTOSEC(date_trunc('second', cl.connected) - date_trunc('second', cl.created))::integer) AS min_pickup_time,
  MAX(INTERVALTOSEC(date_trunc('second', cl.connected) - date_trunc('second', cl.created))::integer) AS max_pickup_time,
  SUM(INTERVALTOSEC(date_trunc('second', cl.ended) - date_trunc('second', cl.connected))::integer) AS sum_speaking_time,
  MIN(INTERVALTOSEC(date_trunc('second', cl.ended) - date_trunc('second', cl.connected))::integer) AS min_speaking_time,
  MAX(INTERVALTOSEC(date_trunc('second', cl.ended) - date_trunc('second', cl.connected))::integer) AS max_speaking_time,
  SUM(mpc.wrapuptime::integer) AS sum_wrapup_time,
  MIN(mpc.wrapuptime::integer) AS min_wrapup_time,
  MAX(mpc.wrapuptime::integer) AS max_wrapup_time,
  SUM(h.quantity_hold::integer) AS quantity_hold,
  SUM(h.sum_holding_time::integer) AS sum_holding_time,
  MIN(h.min_holding_time::integer) AS min_holding_time,
  MAX(h.max_holding_time::integer) AS max_holding_time
FROM queued_calls_ms qc
LEFT JOIN call_legs cl
  ON cl.session_id = qc.session_id
 AND cl.leg_id     = qc.next_leg_id
 AND qc.final_stage = 'operator'
LEFT JOIN mv_employee me
  ON me.login = cl.dst_abonent
LEFT JOIN mv_phone_call mpc
  ON mpc.projectuuid  = qc.project_id
 AND mpc.sessionid    = qc.session_id
 AND mpc.operatoruuid = me.uuid
LEFT JOIN holds h
  ON h.login = cl.dst_abonent
 AND h.session_id = cl.session_id
WHERE qc.enqueued_time >= %(start)s
  AND qc.enqueued_time <  %(end)s
  AND qc.final_stage = 'operator'
  AND qc.project_id = %(project_id)s
GROUP BY 1, 2
ORDER BY 1, 2;
"""


# ---------------------------------------------------------------------------
# Утилиты для ресурсов
# ---------------------------------------------------------------------------

def _resolve_naumen_conn(context, source_name: str):
    """Вернёт psycopg2 connection по алиасу ('naumen_1' | 'naumen_3').

    Поддерживаемые варианты:
      - через dict-мэппинг context.resources.naumen_sources: ключи 'naumen_1'/'naumen_3'
      - через отдельные ресурсы: context.resources.source_naumen_1 / source_naumen_3
    Допустимые алиасы: 'naumen_1', 'source_naumen_1', 'naumen_3', 'source_naumen_3'
    """
    if hasattr(context.resources, "naumen_sources"):
        mapping = context.resources.naumen_sources
        key = {
            "source_naumen_1": "naumen_1",
            "source_naumen_3": "naumen_3",
            "naumen_1": "naumen_1",
            "naumen_3": "naumen_3",
        }.get(source_name, source_name)
        try:
            return mapping[key]
        except KeyError:
            raise RuntimeError(
                f"Unknown Naumen source '{source_name}'. "
                f"Available in naumen_sources: {list(mapping.keys())}"
            )

    normalized = {
        "naumen": "naumen_1",
        "source_naumen_1": "naumen_1", "naumen_1": "naumen_1", "naumen1": "naumen_1", "n1": "naumen_1", "1": "naumen_1",
        "source_naumen_3": "naumen_3", "naumen_3": "naumen_3", "naumen3": "naumen_3", "n3": "naumen_3", "3": "naumen_3",
    }.get((source_name or "").strip().lower().replace("-", "_"))

    if normalized == "naumen_1" and hasattr(context.resources, "source_naumen_1"):
        return context.resources.source_naumen_1
    if normalized == "naumen_3" and hasattr(context.resources, "source_naumen_3"):
        return context.resources.source_naumen_3

    available = [k for k in ("source_naumen_1", "source_naumen_3") if hasattr(context.resources, k)]
    raise RuntimeError(
        f"Naumen connection not found for '{source_name}'. "
        f"Available resources: {available or 'none'}; "
        f"or provide a 'naumen_sources' dict with keys ['naumen_1','naumen_3']."
    )


def _get_last_hour_for_project(dwh_conn, project_id):
    sql = f'SELECT MAX("hour") FROM {TARGET_TABLE} WHERE project_id::text = %s;'
    with dwh_conn.cursor() as cur:
        cur.execute(sql, (str(project_id),))
        row = cur.fetchone()
        return row[0] if row and row[0] is not None else None

def _normalize_naumen_source(src: str) -> str:
    s = (src or "").strip().lower().replace("-", "_")
    mapping = {

        "naumen": "naumen_1",

        "naumen_1": "naumen_1", "source_naumen_1": "naumen_1",
        "naumen1": "naumen_1", "naumen-1": "naumen_1", "n1": "naumen_1", "1": "naumen_1",
        "naumen_3": "naumen_3", "source_naumen_3": "naumen_3",
        "naumen3": "naumen_3", "naumen-3": "naumen_3", "n3": "naumen_3", "3": "naumen_3",
    }
    return mapping.get(s)


def _insert_rows(dwh_conn, rows: List[Dict[str, Any]]):
    if not rows:
        return
    keys = [
        "date",
        "hour",
        "group",
        "source",
        "project_id",
        "project_title",
        "arpu",
        "language",
        "staff_unit_id",
        "user_login",
        "quantity",
        "sum_pickup_time",
        "min_pickup_time",
        "max_pickup_time",
        "sum_speaking_time",
        "min_speaking_time",
        "max_speaking_time",
        "sum_wrapup_time",
        "min_wrapup_time",
        "max_wrapup_time",
        "quantity_hold",
        "sum_holding_time",
        "min_holding_time",
        "max_holding_time",
        "query",
        "created_at",
        "updated_at",
    ]
    cols_sql = ", ".join(f'"{k}"' for k in keys)
    template = "(" + ",".join(["%s"] * len(keys)) + ")"
    values = [tuple(r.get(k) for k in keys) for r in rows]
    with dwh_conn.cursor() as cur:
        execute_values(
            cur,
            f"INSERT INTO {TARGET_TABLE} ({cols_sql}) VALUES %s",
            values,
            template=template,
        )
        dwh_conn.commit()

def as_bool(x):
    if isinstance(x, bool):
        return x
    if x is None:
        return False
    if isinstance(x, (int, float)):
        return x != 0
    if isinstance(x, str):
        s = x.strip().lower()
        if s in {"1", "true", "t", "yes", "y"}:
            return True
        if s in {"0", "false", "f", "no", "n", ""}:
            return False
    return bool(x)


# ---------------------------------------------------------------------------
# Dagster op
# ---------------------------------------------------------------------------

@op(
    required_resource_keys={"source_dwh", "source_naumen_1", "source_naumen_3"},

    config_schema={
        "start": Field(str, is_required=False, description="YYYY-MM-DD (local) — начало дня 00:00"),
        "end": Field(str, is_required=False, description="YYYY-MM-DD (local), exclusive; default: today 00:00 Asia/Bishkek"),
        "group": Field(str, is_required=False, description="Фильтр проектов по group если нужно"),
    },
)
def naumen_handling_time_data(context) -> None:
    logger = context.log
    dwh = context.resources.source_dwh

    end_day = datetime.strptime(context.op_config["end"], "%Y-%m-%d").date() if context.op_config.get("end") else datetime.now(TZ).date()
    end_dt = datetime.combine(end_day, time(0, 0, 0))

    start_cfg = context.op_config.get("start")
    group_filter = context.op_config.get("group")

    logger.info(f"[NAUMEN-HT] REPLICATION WINDOW end(exclusive) = {end_dt:%Y-%m-%d %H:%M:%S} Asia/Bishkek")

    getter = NaumenProjectsGetter()
    projects = getter.project_id
    if group_filter:
        projects = [p for p in projects if p.get("group") == group_filter]

    overall_inserted = 0
    for p in projects:
        replicate = as_bool(p.get("replicate"))
        archived = as_bool(p.get("archived"))

        if not replicate:
            continue
        if archived:
            continue

        proj_title   = p.get("title")
        proj_id      = str(p.get("project_id"))
        proj_group   = p.get("group")
        proj_arpu    = p.get("arpu")
        proj_language= p.get("language")
        proj_start   = p.get("replicate_start_at")
        raw_source = p.get("source")
        norm_source = _normalize_naumen_source(raw_source)
        if norm_source not in ("naumen_1", "naumen_3"):
            logger.error(f"\tInvalid source {raw_source!r} for project {p.get('project_id')}. Skipping.")
            continue

        logger.info(f"Project: {proj_title} (id={proj_id}, source={raw_source} → {norm_source})")

        if start_cfg:
            # Старт из конфига: всегда с 00:00 указанной даты
            current = datetime.strptime(start_cfg, "%Y-%m-%d").replace(hour=0, minute=0, second=0, microsecond=0)
            logger.info(f"\tStart (from config): {current:%Y-%m-%d %H:%M:%S}")
        else:
            last_hour = _get_last_hour_for_project(dwh, proj_id)
            if last_hour is not None:
                # ★ Новая логика: просто продолжаем с последнего часа + 1
                # (без требования полной дневной выборки до 23:00:00)
                current = (last_hour + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
                logger.info(
                    "\tResume from last hour: last_hour=%s -> current=%s",
                    last_hour.strftime("%Y-%m-%d %H:%M:%S"),
                    current.strftime("%Y-%m-%d %H:%M:%S"),
                )
            else:
                # Фолбэк: стартуем с replicate_start_at проекта (00:00)
                if not proj_start:
                    logger.error(f"\tNo replicate_start_at for project {proj_id}. Skipping.")
                    continue
                current = datetime.strptime(proj_start, "%Y-%m-%d").replace(hour=0, minute=0, second=0, microsecond=0)
                logger.info(f"\tStart from project.replicate_start_at: {current:%Y-%m-%d %H:%M:%S}")

        if current >= end_dt:
            logger.info(
                "\tAlready up-to-date for this project (current >= end). Skipping. "
                f"current={current:%Y-%m-%d %H:%M:%S}, end={end_dt:%Y-%m-%d %H:%M:%S}"
            )
            continue

        try:
            conn = _resolve_naumen_conn(context, norm_source)
        except RuntimeError as e:
            logger.error(f"Can't resolve Naumen connection for source='{norm_source}': {e}")
            continue

        # 3.3 Итерация по часам
        inserted_for_project = 0
        project_json = json.dumps(p, ensure_ascii=False, default=_json_default)

        while current < end_dt:
            next_hour = current + timedelta(hours=1)
            logger.info(f"  Hour: {current:%Y-%m-%d %H}:00")

            with conn.cursor() as cur:
                cur.execute(
                    AHT_SQL,
                    {
                        "start": current.strftime("%Y-%m-%d %H:%M:%S"),
                        "end":   next_hour.strftime("%Y-%m-%d %H:%M:%S"),
                        "project_id": proj_id,  # строка
                    },
                )
                cols = [d[0] for d in cur.description]
                rows = [dict(zip(cols, r)) for r in cur.fetchall()]

            now_str = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
            to_insert: List[Dict[str, Any]] = []

            if rows:
                for item in rows:
                    login = item.get("login")
                    try:
                        su_id = get_staff_unit_id_current(dwh, login) if login else None
                    except Exception as e:
                        context.log.warning(f"staff_unit resolve failed for login={login}: {e}")
                        su_id = None
                    to_insert.append({
                        "date": current.strftime("%Y-%m-%d"),
                        "hour": current.strftime("%Y-%m-%d %H:%M:%S"),
                        "group": proj_group,
                        "source": raw_source,
                        "project_id": proj_id,
                        "project_title": proj_title,
                        "arpu": proj_arpu,
                        "language": proj_language,
                        "staff_unit_id": su_id,
                        "user_login": login,
                        "quantity": item.get("quantity"),
                        "sum_pickup_time": item.get("sum_pickup_time"),
                        "min_pickup_time": item.get("min_pickup_time"),
                        "max_pickup_time": item.get("max_pickup_time"),
                        "sum_speaking_time": item.get("sum_speaking_time"),
                        "min_speaking_time": item.get("min_speaking_time"),
                        "max_speaking_time": item.get("max_speaking_time"),
                        "sum_wrapup_time": item.get("sum_wrapup_time") or 0,
                        "min_wrapup_time": item.get("min_wrapup_time") or 0,
                        "max_wrapup_time": item.get("max_wrapup_time") or 0,
                        "quantity_hold": item.get("quantity_hold"),
                        "sum_holding_time": item.get("sum_holding_time"),
                        "min_holding_time": item.get("min_holding_time"),
                        "max_holding_time": item.get("max_holding_time"),
                        "query": project_json,
                        "created_at": now_str,
                        "updated_at": now_str,
                    })
            else:
                to_insert.append({
                    "date": current.strftime("%Y-%m-%d"),
                    "hour": current.strftime("%Y-%m-%d %H:%M:%S"),
                    "group": proj_group,
                    "source": raw_source,
                    "project_id": proj_id,
                    "project_title": proj_title,
                    "arpu": proj_arpu,
                    "language": proj_language,
                    "staff_unit_id": None,
                    "user_login": None,
                    "quantity": None,
                    "sum_pickup_time": None,
                    "min_pickup_time": None,
                    "max_pickup_time": None,
                    "sum_speaking_time": None,
                    "min_speaking_time": None,
                    "max_speaking_time": None,
                    "sum_wrapup_time": None,
                    "min_wrapup_time": None,
                    "max_wrapup_time": None,
                    "quantity_hold": None,
                    "sum_holding_time": None,
                    "min_holding_time": None,
                    "max_holding_time": None,
                    "query": project_json,
                    "created_at": now_str,
                    "updated_at": now_str,
                })

            _insert_rows(dwh, to_insert)
            inserted_for_project += len(to_insert)
            overall_inserted += len(to_insert)
            current = next_hour

        logger.info(f"Project done: {proj_title} (id={proj_id}). Inserted rows: {inserted_for_project}")

    logger.info(f"[NAUMEN-HT] DONE. Total inserted rows: {overall_inserted}")