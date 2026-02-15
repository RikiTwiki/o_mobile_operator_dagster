# replicate_naumen_sl.py
# Python 3.11, Dagster ≥ 1.8, psycopg2-binary
# Replicator for Naumen Service Level (SL) data
#
# Порт PHP App\Console\Commands\Actions\NaumenServiceLevelDataCommand
# Логика:
# - По каждому проекту из NaumenProjectsGetter
# - Игнорируем archived и replicate=false (две отдельные проверки, без "or")
# - Окно репликации — почасовые интервалы [start, end), где end по умолчанию — сегодня 00:00 Asia/Bishkek
# - start: из конфига, или MAX("hour") в DWH + 1 день (00:00, при этом последняя запись обязана быть 23:00:00),
#          иначе project['replicate_start_at'] 00:00
# - Для каждого часового окна: выборка SL из Naumen, сбор агрегатов, батч-вставка 1 строки в DWH
# - Если нет данных за час — вставляем «пустую» строку (все метрики NULL), как в PHP
from __future__ import annotations

import json
from datetime import datetime, timedelta, time, date
from typing import Any, Dict, List, Optional

import pytz
from dagster import Field, op
from psycopg2.extras import execute_values

# Локальные утилиты / модели проекта
from utils.getters.naumen_projects_getter import NaumenProjectsGetter  # ожидается совместимый API

TZ = pytz.timezone("Asia/Bishkek")
TARGET_TABLE = "stat.replication_naumen_service_level_data"


def _json_default(o):
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    return str(o)


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


def _resolve_naumen_conn(context, source_name: str):
    """Вернёт psycopg2 connection по алиасу ('naumen_1' | 'naumen_3').

    Поддерживаются:
      - dict context.resources.naumen_sources с ключами 'naumen_1'/'naumen_3'
      - отдельные ресурсы context.resources.source_naumen_1 / source_naumen_3
      - алиасы: 'naumen'→'naumen_1', 'naumen1/3', 'source_naumen_1/3', 'n1/n3'
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
        f"Available resources: {available or 'none'}; or provide a 'naumen_sources' dict."
    )


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


def _get_last_hour_for_project(dwh_conn, project_id: str):
    sql = f'SELECT MAX("hour") FROM {TARGET_TABLE} WHERE project_id::text = %s;'
    with dwh_conn.cursor() as cur:
        cur.execute(sql, (str(project_id),))
        row = cur.fetchone()
        return row[0] if row and row[0] is not None else None


def _insert_rows(dwh_conn, rows: List[Dict[str, Any]]):
    if not rows:
        return
    keys = [
        "date",
        "hour",
        "type",
        "group",
        "source",
        "project_id",
        "project_title",
        "arpu",
        "language",
        "total",
        "ivr",
        "redirected",
        "call_project_changed",
        "queue",
        "threshold_queue",
        "total_to_operators",
        "answered",
        "lost",
        "sla_total",
        "sla_answered",
        "summary_waiting_time",
        "minimum_waiting_time",
        "maximum_waiting_time",
        "callback_success",
        "callback_disabled",
        "callback_unsuccessful",
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


# ---------------------------
# SQL: Service Level (за час)
# ---------------------------
SL_SQL = """
SELECT
  to_char(date_trunc('hour', qc.enqueued_time), 'YYYY-MM-DD HH24:MI:SS') AS date,
  SUM(CASE WHEN qc.enqueued_time IS NOT NULL THEN 1 ELSE 0 END) AS total,
  SUM(CASE WHEN qc.final_stage = 'ivr' THEN 1 ELSE 0 END) AS ivr,
  SUM(CASE WHEN qc.final_stage = 'redirect' THEN 1 ELSE 0 END) AS redirected,
  SUM(CASE WHEN qc.final_stage = 'callprojectchanged' THEN 1 ELSE 0 END) AS call_project_changed,
  SUM(CASE WHEN qc.final_stage = 'queue' AND INTERVALTOSEC(qc.dequeued_time - qc.unblocked_time) > %(short_calls_time_out)s
           THEN 1 ELSE 0 END) AS queue,
  SUM(CASE WHEN qc.final_stage = 'queue' AND INTERVALTOSEC(qc.dequeued_time - qc.unblocked_time) <= %(short_calls_time_out)s
           THEN 1 ELSE 0 END) AS threshold_queue,
  SUM(CASE WHEN qc.final_stage::text = 'operator'::text
            THEN 1
           WHEN qc.final_stage::text = 'queue'::text
            THEN 1
           ELSE 0 END) AS total_to_operators,
  SUM(CASE WHEN qc.final_stage::text = 'operator'::text
           THEN 1 ELSE 0 END) AS answered,
  SUM(CASE WHEN (qc.final_stage::text = 'queue'::text AND intervaltosec(qc.dequeued_time - qc.unblocked_time) > %(short_calls_time_out)s::numeric)
                 OR qc.final_stage::text = 'operator'::text
           THEN 1 ELSE 0 END) AS sla_total,
  SUM(CASE WHEN qc.final_stage::text = 'operator'::text
             AND intervaltosec(qc.dequeued_time - qc.unblocked_time) <= %(target_time)s::numeric
           THEN 1 ELSE 0 END) AS sla_answered,
  SUM(intervaltosec(date_trunc('second', qc.dequeued_time) - date_trunc('second', qc.unblocked_time)))::integer AS summary_waiting_time,
  MIN(intervaltosec(date_trunc('second', qc.dequeued_time) - date_trunc('second', qc.unblocked_time)))::integer AS minimum_waiting_time,
  MAX(intervaltosec(date_trunc('second', qc.dequeued_time) - date_trunc('second', qc.unblocked_time)))::integer AS maximum_waiting_time,
  SUM(CASE WHEN qc.final_stage = 'callback' AND cp.param_value = 'success'
           THEN 1 ELSE 0 END) AS callback_success,
  SUM(CASE WHEN qc.final_stage = 'callback' AND cp.param_value IN ('callback is disabled for project', 'caller phoned himself')
           THEN 1 ELSE 0 END) AS callback_disabled,
  SUM(CASE WHEN qc.final_stage = 'callback' AND cp.param_value NOT IN ('success', 'callback is disabled for project', 'caller phoned himself')
           THEN 1 ELSE 0 END) AS callback_unsuccessful
FROM queued_calls_ms qc
LEFT JOIN call_params cp
  ON cp.session_id = qc.session_id
 AND cp.param_name = 'CallbackResult'
WHERE qc.enqueued_time >= %(start)s
  AND qc.enqueued_time <  %(end)s
  AND qc.project_id = %(project_id)s
GROUP BY 1
ORDER BY 1;
"""


@op(
    required_resource_keys={"source_dwh", "source_naumen_1", "source_naumen_3"},
    config_schema={
        "start": Field(str, is_required=False, description="YYYY-MM-DD (local) — начало дня 00:00"),
        "end": Field(str, is_required=False, description="YYYY-MM-DD (local), exclusive; default: today 00:00 Asia/Bishkek)"),
        "group": Field(str, is_required=False, description="Опциональный фильтр по project.group"),
    },
    description="Репликация Naumen Service Level по часам в stat.replication_naumen_service_level_data",
)
def naumen_service_level_data(context) -> None:
    log = context.log
    dwh = context.resources.source_dwh

    if context.op_config.get("end"):
        end_day = datetime.strptime(context.op_config["end"], "%Y-%m-%d").date()
    else:
        end_day = datetime.now(TZ).date()
    end_dt = datetime.combine(end_day, time(0, 0, 0))

    start_cfg = context.op_config.get("start")
    group_filter = context.op_config.get("group")

    log.info(f"[NAUMEN-SL] REPLICATION WINDOW end(exclusive) = {end_dt:%Y-%m-%d %H:%M:%S} Asia/Bishkek")

    getter = NaumenProjectsGetter()
    projects = getter.project_id
    if group_filter:
        projects = [p for p in projects if p.get("group") == group_filter]

    total_rows = 0

    for p in projects:
        # Две отдельные проверки как в PHP
        if not as_bool(p.get("replicate")):
            continue
        if as_bool(p.get("archived")):
            continue

        proj_id = str(p.get("project_id"))
        raw_source = p.get("source")
        norm_source = _normalize_naumen_source(raw_source)
        if norm_source not in ("naumen_1", "naumen_3"):
            log.error(f"Invalid source {raw_source!r} for project {proj_id}. Skipping.")
            continue

        proj_title    = p.get("title")
        proj_group    = p.get("group")
        proj_type     = p.get("type")
        proj_arpu     = p.get("arpu")
        proj_language = p.get("language")
        proj_start    = p.get("replicate_start_at")
        target_time   = int(p.get("target_time") or 0)
        short_calls   = int(p.get("short_calls_time_out") or 0)

        log.info(f"Project: {proj_title} (id={proj_id}, source={raw_source} → {norm_source})")

        current: Optional[datetime] = None
        if start_cfg:
            try:
                # пробуем как полный timestamp
                current = datetime.strptime(start_cfg, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                # иначе как дату (берём 00:00)
                current = datetime.strptime(start_cfg, "%Y-%m-%d").replace(hour=0, minute=0, second=0, microsecond=0)
            log.info(f"\tStart (from config): {current:%Y-%m-%d %H:%M:%S}")
        else:
            # 2) Иначе берём MAX(hour) из DWH и продолжаем СЛЕДУЮЩИМ ЧАСОМ (без ошибки о 23:00:00)
            last_hour = _get_last_hour_for_project(dwh, proj_id)
            if last_hour is not None:
                # Продолжаем ровно с +1 час; усечём до часа на всякий случай
                lh = last_hour.replace(minute=0, second=0, microsecond=0)
                current = lh + timedelta(hours=1)
                log.info(
                    f"\tResuming from DWH last+1h: last={last_hour:%Y-%m-%d %H:%M:%S} → start={current:%Y-%m-%d %H:%M:%S}")
            else:
                # 3) Если вообще нет записей — стартуем с replicate_start_at 00:00
                if not proj_start:
                    log.error(f"\tNo replicate_start_at for project {proj_id}. Skipping.")
                    continue
                current = datetime.strptime(proj_start, "%Y-%m-%d").replace(hour=0, minute=0, second=0, microsecond=0)
                log.info(f"\tStart from project.replicate_start_at: {current:%Y-%m-%d %H:%M:%S}")
        # ---------- конец нового алгоритма ----------

        # Если уже догнали конец окна — пропускаем проект
        if current >= end_dt:
            log.info(f"\tUp-to-date (start {current:%Y-%m-%d %H:%M:%S} >= end {end_dt:%Y-%m-%d %H:%M:%S}). Skipping.")
            continue

        # Коннект к нужному Naumen
        try:
            naumen_conn = _resolve_naumen_conn(context, norm_source)
        except RuntimeError as e:
            log.error(f"\tConn resolve failed for source='{raw_source}': {e}")
            continue

        inserted_for_project = 0
        project_json = json.dumps(p, ensure_ascii=False, default=_json_default)

        # По часу
        while current < end_dt:
            next_hour = current + timedelta(hours=1)
            log.info(f"  Hour: {current:%Y-%m-%d %H}:00")

            with naumen_conn.cursor() as cur:
                cur.execute(
                    SL_SQL,
                    {
                        "start": current.strftime("%Y-%m-%d %H:%M:%S"),
                        "end": next_hour.strftime("%Y-%m-%d %H:%M:%S"),
                        "project_id": proj_id,
                        "target_time": target_time,
                        "short_calls_time_out": short_calls,
                    },
                )
                row = cur.fetchone()
                cols = [d[0] for d in cur.description] if cur.description else []
                item = dict(zip(cols, row)) if row else None

            now_str = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")

            if item:
                total_to_operators = item.get("total_to_operators")
                answered = item.get("answered")
                lost_val = None
                if total_to_operators is not None and answered is not None:
                    try:
                        lost_val = int(total_to_operators) - int(answered)
                    except Exception:
                        lost_val = None

                rec = {
                    "date": current.strftime("%Y-%m-%d"),
                    "hour": current.strftime("%Y-%m-%d %H:%M:%S"),
                    "type": proj_type,
                    "group": proj_group,
                    "source": raw_source,                 # сохраняем «сырой» source (Naumen/Naumen3)
                    "project_id": proj_id,
                    "project_title": proj_title,
                    "arpu": proj_arpu,
                    "language": proj_language,
                    "total": item.get("total"),
                    "ivr": item.get("ivr"),
                    "redirected": item.get("redirected"),
                    "call_project_changed": item.get("call_project_changed"),
                    "queue": item.get("queue"),
                    "threshold_queue": item.get("threshold_queue"),
                    "total_to_operators": total_to_operators,
                    "answered": answered,
                    "lost": lost_val,
                    "sla_total": item.get("sla_total"),
                    "sla_answered": item.get("sla_answered"),
                    "summary_waiting_time": item.get("summary_waiting_time"),
                    "minimum_waiting_time": item.get("minimum_waiting_time"),
                    "maximum_waiting_time": item.get("maximum_waiting_time"),
                    "callback_success": item.get("callback_success"),
                    "callback_disabled": item.get("callback_disabled"),
                    "callback_unsuccessful": item.get("callback_unsuccessful"),
                    "query": project_json,
                    "created_at": now_str,
                    "updated_at": now_str,
                }
            else:
                # Пустая строка, если за час данных нет
                rec = {
                    "date": current.strftime("%Y-%m-%d"),
                    "hour": current.strftime("%Y-%m-%d %H:%M:%S"),
                    "type": proj_type,
                    "group": proj_group,
                    "source": raw_source,
                    "project_id": proj_id,
                    "project_title": proj_title,
                    "arpu": proj_arpu,
                    "language": proj_language,
                    "total": None,
                    "ivr": None,
                    "redirected": None,
                    "call_project_changed": None,
                    "queue": None,
                    "threshold_queue": None,
                    "total_to_operators": None,
                    "answered": None,
                    "lost": None,
                    "sla_total": None,
                    "sla_answered": None,
                    "summary_waiting_time": None,
                    "minimum_waiting_time": None,
                    "maximum_waiting_time": None,
                    "callback_success": None,
                    "callback_disabled": None,
                    "callback_unsuccessful": None,
                    "query": project_json,
                    "created_at": now_str,
                    "updated_at": now_str,
                }

            _insert_rows(dwh, [rec])
            inserted_for_project += 1
            total_rows += 1
            current = next_hour

        log.info(f"Project done: {proj_title} (id={proj_id}). Inserted rows: {inserted_for_project}")

    log.info(f"[NAUMEN-SL] DONE. Total inserted rows: {total_rows}")