# replicate_naumen_repeat_calls.py
# Python 3.11, Dagster ≥ 1.8, psycopg2-binary
# Репликатор Naumen Repeat Calls (повторные звонки)
#
# Порт PHP App\Console\Commands\Actions\NaumenRepeatCallDataCommand
# Ключевые моменты:
# - Аргументы: group, connection, start, end
# - Смены берём из bpm.timetable_data_detailed (view_id = 6)
# - Окно репликации: по дням, отдельно для каждой смены (start_time)
# - Начало:
#     if end задан → start = (если start != "default": та дата; иначе — 1-е число текущего месяца)
#     elif в DWH есть записи → start = last(repeat_calls_period_date)+1 день
#     else → start = (если start != "default": та дата; иначе — 1-е число текущего месяца)
# - Конец: end или "вчера" (локально Asia/Bishkek)
# - Источник Naumen выбирается по алиасу (connection): 'naumen'→naumen_1; 'naumen3'→naumen_3 и т.п.
# - Для каждого звонка рассчитываем resolve_status и вставляем строку в stat.replication_repeat_calls_data
# - Если для смен присутствует '00:00:00' — сперва запускаем её отдельно (как в PHP), затем прогоняем все смены

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pytz
from dagster import Field, op
from psycopg2.extras import execute_values

from utils.getters.naumen_projects_getter import NaumenProjectsGetter

TZ = pytz.timezone("Asia/Bishkek")
VIEW_ID_FOR_SHIFTS = 6
TARGET_TABLE = "stat.replication_repeat_calls_data"

TASKS: list[dict[str, str]] = [
    {"group": "General",       "connection": "naumen"},
    {"group": "Saima",         "connection": "naumen"},
    {"group": "Agent",         "connection": "naumen3"},
    {"group": "Money",         "connection": "naumen3"},
    {"group": "Terminals",     "connection": "naumen3"},
    {"group": "Entrepreneurs", "connection": "naumen"},
    {"group": "AkchaBulak",    "connection": "naumen3"},
    {"group": "Bank",          "connection": "naumen3"},
    {"group": "O!Bank",        "connection": "naumen3"},
]


# --------------------------
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# --------------------------

def _today_local() -> date:
    return datetime.now(TZ).date()


def _yesterday_local() -> date:
    return _today_local() - timedelta(days=1)


def _parse_date_ymd(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _dt(s: str) -> datetime:
    # 'YYYY-MM-DD HH:MM:SS'
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")

def _to_date(x) -> date | None:
    if x is None:
        return None
    if isinstance(x, datetime):
        return x.date()
    if isinstance(x, date):
        return x
    # на всякий случай: строки -> пытаемся распарсить YYYY-MM-DD[ HH:MM:SS]
    try:
        return datetime.fromisoformat(str(x)).date()
    except Exception:
        return None

def _pick_staff_unit_id(login: Optional[str], as_of_day: date,
                        login_to_user_ids: Dict[str, List[int]],
                        staff_by_user: Dict[int, List[Tuple[int, Optional[datetime], Optional[datetime]]]]) -> Optional[int]:
    if not login:
        return None
    user_ids = login_to_user_ids.get(login) or []
    if not user_ids:
        return None

    # 1) активный на дату
    for uid in user_ids:
        for sid, acc, dism in staff_by_user.get(uid, []):
            acc_d = _to_date(acc)
            dism_d = _to_date(dism)
            ok_start = (acc_d is None) or (acc_d < as_of_day)          # как в PHP: accepted_at < date
            ok_end   = (dism_d is None) or (dism_d > as_of_day)        # dismissed_at > date
            if ok_start and ok_end:
                return sid

    # 2) самый свежий, если активного не нашли
    for uid in user_ids:
        items = staff_by_user.get(uid, [])
        if items:
            return items[0][0]
    return None


def _normalize_naumen_source(src: str) -> str | None:
    s = (src or "").strip().lower().replace("-", "_").replace(" ", "")
    mapping = {
        # naumen_1
        "naumen": "naumen_1",
        "naumen1": "naumen_1",
        "source_naumen_1": "naumen_1",
        "naumen_1": "naumen_1",
        "n1": "naumen_1",
        "1": "naumen_1",

        # naumen_3
        "naumen3": "naumen_3",
        "naumen_3": "naumen_3",
        "source_naumen_3": "naumen_3",
        "n3": "naumen_3",
        "3": "naumen_3",

        # ВАЖНО: ваши алиасы
        "naumenseven": "naumen_3",   # 'NaumenSeven' → naumen_3
        "seven": "naumen_3",         # на всякий случай
        "nseven": "naumen_3",
        "naumen7": "naumen_3",
        "naumen_7": "naumen_3",
    }
    return mapping.get(s)

def _build_project_ids_by_source(dwh_conn, group: str, *, type_: str = "Operator", log=None) -> dict[str, list[str]]:
    getter = NaumenProjectsGetter()
    projects = getter.filter_projects_by_group(group, type_)
    buckets: dict[str, set[str]] = {"naumen_1": set(), "naumen_3": set()}
    unknown: dict[str, int] = {}

    for p in projects or []:
        if not isinstance(p, dict):
            continue
        raw_src = str(p.get("source", "") or "")
        norm = _normalize_naumen_source(raw_src)
        pid = p.get("project_id") or p.get("naumen_project_id") or p.get("id")
        if not pid:
            continue
        if norm in buckets:
            buckets[norm].add(str(pid))
        else:
            k = raw_src or "<EMPTY>"
            unknown[k] = unknown.get(k, 0) + 1

    out = {k: sorted(v) for k, v in buckets.items()}
    if log:
        log.info(f"[NAUMEN-RC] Project buckets: n1={len(out['naumen_1'])}, n3={len(out['naumen_3'])}")
        if unknown:
            log.warning(f"[NAUMEN-RC] Unknown sources encountered (not mapped): {unknown}")
    return out


def _resolve_naumen_conn(context, source_name: str):
    # 1) Пытаемся взять из агрегированного словаря ресурсов
    if hasattr(context.resources, "naumen_sources"):
        mapping = context.resources.naumen_sources
        key = {
            "source_naumen_1": "naumen_1",
            "source_naumen_3": "naumen_3",
            "naumen": "naumen_1",
            "naumen1": "naumen_1",
            "naumen3": "naumen_3",
            "n1": "naumen_1",
            "n3": "naumen_3",
        }.get(source_name, source_name)
        try:
            return mapping[key]
        except KeyError:
            raise RuntimeError(f"Unknown Naumen source '{source_name}'. Available: {list(mapping.keys())}")

    # 2) Иначе отдельные ресурсы
    normalized = _normalize_naumen_source(source_name)
    if normalized == "naumen_1" and hasattr(context.resources, "source_naumen_1"):
        return context.resources.source_naumen_1
    if normalized == "naumen_3" and hasattr(context.resources, "source_naumen_3"):
        return context.resources.source_naumen_3

    available = [k for k in ("source_naumen_1", "source_naumen_3") if hasattr(context.resources, k)]
    raise RuntimeError(
        f"Naumen connection not found for '{source_name}'. "
        f"Available resources: {available or 'none'}; or provide a 'naumen_sources' dict."
    )


def _get_shifts_by_view_id(dwh_conn, timetable_view_id: int) -> list[str]:
    """
    Возвращает список смен в формате 'HH:MM:SS' из bpm.timetable_data_detailed,
    беря TIME-часть из session_start для указанного timetable_view_id.
    """
    with dwh_conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT to_char(session_start::time, 'HH24:MI:SS') AS shift
            FROM bpm.timetable_data_detailed
            WHERE timetable_view_id = %s
            ORDER BY 1
            """,
            (timetable_view_id,),
        )
        shifts = [r[0] for r in cur.fetchall()]
        if not shifts:
            raise RuntimeError(
                f"В bpm.timetable_data_detailed нет смен для timetable_view_id={timetable_view_id}"
            )
        return shifts


def _get_default_start_date(start_cfg: Optional[str]) -> date:
    """
    Если передан start != 'default' → вернуть эту дату.
    Иначе → первое число текущего месяца по Asia/Bishkek.
    """
    if start_cfg and start_cfg != "default":
        return _parse_date_ymd(start_cfg)
    today = _today_local()
    return date(today.year, today.month, 1)


def _get_last_replicated_date(dwh_conn, start_time: str, group: str) -> Optional[date]:
    sql = f"""
        SELECT MAX(repeat_calls_period_date)::date
        FROM {TARGET_TABLE}
        WHERE start_time = %s AND "group" = %s
    """
    with dwh_conn.cursor() as cur:
        cur.execute(sql, (start_time, group))
        row = cur.fetchone()
        return row[0] if row and row[0] else None


def _load_users_and_staff_units(dwh_conn) -> Tuple[Dict[str, List[int]], Dict[int, List[Tuple[int, Optional[datetime], Optional[datetime]]]]]:
    """
    Возвращает:
      - login_to_user_ids: login -> [user_id, ...]
      - staff_by_user: user_id -> [(staff_unit_id, accepted_at, dismissed_at), ...] (DESC by accepted_at)
    """
    login_to_user_ids: Dict[str, List[int]] = {}
    staff_by_user: Dict[int, List[Tuple[int, Optional[datetime], Optional[datetime]]]] = {}

    with dwh_conn.cursor() as cur:
        cur.execute("SELECT id, login FROM bpm.users")
        for uid, login in cur.fetchall():
            if login is None:
                continue
            login_to_user_ids.setdefault(login, []).append(uid)

        cur.execute("""
            SELECT id, user_id, accepted_at, dismissed_at
            FROM bpm.staff_units
            ORDER BY accepted_at DESC NULLS LAST
        """)
        for sid, user_id, accepted_at, dismissed_at in cur.fetchall():
            staff_by_user.setdefault(user_id, []).append((sid, accepted_at, dismissed_at))

    return login_to_user_ids, staff_by_user

def _get_selected_project_ids(group: str, log=None) -> list[str]:
    getter = NaumenProjectsGetter()
    getter.filter_projects_by_group(group)          # ← аналог PHP-вызова
    selected = getattr(getter, "SelectedProjects", None) or []
    ids = [str(x) for x in selected if x is not None]
    ids = sorted(set(ids))
    if log: log.info(f"[NAUMEN-RC] SelectedProjects ids: {ids[:6]}{' ...' if len(ids)>6 else ''} (total={len(ids)})")
    return ids

def _compute_replication_start(dwh_conn, start_time: str, group: str, default_start_day: date, end_given: bool) -> date:
    """
    Считает replication_start так же, как раньше, но ВНЕ _execute_for_start_time,
    чтобы при запуске обоих источников внутри одного опа оба использовали ОДИНАКОВЫЙ старт.
    """
    if end_given:
        return default_start_day
    last_day = _get_last_replicated_date(dwh_conn, start_time, group)
    if last_day is None:
        return default_start_day
    return last_day + timedelta(days=1)


# --------------------------
# SQL-ЗАПРОСЫ (Naumen/Omni)
# --------------------------

# Выборка звонков за окно [start_date+start_time, end_date+start_time)
REPEAT_CALLS_SQL = """
WITH base AS (
  SELECT
    date_trunc('day', cl.connected - %(start_time)s::interval) AS repeat_calls_period_date,
    %(start_time)s::text AS start_time,
    cl.connected AS connected,
    cl.src_id AS msisdn,
    cl.session_id AS session_id,
    cl.dst_abonent AS employee_login,
    intervaltosec(date_trunc('second', cl.ended) - date_trunc('second', cl.created))::integer AS duration,
    row_number() OVER (
        PARTITION BY date_trunc('day', cl.connected - %(start_time)s::interval), cl.src_id
        ORDER BY cl.connected
    )::integer AS number_of_call,
    row_number() OVER (
        PARTITION BY date_trunc('day', cl.connected - %(start_time)s::interval), cl.session_id
        ORDER BY cl.connected
    )::integer AS number_of_call_transfer
  FROM call_legs cl
  LEFT JOIN queued_calls_ms qc
    ON qc.session_id = cl.session_id
   AND cl.created >= qc.unblocked_time
   AND cl.created <= qc.dequeued_time
  WHERE cl.dst_abonent_type = 'SP'
    AND qc.final_stage = 'operator'
    AND cl.incoming = 0
    AND cl.connected >= (%(start_date)s::timestamp + %(start_time)s::interval)
    AND cl.connected <  (%(end_date)s::timestamp   + %(start_time)s::interval)
    AND qc.project_id::text = ANY(%(project_ids)s::text[])
),
maxcalls AS (
  SELECT
    repeat_calls_period_date,
    msisdn,
    MAX(number_of_call) AS max_number_of_call,
    MAX(number_of_call_transfer) AS max_number_of_call_transfer
  FROM base
  GROUP BY 1, 2
)
SELECT
  b.repeat_calls_period_date,
  b.start_time,
  b.connected,
  b.msisdn,
  b.session_id,
  b.employee_login,
  b.duration,
  b.number_of_call,
  b.number_of_call_transfer,
  mc.max_number_of_call,
  mc.max_number_of_call_transfer
FROM base b
LEFT JOIN maxcalls mc
  ON mc.repeat_calls_period_date = b.repeat_calls_period_date
 AND mc.msisdn = b.msisdn
ORDER BY b.msisdn, b.connected;
"""

# Исключения из Omni: берём по каждой platform_session_id последнюю по created_at запись
EXCLUDED_CONTACTS_SQL = """
SELECT platform_session_id, started_at
FROM (
  SELECT
    platform_session_id,
    started_at,
    ROW_NUMBER() OVER (PARTITION BY platform_session_id ORDER BY created_at DESC) AS rn
  FROM omni.contacts
  WHERE started_at >= %(start_date)s::timestamp + %(start_time)s::interval
    AND started_at <  %(end_date)s::timestamp   + %(start_time)s::interval
    AND type = 'call'
    AND contact_status_id != 1
) t
WHERE rn = 1;
"""


def _fetch_repeat_calls(naumen_conn, start_date: date, end_date: date, start_time: str, project_ids: List[str]) -> list[dict[str, Any]]:
    with naumen_conn.cursor() as cur:
        cur.execute(
            REPEAT_CALLS_SQL,
            {
                "start_date": start_date.strftime("%Y-%m-%d"),
                "end_date": end_date.strftime("%Y-%m-%d"),
                "start_time": start_time,
                "project_ids": project_ids,  # psycopg2 адаптирует Python list -> ARRAY
            },
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def _fetch_excluded_contacts(omni_conn, start_date: date, end_date: date, start_time: str) -> List[Dict[str, Any]]:
    with omni_conn.cursor() as cur:
        cur.execute(
            EXCLUDED_CONTACTS_SQL,
            {
                "start_date": start_date.strftime("%Y-%m-%d"),
                "end_date": end_date.strftime("%Y-%m-%d"),
                "start_time": start_time,
            },
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def _insert_rows(dwh_conn, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    keys = [
        "repeat_calls_period_date",
        "start_time",
        "connected",
        "msisdn",
        "session_id",
        "user_login",
        "staff_unit_id",
        "duration",
        "number_of_call",
        "max_number_of_call",
        "number_of_call_transfer",
        "max_number_of_call_transfer",
        "resolve_status",
        "group",
        "created_at",
        "updated_at",
    ]
    cols_sql = ", ".join(f'"{k}"' if k == "group" else k for k in keys)  # "group" зарезервировано
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

@op(
    required_resource_keys={"source_dwh", "source_omni", "source_naumen_1", "source_naumen_3"},
    config_schema={
        "start": Field(str, default_value="default"),
        "end": Field(str, is_required=False),
    },
    description="Репликация Naumen Repeat Calls по сменам в stat.replication_repeat_calls_data",
)
def naumen_repeat_call_data(context) -> None:
    log = context.log
    dwh = context.resources.source_dwh
    omni = context.resources.source_omni

    start_arg = context.op_config.get("start", "default")
    end_arg = context.op_config.get("end")
    default_start_day = _get_default_start_date(start_arg)
    end_day = _parse_date_ymd(end_arg) if end_arg else _yesterday_local()
    end_given = bool(end_arg)

    shifts = _get_shifts_by_view_id(dwh, VIEW_ID_FOR_SHIFTS)
    special_midnight = "00:00:00" in shifts

    # Подготовим кэши
    login_to_user_ids, staff_by_user = _load_users_and_staff_units(dwh)

    # <<< ГЛАВНОЕ: идём по жёстко заданным задачам
    for task in TASKS:
        group = task["group"]
        conn_mode = task["connection"].lower()

        # Резолвим источник(и) под конкретную задачу
        if conn_mode in ("naumen", "naumen_1", "n1"):
            sources_to_run = ["naumen_1"]
        elif conn_mode in ("naumen3", "naumen_3", "n3"):
            sources_to_run = ["naumen_3"]
        elif conn_mode == "both":
            sources_to_run = ["naumen_1", "naumen_3"]
        else:
            log.warning(f"[NAUMEN-RC] group='{group}': неизвестный connection='{conn_mode}', пропуск.")
            continue

        conns: dict[str, any] = {}
        for src in sources_to_run:
            try:
                conns[src] = _resolve_naumen_conn(context, src)
            except RuntimeError as e:
                log.error(f"[NAUMEN-RC] Conn resolve failed for {src}: {e}")

        if not conns:
            log.warning(f"[NAUMEN-RC] group='{group}': нет доступных коннектов, пропуск.")
            continue

        # Список проектов для группы (как в PHP SelectedProjects)
        pids = _get_selected_project_ids(group, log)
        if not pids:
            log.warning(f"[NAUMEN-RC] group='{group}': список проектов пуст — пропуск.")
            continue

        log.info(f"[NAUMEN-RC] >>> START group='{group}', sources={list(conns.keys())}, projects={len(pids)}")

        # 1) сначала 00:00:00
        if special_midnight:
            st = "00:00:00"
            base_start = _compute_replication_start(dwh, st, group, default_start_day, end_given)
            for src, conn in conns.items():
                _execute_for_start_time(
                    context=context,
                    naumen_conn=conn,
                    dwh_conn=dwh,
                    omni_conn=omni,
                    start_time=st,
                    default_start_day=default_start_day,
                    end_day=end_day,
                    group=group,
                    project_ids=pids,
                    login_to_user_ids=login_to_user_ids,
                    staff_by_user=staff_by_user,
                    replication_start_override=base_start,
                )

        # 2) остальные смены (без повторного 00:00:00)
        for st in shifts:
            if special_midnight and st == "00:00:00":
                continue
            base_start = _compute_replication_start(dwh, st, group, default_start_day, end_given)
            for src, conn in conns.items():
                _execute_for_start_time(
                    context=context,
                    naumen_conn=conn,
                    dwh_conn=dwh,
                    omni_conn=omni,
                    start_time=st,
                    default_start_day=default_start_day,
                    end_day=end_day,
                    group=group,
                    project_ids=pids,
                    login_to_user_ids=login_to_user_ids,
                    staff_by_user=staff_by_user,
                    replication_start_override=base_start,
                )

        log.info(f"[NAUMEN-RC] <<< DONE group='{group}'")

    log.info("[NAUMEN-RC] Репликация закончена")


def _execute_for_start_time(
    context,
    naumen_conn,
    dwh_conn,
    omni_conn,
    start_time: str,
    default_start_day: date,
    end_day: date,
    group: str,
    project_ids: list[str],
    login_to_user_ids: Dict[str, List[int]],
    staff_by_user: Dict[int, List[Tuple[int, Optional[datetime], Optional[datetime]]]],
    replication_start_override: Optional[date] = None,   # ← новое
) -> None:
    log = context.log

    if replication_start_override is not None:
        replication_start = replication_start_override
    else:
        end_given = bool(context.op_config.get("end"))
        last_day = _get_last_replicated_date(dwh_conn, start_time, group)
        if end_given or last_day is None:
            replication_start = default_start_day
        else:
            replication_start = last_day + timedelta(days=1)

    replication_end = end_day
    log.info(f"[NAUMEN-RC] StartTime={start_time}; start={replication_start} end(exclusive)={replication_end} group='{group}'")

    # Пока дата начала < конца
    # В PHP end трактуется как включение дня-1 (они делают endDate = переданная дата, а фактически окно [start, end))
    curr = replication_start
    batch_size = 2000  # на вставку
    inserted_total = 0
    while curr < replication_end:
        next_day = curr + timedelta(days=1)
        log.info(f"Дата репликации: {curr:%d.%m.%Y}, {start_time}")

        # Выборки
        calls = _fetch_repeat_calls(
            naumen_conn=naumen_conn,
            start_date=curr,
            end_date=next_day,
            start_time=start_time,
            project_ids=project_ids,
        )
        excluded = _fetch_excluded_contacts(
            omni_conn=omni_conn,
            start_date=curr,
            end_date=next_day,
            start_time=start_time,
        )

        log.info(f"Найдено {len(calls)} записей; потенциальных исключений по запросам {len(excluded)}")

        # Индексируем исключения по session_id
        excl_by_session: Dict[str, List[datetime]] = {}
        for e in excluded:
            sid = e.get("platform_session_id")
            st = e.get("started_at")
            if not sid or not st:
                continue
            if isinstance(st, str):
                # ожидаем TIMESTAMP (psycopg2 обычно даёт datetime),
                # но на всякий случай конвертируем
                st = _dt(st)
            excl_by_session.setdefault(str(sid), []).append(st)

        # Подготовим записи к вставке
        rows: List[Dict[str, Any]] = []

        now_dt = datetime.now(TZ).replace(tzinfo=None)

        for r in calls:
            # Поля из запроса
            period_day = r["repeat_calls_period_date"]
            if isinstance(period_day, datetime):
                period_day = period_day.date()
            start_time_str = r["start_time"]

            conn_dt = r["connected"] if isinstance(r["connected"], datetime) else _dt(r["connected"])

            if isinstance(conn_dt, datetime) and conn_dt.tzinfo is not None:
                conn_dt = conn_dt.replace(tzinfo=None)

            msisdn = r["msisdn"]
            session_id = str(r["session_id"])
            login = r["employee_login"]
            duration = int(r["duration"] or 0)
            number_of_call = int(r["number_of_call"])
            number_of_call_transfer = int(r["number_of_call_transfer"])
            max_number_of_call = int(r["max_number_of_call"])
            max_number_of_call_transfer = int(r["max_number_of_call_transfer"])

            # Разрешение staff_unit_id на дату curr (как в PHP)
            staff_unit_id = _pick_staff_unit_id(
                login=login,
                as_of_day=curr,
                login_to_user_ids=login_to_user_ids,
                staff_by_user=staff_by_user,
            )

            # Статус по правилам
            resolution = "resolved"
            try:
                if duration > 10 and (number_of_call is not None) and (max_number_of_call is not None) and (number_of_call != max_number_of_call):
                    resolution = "not_resolved"
                if (number_of_call_transfer is not None) and (max_number_of_call_transfer is not None) and (number_of_call_transfer != max_number_of_call_transfer):
                    resolution = "neutral"
                if msisdn in ("anonymous", "996anonymous"):
                    resolution = "neutral"
            except Exception:
                # На случай неожиданного None/типов — оставим 'resolved'
                pass

            # Проверка исключений из Omni (по окну [connected, connected+duration])
            try:
                end_dt = conn_dt + timedelta(seconds=max(0, duration))
                for excl_ts in excl_by_session.get(session_id, []) or []:
                    if conn_dt <= excl_ts <= end_dt:
                        resolution = "neutral"
                        break
            except Exception:
                pass

            rows.append({
                "repeat_calls_period_date": period_day,
                "start_time": start_time_str,
                "connected": conn_dt,
                "msisdn": msisdn,
                "session_id": session_id,
                "user_login": login,
                "staff_unit_id": staff_unit_id,
                "duration": duration,
                "number_of_call": number_of_call,
                "max_number_of_call": max_number_of_call,
                "number_of_call_transfer": number_of_call_transfer,
                "max_number_of_call_transfer": max_number_of_call_transfer,
                "resolve_status": resolution,
                "group": group,
                "created_at": now_dt,
                "updated_at": now_dt,
            })

            # Пакетная вставка порциями
            if len(rows) >= batch_size:
                _insert_rows(dwh_conn, rows)
                inserted_total += len(rows)
                rows.clear()

        if rows:
            _insert_rows(dwh_conn, rows)
            inserted_total += len(rows)
            rows.clear()

        # Следующий день
        curr = next_day

    log.info(f"[NAUMEN-RC] StartTime={start_time} — вставлено строк: {inserted_total}")