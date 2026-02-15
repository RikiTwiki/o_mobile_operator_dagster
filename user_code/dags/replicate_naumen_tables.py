# replicate_naumen_tables.py
# Python 3.11, Dagster ≥ 1.8, psycopg2-binary
# Универсальная репликация таблиц из двух Naumen (naumen_1, naumen_3)
# в схему "naumen" базы CCDWH. Поддерживает инкремент по watermark-колонке
# (например, updated_at/modified/created) и UPSERT по (PK..., source).
# Для каждой записи добавляет: source ("Naumen" | "Naumen3") и synced_at.
#
# Требуемые ресурсы Dagster: source_naumen_1, source_naumen_3, source_dwh
# (psycopg2 connections). Пример конфигурации см. внизу файла.

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterable, List, Optional, Sequence, Dict, Tuple

from dagster import op, job, ScheduleDefinition, Field, StringSource, IntSource
import psycopg2
from psycopg2.extras import execute_values, Json

GLOBAL_DEFAULT_START = "2025-06-01 00:00:00"

# ===== описание таблиц =====
@dataclass
class TableSpec:
    name: str
    wm: Optional[str]                      # watermark в источнике
    pk: Sequence[str]                      # бизнес-ключ(и) в DWH (для ON CONFLICT), всегда + source
    rename: Dict[str, str] = field(default_factory=dict)  # src_col -> tgt_col
    src_schema: str = "public"
    tgt_schema: str = "naumen"

TABLE_SPECS: List[TableSpec] = [
    TableSpec("call_legs",               wm="created",        pk=["naumen_id"],  rename={"id": "naumen_id"}),
    TableSpec("queued_calls_ms",         wm="enqueued_time",  pk=["naumen_id"], rename={"id": "naumen_id"}),
    TableSpec("detail_outbound_sessions_ms", wm="attempt_start", pk=["session_id", "attempt_start", "attempt_number", "project_id"]),
    TableSpec("mv_call_case",            wm="creationdate",   pk=["uuid"]),
    TableSpec("mv_case_history",         wm="historyitemdate",pk=["historyitemuuid"]),
    TableSpec("mv_contact_history",      wm="contactdate",    pk=["uuid"]),
    TableSpec("mv_custom_form",          wm="creationdate",   pk=["uuid"]),
    TableSpec("mv_employee",             wm="creationdate",   pk=["uuid"]),
    TableSpec("mv_event_log",            wm="event_time",     pk=["uuid"]),
    TableSpec("mv_participant_history",  wm="begindate",      pk=["uuid"]),
    TableSpec("mv_phone_call",           wm="creationdate",   pk=["uuid"]),
    TableSpec("status_changes_ms",       wm="entered",        pk=["sequence_id"]),

    TableSpec("mv_outcoming_call_project", wm="creationdate", pk=["uuid"]),

    TableSpec("mv_incoming_call_project",  wm="creationdate", pk=["uuid"]),

]

def fetch_target_column_types(dst_conn, schema: str, table: str) -> dict[str, str]:
    sql = """
    SELECT column_name, COALESCE(udt_name, data_type) AS t
    FROM information_schema.columns
    WHERE table_schema=%s AND table_name=%s
    """
    with dst_conn.cursor() as cur:
        cur.execute(sql, (schema, table))
        return {name: t for name, t in cur.fetchall()}

def fetch_target_columns(dst_conn, schema: str, table: str) -> List[str]:
    sql = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema=%s AND table_name=%s
    ORDER BY ordinal_position
    """
    with dst_conn.cursor() as cur:
        cur.execute(sql, (schema, table))
        return [r[0] for r in cur.fetchall()]

# ===== утилиты =====
def qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'

def list_cols(cols: Sequence[str]) -> str:
    return ", ".join(qident(c) for c in cols)

WATERMARKS_TABLE = 'naumen.__replication_watermarks'
CREATE_WM_SQL = f"""
CREATE TABLE IF NOT EXISTS {WATERMARKS_TABLE} (
    table_name   text        NOT NULL,
    source_label text        NOT NULL,
    watermark    text        NOT NULL,
    updated_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (table_name, source_label)
);
"""
GET_WM_SQL   = f"SELECT watermark FROM {WATERMARKS_TABLE} WHERE table_name=%s AND source_label=%s"
UPSERT_WM_SQL = f"""
INSERT INTO {WATERMARKS_TABLE} (table_name, source_label, watermark)
VALUES (%s, %s, %s)
ON CONFLICT (table_name, source_label) DO UPDATE
SET watermark = EXCLUDED.watermark, updated_at = now();
"""

def ensure_watermarks_table(dst_conn):
    with dst_conn, dst_conn.cursor() as cur:
        cur.execute(CREATE_WM_SQL)

def get_watermark(dst_conn, table_name: str, source_label: str) -> Optional[str]:
    with dst_conn.cursor() as cur:
        cur.execute(GET_WM_SQL, (table_name, source_label))
        row = cur.fetchone()
        return row[0] if row else None

def upsert_watermark(dst_conn, table_name: str, source_label: str, watermark: str) -> None:
    with dst_conn, dst_conn.cursor() as cur:
        cur.execute(UPSERT_WM_SQL, (table_name, source_label, watermark))

def fetch_source_columns(src_conn, schema: str, table: str) -> List[str]:
    sql = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema=%s AND table_name=%s
    ORDER BY ordinal_position
    """
    with src_conn.cursor() as cur:
        cur.execute(sql, (schema, table))
        return [r[0] for r in cur.fetchall()]

def ensure_target_columns(dst_conn, spec: TableSpec):
    """Добавляем служебные колонки в целевую таблицу (если нет): source, synced_at."""
    tgt = f"{qident(spec.tgt_schema)}.{qident(spec.name)}"
    ddl = f"""
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='{spec.tgt_schema}' AND table_name='{spec.name}' AND column_name='source'
        ) THEN
            EXECUTE 'ALTER TABLE {tgt} ADD COLUMN source text NOT NULL DEFAULT ''Naumen''';
        END IF;

        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='{spec.tgt_schema}' AND table_name='{spec.name}' AND column_name='synced_at'
        ) THEN
            EXECUTE 'ALTER TABLE {tgt} ADD COLUMN synced_at timestamptz NOT NULL DEFAULT now()';
        END IF;
    END$$;
    """
    with dst_conn, dst_conn.cursor() as cur:
        cur.execute(ddl)

def ensure_unique_pk_source(dst_conn, spec: TableSpec):
    """
    Гарантируем наличие UNIQUE (pk..., source) для ON CONFLICT.
    Если ограничения с таким набором колонок нет — создаём с неколлизирующим именем.
    """
    desired_cols = list(spec.pk) + ["source"]

    # 1) Уже есть ограничение с ТОЧНО таким набором колонок? Тогда ничего не делаем.
    with dst_conn.cursor() as cur:
        cur.execute("""
        SELECT 1
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        JOIN LATERAL (
            SELECT array_agg(a.attname::text ORDER BY x.ord) AS cols
            FROM unnest(c.conkey) WITH ORDINALITY AS x(attnum, ord)
            JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = x.attnum
        ) s ON TRUE
        WHERE n.nspname = %s
          AND t.relname  = %s
          AND c.contype IN ('u','p')
          AND s.cols     = %s::text[];
        """, (spec.tgt_schema, spec.name, desired_cols))
        exists_by_cols = cur.fetchone() is not None

    if exists_by_cols:
        return  # всё уже есть

    # 2) Подберём СВОБОДНОЕ имя для нового UNIQUE
    base = f"{spec.name}_{'_'.join(spec.pk)}_source_uq"
    # укоротим, чтобы поместились суффиксы и лимит 63 символа
    if len(base) > 56:
        base = base[:56]
    candidate = base
    with dst_conn.cursor() as cur:
        i = 0
        while True:
            cur.execute("SELECT 1 FROM pg_constraint WHERE conname = %s", (candidate,))
            if cur.fetchone() is None:
                break
            i += 1
            candidate = f"{base}_{i}"

    # 3) Создадим ограничение
    cols_sql = list_cols(desired_cols)
    with dst_conn, dst_conn.cursor() as cur:
        cur.execute(
            f"ALTER TABLE {qident(spec.tgt_schema)}.{qident(spec.name)} "
            f"ADD CONSTRAINT {qident(candidate)} UNIQUE ({cols_sql})"
        )

def build_select_sql(spec: TableSpec, src_cols: Sequence[str], since: Optional[str], inclusive: bool) -> Tuple[str, list]:
    base = f"SELECT {list_cols(src_cols)} FROM {qident(spec.src_schema)}.{qident(spec.name)}"
    where_parts: List[str] = []
    params: List[str] = []
    order_clause = ""
    if spec.wm and spec.wm in src_cols:
        if since is not None:
            op = ">=" if inclusive else ">"
            where_parts.append(f"{qident(spec.wm)} {op} %s")
            params.append(since)
        order_clause = f" ORDER BY {qident(spec.wm)} ASC"
    sql = base + (" WHERE " + " AND ".join(where_parts) if where_parts else "") + order_clause
    return sql, params

def upsert_batch(dst_conn, spec: TableSpec, src_cols: Sequence[str], rows: Iterable[Sequence], source_label: str):
    # 1) источ. колонки без служебных
    filtered_src_cols = [c for c in src_cols if c not in ("source", "synced_at")]
    idx_src = {c: i for i, c in enumerate(filtered_src_cols)}

    # 2) реальные колонки цели
    tgt_all = fetch_target_columns(dst_conn, spec.tgt_schema, spec.name)
    tgt_types = fetch_target_column_types(dst_conn, spec.tgt_schema, spec.name)
    json_like = {c for c, t in tgt_types.items() if t in ("json", "jsonb")}

    # 3) пары (src -> tgt) с учётом переименования и наличия в цели
    col_pairs = []
    for c in filtered_src_cols:
        tgt_c = spec.rename.get(c, c)
        if tgt_c in tgt_all:
            col_pairs.append((c, tgt_c))

    if not col_pairs:
        return  # нечего вставлять

    # ключевые поля должны быть среди выбранных таргет-колонок
    tgt_selected = [t for _, t in col_pairs]
    missing_pk = [k for k in spec.pk if k not in tgt_selected]
    if missing_pk:
        raise RuntimeError(f"[{spec.name}] В INSERT отсутствуют ключевые колонки {missing_pk} (rename={spec.rename}).")

    insert_cols = tgt_selected + ["source", "synced_at"]

    key_set = set(spec.pk) | {"source"}
    set_cols = [c for c in tgt_selected if c not in key_set]
    set_clause = ", ".join([f'{qident(c)} = EXCLUDED.{qident(c)}' for c in set_cols] + ["synced_at = EXCLUDED.synced_at"])

    now = datetime.utcnow()

    def row_gen():
        for r in rows:
            vals = []
            for s, t in col_pairs:
                v = r[idx_src[s]]
                # критично: dict нужно оборачивать ТОЛЬКО если целевая колонка json/jsonb
                if t in json_like and isinstance(v, (dict, list)):
                    v = Json(v)
                vals.append(v)
            vals += [source_label, now]
            yield vals

    tgt = f"{qident(spec.tgt_schema)}.{qident(spec.name)}"
    conflict_cols = list_cols(list(spec.pk) + ["source"])
    sql = (
        f"INSERT INTO {tgt} ({list_cols(insert_cols)}) VALUES %s "
        f"ON CONFLICT ({conflict_cols}) DO UPDATE SET {set_clause}"
    )
    with dst_conn.cursor() as cur:
        execute_values(cur, sql, row_gen(), page_size=5000)

def replicate_table_for_source(context, spec: TableSpec, src_conn, dst_conn, source_label: str, batch_size: int) -> int:
    context.log.info(f"[{spec.name} @ {source_label}] старт")

    ensure_watermarks_table(dst_conn)
    ensure_target_columns(dst_conn, spec)
    ensure_unique_pk_source(dst_conn, spec)

    # колонки источника
    src_cols = fetch_source_columns(src_conn, spec.src_schema, spec.name)
    # проверим ключевые (через обратное переименование)
    reverse_rename = {v: k for k, v in spec.rename.items()}
    for k in spec.pk:
        sc = reverse_rename.get(k, k)
        if sc not in src_cols:
            context.log.warning(f"[{spec.name}] в источнике нет ключевой колонки '{sc}' (для pk '{k}')")

    # watermark
    wm_key = f"{spec.src_schema}.{spec.name}"
    since = get_watermark(dst_conn, wm_key, source_label)
    first_run = False
    if spec.wm and since is None:
        since = GLOBAL_DEFAULT_START
        first_run = True
        context.log.info(f"[{spec.name} @ {source_label}] нет watermark — стартуем с {since} (включительно)")

    filtered_src_cols = [c for c in src_cols if c not in ("source", "synced_at")]
    select_sql, params = build_select_sql(spec, filtered_src_cols, since, inclusive=first_run)
    context.log.debug(f"[{spec.name} @ {source_label}] SELECT: {select_sql}")
    context.log.debug(f"[{spec.name} @ {source_label}] params: {params}")

    total = 0
    wm_idx = (filtered_src_cols.index(spec.wm) if (spec.wm and spec.wm in filtered_src_cols) else None)

    server_side = not getattr(src_conn, "autocommit", False)
    cursor_kwargs = {"name": f"cur_{spec.name}_{source_label}"} if server_side else {}

    with src_conn.cursor(**cursor_kwargs) as cur:
        if server_side:
            cur.itersize = batch_size
        cur.execute(select_sql, params)

        # индексы ключей для дедупа (по src-именам)
        pk_src_cols = [reverse_rename.get(k, k) for k in spec.pk if reverse_rename.get(k, k) in filtered_src_cols]
        pk_idx = [filtered_src_cols.index(c) for c in pk_src_cols] if pk_src_cols else []

        while True:
            batch = cur.fetchmany(batch_size)
            if not batch:
                break

            # дедуп в батче по ключу (без source, он фиксирован в вызове)
            if pk_idx:
                before = len(batch)
                ded = {}
                for r in batch:
                    key = tuple(r[i] for i in pk_idx)
                    ded[key] = r
                batch = list(ded.values())
                removed = before - len(batch)
                if removed > 0:
                    context.log.warning(f"[{spec.name} @ {source_label}] удалено дублей: {removed} по ключу {spec.pk}")

            wm_vals = None
            if wm_idx is not None:
                wm_vals = [r[wm_idx] for r in batch if r[wm_idx] is not None]
                if wm_vals:
                    context.log.info(f"[{spec.name} @ {source_label}] батч {len(batch)}; {spec.wm} ∈ [{min(wm_vals)} .. {max(wm_vals)}]")

            upsert_batch(dst_conn, spec, filtered_src_cols, batch, source_label)
            total += len(batch)
            context.log.info(f"[{spec.name} @ {source_label}] +{len(batch)} (итого {total})")

            if spec.wm and wm_vals:
                upsert_watermark(dst_conn, wm_key, source_label, str(max(wm_vals)))

    context.log.info(f"[{spec.name} @ {source_label}] готово: {total} строк")
    return total

# ===== Dagster op / job / schedule =====
@op(
    required_resource_keys={"source_naumen_1", "source_naumen_3", "source_ccdwh_resource"},
    config_schema={
        "batch_size": IntSource,
        "source_labels": {
            "naumen_1": StringSource,  # значение попадёт в колонку source
            "naumen_3": StringSource,
        },
        "only_tables": Field(list, is_required=False, default_value=[]),  # можно ограничить запуск
    },
)
def replicate_naumen_tables(context):
    cfg = context.op_config
    src1 = context.resources.source_naumen_1
    src3 = context.resources.source_naumen_3
    dst  = context.resources.source_ccdwh_resource

    batch_size: int = cfg["batch_size"]
    label1: str = cfg["source_labels"]["naumen_1"]  # "Naumen"
    label3: str = cfg["source_labels"]["naumen_3"]  # "Naumen3"
    only: List[str] = cfg.get("only_tables") or []

    specs = [s for s in TABLE_SPECS if not only or s.name in only]
    context.log.info("[RC] К репликации: " + ", ".join(s.name for s in specs))

    total_all = 0
    for spec in specs:
        total_all += replicate_table_for_source(context, spec, src1, dst, label1, batch_size=batch_size)
        total_all += replicate_table_for_source(context, spec, src3, dst, label3, batch_size=batch_size)

    context.log.info(f"[RC] <<< Готово. Всего обработано строк: {total_all}")