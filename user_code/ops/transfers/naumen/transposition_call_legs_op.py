# replicate_call_legs_job.py
# Python 3.11, Dagster ≥ 1.8, psycopg2-binary
# Задача: каждые 2 часа забирать свежие строки из call_legs
# из продовых БД Naumen1 и Naumen3 и апсертом класть в ccdwh.naumen.call_legs
# Отличия источников фиксируем в колонке "source" (= 'Naumen' | 'Naumen3').
# Конфликт (upsert) — по (source, id). Предполагается, что в целевой таблице
# существует уникальный индекс/PK: UNIQUE (source, id).
# Окно репликации: последний выровненный часовой тайм-слот; переносим последние
# window_hours (по умолчанию 2) с дополнительным lookback (по умолчанию 4) на случай
# поздних обновлений. Фильтрация по time_column (updated_at, если нет — created_at).

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, List, Sequence, Tuple

import pytz
import psycopg2
from psycopg2.extras import execute_values
from dagster import Field, In, Nothing, Out, get_dagster_logger, job, op, ScheduleDefinition

# === Константы ===
TZ = pytz.timezone("Asia/Bishkek")
DEFAULT_SOURCE_SCHEMA = "public"           # при необходимости поменять
DEST_SCHEMA = "naumen"
DEST_TABLE = "call_legs"

# === Вспомогательные функции ===

def _floor_to_hour(dt: datetime) -> datetime:
    return dt.replace(minute=0, second=0, microsecond=0)


def _choose_time_column(conn: psycopg2.extensions.connection, source_schema: str, table: str, fallback: str = "created_at") -> str:
    """Проверяет наличие updated_at; иначе возвращает fallback."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
              FROM information_schema.columns
             WHERE table_schema = %s
               AND table_name   = %s
               AND column_name  IN ('updated_at', %s)
            """,
            (source_schema, table, fallback),
        )
        cols = {r[0] for r in cur.fetchall()}
        return "updated_at" if "updated_at" in cols else fallback


def _fetch_iter(
    conn: psycopg2.extensions.connection,
    sql: str,
    params: Tuple[Any, ...],
    itersize: int = 10_000,
) -> Iterable[List[Tuple[Any, ...]]]:
    """Итерируемся пачками, чтобы не держать всё в памяти."""
    with conn.cursor() as cur:
        cur.itersize = itersize
        cur.execute(sql, params)
        cols = [d.name for d in cur.description]
        while True:
            rows = cur.fetchmany(itersize)
            if not rows:
                break
            yield cols, rows


def _upsert_batch(
    dest_conn: psycopg2.extensions.connection,
    dest_schema: str,
    dest_table: str,
    source_label: str,
    cols: Sequence[str],
    rows: Sequence[Tuple[Any, ...]],
):
    logger = get_dagster_logger()
    if "id" not in cols:
        raise RuntimeError("В выборке нет колонки id — нужен ключ для upsert.")

    # Целевые колонки: все из источника + 'source'
    dest_cols = list(cols) + ["source"]

    # Список колонок для апдейта (все, кроме id и source)
    update_cols = [c for c in cols if c not in ("id", "source")]
    set_clause = ", ".join([f'{c} = EXCLUDED.{c}' for c in update_cols])

    # Шаблон VALUES — %s для каждого столбца + source в конце
    template = "(" + ", ".join(["%s"] * len(cols)) + ", %s)"

    data = [tuple(r) + (source_label,) for r in rows]

    sql = f"""
        INSERT INTO {dest_schema}.{dest_table} ({', '.join(dest_cols)})
        VALUES %s
        ON CONFLICT (source, id) DO UPDATE SET {set_clause}
    """
    with dest_conn.cursor() as cur:
        execute_values(cur, sql, data, template=template, page_size=10_000)
    logger.info(f"UPSERT {len(rows)} rows into {dest_schema}.{dest_table} for source={source_label}")


# === OP: репликация для одного источника ===
@op(
    required_resource_keys={"naumen_1", "naumen_3", "ccdwh"},
    ins={"start": In(Nothing)},
    out=Out(Nothing),
    config_schema={
        "resource_key": Field(str, description="naumen_1 | naumen_3"),
        "source_label": Field(str, description="Метка источника в целевой таблице: 'Naumen' | 'Naumen3'"),
        "source_schema": Field(str, default_value=DEFAULT_SOURCE_SCHEMA),
        "dest_schema": Field(str, default_value=DEST_SCHEMA),
        "dest_table": Field(str, default_value=DEST_TABLE),
        "window_hours": Field(int, default_value=2, description="Сколько часов переносим"),
        "lookback_hours": Field(int, default_value=4, description="Добавочный лаг для поздних апдейтов"),
        "time_column": Field(str, default_value="auto", description="updated_at | created_at | auto"),
        "extra_filter": Field(str, is_required=False, description="Дополнительный SQL-фильтр в WHERE, без слова WHERE"),
    },
)
def replicate_call_legs_for_source(context) -> None:
    logger = context.log
    cfg = context.op_config

    # Определяем подключения
    if cfg["resource_key"] == "naumen_1":
        src = context.resources.naumen_1
    elif cfg["resource_key"] == "naumen_3":
        src = context.resources.naumen_3
    else:
        raise RuntimeError("resource_key должен быть 'naumen_1' или 'naumen_3'")
    dest = context.resources.ccdwh

    # Выровняем конец окна на час и отбросим текущий незавершённый час
    now_local = datetime.now(TZ)
    end_aligned = _floor_to_hour(now_local)

    # Основное окно + лаг
    hours = max(int(cfg["window_hours"]), 1)
    lag = max(int(cfg["lookback_hours"]), 0)
    start_ts = end_aligned - timedelta(hours=hours + lag)
    end_ts = end_aligned

    logger.info(
        f"[call_legs] source={cfg['source_label']} window=({start_ts.isoformat()} → {end_ts.isoformat()}) "
        f"hours={hours} lag={lag}"
    )

    # Определяем time_column
    source_schema = cfg["source_schema"]
    if cfg["time_column"] == "auto":
        time_col = _choose_time_column(src, source_schema, "call_legs", fallback="created_at")
    else:
        time_col = cfg["time_column"]

    # Собираем SQL выборки
    where = f"{time_col} >= %s AND {time_col} < %s"
    if cfg.get("extra_filter"):
        where += f" AND (" + cfg["extra_filter"] + ")"

    select_sql = f"SELECT * FROM {source_schema}.call_legs WHERE {where}"

    total = 0
    for cols, rows in _fetch_iter(src, select_sql, (start_ts.astimezone(timezone.utc), end_ts.astimezone(timezone.utc))):
        _upsert_batch(dest, cfg["dest_schema"], cfg["dest_table"], cfg["source_label"], cols, rows)
        total += len(rows)
        dest.commit()

    logger.info(f"[call_legs] source={cfg['source_label']} replicated rows: {total}")


# === Стартовая заглушка, чтобы связать multiple ops в один job ===
@op(out=Out(Nothing))
def start() -> None:
    return None