import os
import sys
import json
import logging
from datetime import datetime
from typing import Any, Dict, List

import psycopg2

# ---------- env ----------
ENV_PATH = r"C:\Users\arenadov\PycharmProjects\test\.env"

def _load_env(path: str) -> None:
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv(dotenv_path=path, override=True)
    except Exception:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k.strip(), v.strip())

_load_env(ENV_PATH)

# ---------- sys.path so imports work ----------
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# ---------- logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("hour_mp_test")

# ---------- imports from your repo ----------
from utils.getters.messaging_platform_data_getter import MessagingPlatformDataGetter

# left_join_array with fallback
try:
    from resources.report_utils import left_join_array  # type: ignore
    log.info("Using left_join_array from resources.report_utils")
except Exception:
    def left_join_array(left: List[Dict[str, Any]], right: List[Dict[str, Any]], key: str) -> List[Dict[str, Any]]:
        r_index = {r[key]: r for r in (right or []) if key in r}
        out: List[Dict[str, Any]] = []
        for row in (left or []):
            merged = dict(row)
            match = r_index.get(row.get(key))
            if match:
                for k, v in match.items():
                    if k != key and k not in merged:
                        merged[k] = v
            out.append(merged)
        return out
    log.warning("Using fallback left_join_array")

# to_hms with fallback
def _fallback_to_hms(val: Any) -> str:
    from datetime import datetime as _dt
    if isinstance(val, _dt):
        return val.strftime("%H:%M:%S")
    if isinstance(val, (int, float)):
        s = int(val)
        h = s // 3600
        m = (s % 3600) // 60
        sec = s % 60
        return f"{h:02d}:{m:02d}:{sec:02d}"
    if isinstance(val, str):
        s = val.strip()
        if " " in s and len(s) >= 19:  # 'YYYY-MM-DD HH:MM:SS'
            return s.split(" ")[1][:8]
        if len(s) >= 5 and s[2] == ":":
            return s[:8] if len(s) >= 8 else s + ":00"
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%d.%m.%Y %H:%M:%S"):
            try:
                return _dt.strptime(s, fmt).strftime("%H:%M:%S")
            except Exception:
                pass
    return str(val)

try:
    from function_helpers import to_hms  # type: ignore
    log.info("Using to_hms from function_helpers")
except Exception:
    try:
        from resources.report_utils import to_hms  # type: ignore
        log.info("Using to_hms from resources.report_utils")
    except Exception:
        to_hms = _fallback_to_hms  # type: ignore
        log.warning("Using fallback to_hms")

# ---------- tiny dagster-like context ----------
class _Context:
    def __init__(self):
        self.log = log

# ---------- pg helpers ----------
def _pg_connect(host: str, port: int, user: str, password: str, dbname: str):
    return psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname=dbname,
        options="-c timezone=Asia/Bishkek",
    )

def _set_search_path(conn, schema: str):
    if not schema:
        return
    try:
        with conn.cursor() as cur:
            cur.execute(f"SET search_path TO {schema}")
        conn.commit()
    except Exception as e:
        log.warning(f"Could not set search_path to '{schema}': {e}")

# ---------- core logic ----------
def hour_mp_data_messenger_detailed_like(
    report_date: datetime,
    start_date: datetime,
    end_date: datetime,
    project_ids: List[int],
    conn_dwh: Any,
    conn_cp: Any,
) -> List[Dict[str, Any]]:
    """
    Standalone equivalent of hour_mp_data_messenger_detailed_op.
    """
    context = _Context()

    # Getter for DWH, trunc='hour', start_date=report_date (как в @op)
    mp_getter = MessagingPlatformDataGetter.prepare_mp_data_getter(
        trunc='hour',
        start_date=report_date,
        end_date=end_date,
        report_date=report_date,
        conn=conn_dwh,
        project_ids=project_ids
    )
    mp_getter.context = context

    context.log.info(f"Dates: {(start_date, mp_getter.start_date, report_date, end_date)}")
    # Логируем параметры, как в @op
    try:
        context.log.info(f"PARAMS: {(mp_getter.trunc, mp_getter.date_field, mp_getter.date_format)}")
    except Exception:
        context.log.info("PARAMS: (could not read trunc/date_field/date_format)")

    # 1) агрегаты
    hour_mp_data_messengers_detailed = mp_getter.get_full_aggregated()
    context.log.info(f"hour_mp_data_messengers_detailed = {hour_mp_data_messengers_detailed}")

    # Getter for CP for entered chats
    mp_getter = MessagingPlatformDataGetter.prepare_mp_data_getter(
        trunc='hour',
        start_date=report_date,
        end_date=end_date,
        report_date=report_date,
        conn=conn_cp,
        project_ids=project_ids
    )

    # 2) вошедшие чаты
    hour_data_messengers_entered_chats = mp_getter.get_entered_chats()
    context.log.info(f"hour_data_messengers_entered_chats = {hour_data_messengers_entered_chats}")

    # 3) join по 'date'
    hour_mp_data_messengers_detailed = left_join_array(
        hour_mp_data_messengers_detailed,
        hour_data_messengers_entered_chats,
        'date'
    )
    context.log.info(f"Report date = {report_date}")

    # Нормализуем date -> HH:MM:SS
    for row in hour_mp_data_messengers_detailed:
        row['date'] = to_hms(row.get('date'))

    context.log.info(f"return: {hour_mp_data_messengers_detailed}")
    return hour_mp_data_messengers_detailed

# ---------- main ----------
def main():
    # выстави нужные даты/проекты под свой прогон
    report_date = datetime(2025, 11, 19)
    start_date  = datetime(2025, 11, 19)  # для лога; в геттер передается report_date
    end_date    = datetime(2025, 11, 20)  # exclusive
    project_ids = [1, 4, 5, 8, 9, 10, 11, 12, 15, 19, 20, 21]

    # DWH (PROD)
    dwh_host = os.getenv("CC_DWH_DB_HOST_PROD", "")
    dwh_port = int(os.getenv("CC_DWH_DB_PORT_PROD", "5432"))
    dwh_db   = os.getenv("CC_DWH_DB_NAME_PROD", "")
    dwh_user = os.getenv("CC_DWH_DB_USER_PROD", "")
    dwh_pwd  = os.getenv("CC_DWH_DB_PASSWORD_PROD", "")
    dwh_schema = os.getenv("CC_DWH_DB_SCHEMA_PROD", "")

    # CP
    cp_host = os.getenv("DB_CP_HOST", "")
    cp_port = int(os.getenv("DB_CP_PORT", "7504"))
    cp_db   = os.getenv("DB_CP_DATABASE", "")
    cp_user = os.getenv("DB_CP_USERNAME", "")
    cp_pwd  = os.getenv("DB_CP_PASSWORD", "")
    cp_schema = os.getenv("DB_CP_SCHEMA", "")

    if not all([dwh_host, dwh_db, dwh_user, dwh_pwd]) or not all([cp_host, cp_db, cp_user, cp_pwd]):
        raise RuntimeError("Missing required DB env vars. Make sure .env is loaded at:\n  " + ENV_PATH)

    conn_dwh = _pg_connect(dwh_host, dwh_port, dwh_user, dwh_pwd, dwh_db)
    _set_search_path(conn_dwh, dwh_schema)

    conn_cp = _pg_connect(cp_host, cp_port, cp_user, cp_pwd, cp_db)
    _set_search_path(conn_cp, cp_schema)

    try:
        rows = hour_mp_data_messenger_detailed_like(
            report_date=report_date,
            start_date=start_date,
            end_date=end_date,
            project_ids=project_ids,
            conn_dwh=conn_dwh,
            conn_cp=conn_cp,
        )
        print(json.dumps(rows, ensure_ascii=False, indent=2))
        # При желании можно сохранить результат в файл:
        # from pathlib import Path
        # Path("./out").mkdir(exist_ok=True, parents=True)
        # (Path("./out")/"hour_mp_data_messengers_detailed.json").write_text(
        #     json.dumps(rows, ensure_ascii=False, indent=2),
        #     encoding="utf-8"
        # )
    finally:
        try:
            conn_dwh.close()
        except Exception:
            pass
        try:
            conn_cp.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()