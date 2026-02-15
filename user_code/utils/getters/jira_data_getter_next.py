import hashlib
import uuid
from datetime import date, datetime
import json
from decimal import Decimal
from pathlib import Path

import pandas as pd
from dagster import get_dagster_logger

from configs.jira import config
from utils.jira_data import get_created_issues_by_types_with_id_nur, get_created_issues_by_types_with_id_saima, \
    SLA_MOCK_DATA, SLA_MS_DATA, get_jira_subtask_time_series_ms, get_jira_issue_counts_by_date_and_type_ma, \
    get_jira_subtask_time_series_cs, get_open_issues_subtasks_by_region_month_ms, \
    get_open_issues_subtasks_by_region_month_ma, get_open_issues_subtasks_by_region_month_cs, \
    get_unique_unresolved_issues_by_month_region_title_ms, get_unique_unresolved_issues_by_month_region_title_ma, \
    get_unique_unresolved_issues_by_month_region_title_cs
from utils.path import BASE_SQL_PATH

import os
import re
import requests
from typing import Any


class JiraDataGetterNext:
    def __init__(self,
                 dwh_conn,
                 start_date=None,
                 end_date=None,
                 trunc=None,
                 jira_nur = None,
                 jira_saima = None,
                 jira='jirasd_nur'):
        self.jira_nur = jira_nur
        self.jira_saima = jira_saima
        self.dwh_conn = dwh_conn
        self.start_date = start_date
        self.end_date = end_date
        self.trunc = trunc
        self.jira = jira

        if jira == 'jirasd_nur':
            self.connection = jira_nur
        elif jira == 'jirasd_saima':
            self.connection = jira_saima

        self.db = config("jira.connections.{jira}.db", jira=jira)
        self.project_keys = config("jira.connections.{jira}.project_keys", jira=jira)
        self.responsible_groups = config("jira.connections.{jira}.responsible_groups", jira=jira)
        self.closed_issue_title = config("jira.connections.{jira}.closed_issue_title", jira=jira)
        self.sla_custom_field_general = config("jira.connections.{jira}.sla_customfield_number_general", jira=jira)
        self.sla_custom_field_secondary = config("jira.connections.{jira}.sla_customfield_number_secondary", jira=jira)
        self.set_title = False
        self.date_format = None
        self.base_sql_path = Path(BASE_SQL_PATH)
        self.set_days_trunked_data()

    _CC_STAT_URL_DEFAULT = "https://cc-stat.o.kg/api/query/execute"
    _PARAM_RE = re.compile(r"%\((?P<name>[A-Za-z_][A-Za-z0-9_]*)\)s")

    @staticmethod
    def _escape_sql_string(s: str) -> str:
        return s.replace("'", "''")

    @classmethod
    def _to_sql_literal(cls, v: Any) -> str:
        if v is None:
            return "NULL"
        if isinstance(v, bool):
            return "TRUE" if v else "FALSE"
        if isinstance(v, (int, float, Decimal)):
            return str(v)
        if isinstance(v, datetime):
            return f"'{cls._escape_sql_string(v.isoformat(sep=' ', timespec='seconds'))}'"
        if isinstance(v, date):
            return f"'{cls._escape_sql_string(v.isoformat())}'"
        if isinstance(v, (list, tuple, set)):
            items = ", ".join(cls._to_sql_literal(x) for x in v)
            return f"ARRAY[{items}]"
        return f"'{cls._escape_sql_string(str(v))}'"

    @classmethod
    def _interpolate_params(cls, sql: str, params: dict) -> str:
        def repl(m: re.Match) -> str:
            name = m.group("name")
            if name not in params:
                raise KeyError(f"Missing SQL param: {name}")
            return cls._to_sql_literal(params[name])

        return cls._PARAM_RE.sub(repl, sql)

    @classmethod
    def _cc_stat_execute(cls, connection_name: str, sql: str) -> list[dict]:
        log = get_dagster_logger()

        url = os.getenv("CC_STAT_QUERY_URL", cls._CC_STAT_URL_DEFAULT)
        key = os.getenv("CC_STAT_QUERY_KEY", "sjdfjksdjfk2323r99#(#*&*@(@ksjdf293928")
        if not key:
            raise RuntimeError("CC_STAT_QUERY_KEY is not set (env var).")

        has_hash = "#" in key
        has_backslash = "\\" in key
        key_sha8 = hashlib.sha256(key.encode("utf-8")).hexdigest()[:8]

        log.info(
            "[cc-stat] auth check key_len=%s has_hash=%s has_backslash=%s key_sha8=%s",
            len(key), has_hash, has_backslash, key_sha8
        )

        verify_ssl = os.getenv("CC_STAT_VERIFY_SSL", "1") != "0"
        connect_timeout = int(os.getenv("CC_STAT_TIMEOUT_CONNECT", "10"))
        read_timeout = int(os.getenv("CC_STAT_TIMEOUT_READ", "600"))

        req_id = str(uuid.uuid4())

        # --- safe debug info (НЕ палим ключ) ---
        key_preview = f"{key[:4]}***{key[-4:]}" if len(key) >= 8 else "***"
        sql_preview = sql.replace("\n", " ")[:1200]  # чтобы логи не взрывались

        log.info(
            "[cc-stat] request "
            f"id={req_id} url={url} connection={connection_name} "
            f"verify_ssl={verify_ssl} timeout=({connect_timeout},{read_timeout}) "
            f"key={key_preview} sql_len={len(sql)} sql_preview={sql_preview}"
        )

        try:
            resp = requests.post(
                url,
                files={
                    "key": (None, key),
                    "connection": (None, connection_name),
                    "query": (None, sql),
                },
                timeout=(connect_timeout, read_timeout),
                verify=verify_ssl,
                headers={
                    "Accept": "application/json",
                    "X-Request-ID": req_id,
                },
            )
        except Exception as e:
            log.error(f"[cc-stat] network error id={req_id}: {repr(e)}")
            raise

        # --- response debug ---
        content_type = resp.headers.get("Content-Type", "")
        text_preview = (resp.text or "")[:2000]

        log.info(
            "[cc-stat] response "
            f"id={req_id} status={resp.status_code} content_type={content_type} "
            f"resp_len={len(resp.text or '')}"
        )

        # На 403/500 полезно увидеть тело ответа (обычно там причина)
        if resp.status_code != 200:
            log.error(f"[cc-stat] error body id={req_id}: {text_preview}")
            raise RuntimeError(f"cc-stat query failed: HTTP {resp.status_code}: {text_preview}")

        # JSON parse
        try:
            payload = resp.json()
        except Exception as e:
            log.error(f"[cc-stat] non-json id={req_id} parse_error={repr(e)} body={text_preview}")
            raise RuntimeError(f"cc-stat returned non-JSON: {text_preview}")

        # shape debug
        if isinstance(payload, list):
            log.info(f"[cc-stat] parsed list id={req_id} rows={len(payload)}")
            return payload

        if isinstance(payload, dict):
            keys = list(payload.keys())[:50]
            log.info(f"[cc-stat] parsed dict id={req_id} keys={keys}")

            for k in ("rows", "data", "result", "items"):
                if k in payload and isinstance(payload[k], list):
                    log.info(f"[cc-stat] using payload['{k}'] id={req_id} rows={len(payload[k])}")
                    return payload[k]

            if "columns" in payload and "rows" in payload and isinstance(payload["rows"], list):
                cols = payload["columns"]
                log.info(f"[cc-stat] using columns+rows id={req_id} cols={len(cols)} rows={len(payload['rows'])}")
                return [dict(zip(cols, r)) for r in payload["rows"]]

            # если сервер вернул {error: ...}
            if any(k in payload for k in ("error", "message", "detail")):
                log.error(f"[cc-stat] error payload id={req_id}: {json.dumps(payload)[:2000]}")

        raise RuntimeError(f"Unknown cc-stat response shape: {json.dumps(payload)[:2000]}")


    def _apply_extra_filter(self, sql: str, extra_sql: str) -> str:
        return sql.replace("/*__EXTRA_FILTER__*/", f" AND ({extra_sql})" if extra_sql else "")

    def _to_type_ids(self, types):
        out = []
        if not types:
            return out

        def push(v):
            try:
                out.append(int(v))
            except (TypeError, ValueError):
                pass

        for x in types:
            if isinstance(x, (int, float, str)) and not isinstance(x, bool):
                push(x)
                continue
            if isinstance(x, dict):
                for k in ("issue_type_id", "issuetype", "id"):
                    if k in x and x[k] is not None:
                        push(x[k])
                        break
                continue

            for attr in ("issue_type_id", "issuetype", "id"):
                if hasattr(x, attr):
                    push(getattr(x, attr))
                    break
        return out

    def get_sla_by_groups(self, conn, groups, group=True, callback=None):
        """
       - Пустые groups -> []
       - group=True добавляет project_name/title и GROUP BY 1,2,3; иначе GROUP BY 1
       - callback() должен вернуть (extra_sql, params_dict) для вливки в WHERE
       - post-processing SLA делается self.sla_callback(row)
       """
        sql_path = self.base_sql_path / "stat" / "get_sla_by_groups.sql"
        sql_template = sql_path.read_text(encoding="utf-8")

        project_fields = (
            "  , jitcd.project_name\n"
            "  , (jitcd.project_name || ' - ' || jitcd.issue_type_name) AS title\n"
            if group else ""
        )
        group_by = "1,2,3" if group else "1"
        query_template = sql_template.format(project_fields=project_fields, group_by=group_by)

        extra_sql, cb_params = ("", {})
        if callback is not None:
            extra_sql, cb_params = callback()

        sql = self._apply_extra_filter(query_template, extra_sql)
        params = {
            "trunc": self.trunc, "date_format": self.date_format,
            "start_date": self.start_date, "end_date": self.end_date,
            "groups": groups,  **cb_params
        }

        get_dagster_logger().info(f"conn: {conn}")

        df = pd.read_sql_query(sql, conn, params=params)
        rows = df.to_dict("records")
        fn = self.sla_callback
        result = [fn(r) for r in rows] if fn else rows
        return result


    def sla_callback(self, row: dict) -> dict:
        sum_ = int(row.get("sla_reached") or 0)
        total = int(row.get("all_resolved_issues") or 0)
        row = dict(row)
        row["sla"] = sla_counter(sum_, total)
        return row

    def get_created_issues_by_types(self, types, group: bool = True, callback=None):

        sql_path = self.base_sql_path / "jira" / "get_created_issues_by_types.sql"
        sql_template = sql_path.read_text(encoding="utf-8")

        group_fields = (
            "\n  , prj.pkey AS project_key"
            "\n  , ji.issuetype"
            "\n  , prj.pname || ' - ' || itype.pname AS title"
            if group else ""
        )
        group_by = "1,2,3,4" if group else "1"
        query_template = sql_template.format(group_fields=group_fields, group_by=group_by)

        extra_sql, cb_params = ("", {})
        if callback is not None:
            extra_sql, cb_params = callback()

        sql = self._apply_extra_filter(query_template, extra_sql)
        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "types": self._to_type_ids(types),
            "closed_issue_title": self.closed_issue_title,
            **cb_params,
        }

        if self.jira == 'jirasd_saima':
            final_sql = self._interpolate_params(sql, params)
            data = self._cc_stat_execute("jira_saima", final_sql)
        else:
            final_sql = self._interpolate_params(sql, params)
            data = self._cc_stat_execute("jira_nur", final_sql)

        if group and not isinstance(types, (list, tuple, set)) and hasattr(self, "get_replacement_with_local_titles"):
            return self.get_replacement_with_local_titles(data, types)

        get_dagster_logger().info(f"get_created_issues_by_types: {data}")

        return data

    def get_closed_issues_by_types(self, types, group: bool = True, callback=None):
        sql_path = self.base_sql_path / "jira" / "get_closed_issues_by_types.sql"
        sql_template = sql_path.read_text(encoding="utf-8")

        group_fields = (
            "\n  , prj.pkey AS project_key"
            "\n  , ji.issuetype"
            "\n  , prj.pname || ' - ' || itype.pname AS title"
            if group else ""
        )
        group_by = "1,2,3,4" if group else "1"
        query_template = sql_template.format(group_fields=group_fields, group_by=group_by)

        extra_sql, cb_params = ("", {})
        if callback is not None:
            extra_sql, cb_params = callback()

        sql = self._apply_extra_filter(query_template, extra_sql)
        types_param = self._to_type_ids(types)

        if not self.closed_issue_title:
            raise ValueError("closed_issue_title must be set before calling get_closed_issues_by_types")

        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "types": types_param,
            "closed_issue_title": self.closed_issue_title,
            **cb_params,
        }

        df = pd.read_sql_query(sql, self.connection, params=params)
        data = df.to_dict("records")

        if group and not isinstance(types, (list, tuple, set)) and hasattr(self, "get_replacement_with_local_titles"):
            return self.get_replacement_with_local_titles(data, types)

        return data

    def get_types_by_groups(self, groups):
        if not groups:
            return []

        first = groups[0]
        if isinstance(first, str):
            group_col = "group_name"
            group_cast = "text"
            groups_param = list(groups)
        else:
            group_col = "group_id"
            group_cast = "int"
            groups_param = [int(g) for g in groups]

        sql_path = self.base_sql_path / "jira" / "get_types_by_groups.sql"
        sql_template = sql_path.read_text(encoding="utf-8")
        query = sql_template.format(group_col=group_col, group_cast=group_cast)

        df = pd.read_sql_query(query, self.connection, params={"groups": groups_param})
        return df.to_dict("records")

    def get_closed_issues_by_groups(self, groups, group: bool = True, callback=None):
        """
        Логика: пустые groups → [], иначе:
          groups -> types (get_types_by_groups) -> get_closed_issues_by_types
        """
        if not groups:
            return []
        types = self.get_types_by_groups(groups)
        return self.get_closed_issues_by_types(types=types, group=group, callback=callback)

    def get_created_issues_by_groups(self, groups, group: bool = True, callback=None):
        """
        Логика:
          - пустой ввод → []
          - получить типы по группам
          - пробросить в get_created_issues_by_types с теми же group/callback
        """
        if not groups:
            return []

        types = self.get_types_by_groups(groups)
        return self.get_created_issues_by_types(
            types=types,
            group=group,
            callback=callback,
        )

    def get_types_by_responsible_groups(self, responsible_groups):
        """
        Возвращает: List[Dict] с полями id, issue_type_id, project_key, title
        """
        rg = list(responsible_groups or [])
        get_dagster_logger().info(f"responsible_groups = {rg}")
        sql_path = self.base_sql_path / "jira" / "get_types_by_responsible_groups.sql"
        sql = sql_path.read_text(encoding="utf-8")

        df = pd.read_sql_query(sql, self.dwh_conn, params={"responsible_groups": rg})
        return df.to_dict("records")

    def get_created_issues_by_responsible_groups(self, responsible_groups=None, group: bool = True, callback=None):
        """
        Логика:
          - если responsible_groups пустой → берём self.responsible_groups
          - получаем типы по ответственным группам
          - прокидываем их в get_created_issues_by_types с теми же group/callback
        """
        rg = list(responsible_groups) if responsible_groups else (
            list(self.responsible_groups) if self.responsible_groups else [])
        if not rg:
            return []

        types = self.get_types_by_responsible_groups(rg)
        return self.get_created_issues_by_types(types=types, group=group, callback=callback)

    def get_sla_by_responsible_groups(self, responsible_groups=None, group: bool = True, callback=None):
        rg = list(responsible_groups) if responsible_groups else (
            list(self.responsible_groups) if self.responsible_groups else [])
        if not rg:
            return []

        sql_path = self.base_sql_path / "jira" / "get_sla_by_responsible_groups.sql"
        sql_template = sql_path.read_text(encoding="utf-8")

        project_fields = (
            "\n  , jitcd.project_name"
            "\n  , (jitcd.project_name || ' - ' || jitcd.issue_type_name) AS title"
            if group else ""
        )
        group_by = "1,2,3" if group else "1"
        query_template = sql_template.format(project_fields=project_fields, group_by=group_by)

        extra_sql, cb_params = ("", {})
        if callback is not None:
            extra_sql, cb_params = callback()

        sql = self._apply_extra_filter(query_template, extra_sql)

        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "responsible_groups": rg,
            **cb_params,
        }

        df = pd.read_sql_query(sql, self.dwh_conn, params=params)
        rows = df.to_dict("records")
        return [self.sla_callback(r) for r in rows] if hasattr(self, "sla_callback") and self.sla_callback else rows

    def get_opened_issues_by_types(self, project_keys):
        """
        Возвращает List[List[Dict]] — группы по 'title', где каждый элемент:
          {'id', 'month', 'status', 'title', 'subtask_title', 'count'}
        """
        if not project_keys:
            return []

        sql_path = self.base_sql_path / "jira" / "get_opened_issues_by_types.sql"
        sql = sql_path.read_text(encoding="utf-8")

        params = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "project_keys": list(project_keys),
            "excluded_status_ids": [10112, 10208, 12500, 10103, 10215, 10206],
        }

        df = pd.read_sql_query(sql, self.connection, params=params)
        rows = df.to_dict("records")

        from collections import defaultdict

        grouped = defaultdict(list)
        bucket = defaultdict(int)

        for r in rows:
            key = (r.get("status"), r.get("title"), r.get("subtask_title"), r.get("month"))
            bucket[key] += 1

        id_map = {}
        id_counter = 1

        def combo_id(status, title, subtask_title):
            nonlocal id_counter
            base_key = (status, title, subtask_title)
            if base_key not in id_map:
                id_map[base_key] = id_counter
                id_counter += 1
            return id_map[base_key]

        for (status, title, subtask_title, month), cnt in bucket.items():
            rec = {
                "id": combo_id(status, title, subtask_title),
                "month": month,
                "status": status,
                "title": title,
                "subtask_title": subtask_title,
                "count": cnt,
            }
            grouped[title].append(rec)

        for title in grouped:
            grouped[title].sort(key=lambda x: x["month"])

        return list(grouped.values())

    def get_types_by_project_keys(self, project_keys):
        """
        Возврат: List[Dict] с полями id, issue_type_id, project_key, title
        """
        keys = list(project_keys or [])
        if not keys:
            return []

        sql_path = self.base_sql_path / "jira" / "get_types_by_project_keys.sql"
        sql = sql_path.read_text(encoding="utf-8")

        df = pd.read_sql_query(sql, self.connection, params={"project_keys": keys})
        return df.to_dict("records")

    def get_created_issues_by_project_keys(self, project_keys=None, group: bool = True, callback=None):
        """
        Поведение:
          - если project_keys пустой → берём self.project_keys
          - собираем метрики created / not_resolved / resolved
          - group=True: добавляем поля project_key, issuetype, title и делаем пост-обработку локальных тайтлов
        """
        keys = list(project_keys) if project_keys else (list(self.project_keys) if self.project_keys else [])
        if not keys:
            return []

        if not self.closed_issue_title:
            raise ValueError("closed_issue_title must be set before calling get_created_issues_by_project_keys")

        sql_path = self.base_sql_path / "jira" / "get_created_issues_by_project_keys.sql"
        sql_template = sql_path.read_text(encoding="utf-8")

        group_fields = (
            "\n  , prj.pkey AS project_key"
            "\n  , ji.issuetype"
            "\n  , prj.pname || ' - ' || itype.pname AS title"
            if group else ""
        )
        group_by = "1,2,3,4" if group else "1"
        query_template = sql_template.format(group_fields=group_fields, group_by=group_by)

        extra_sql, cb_params = ("", {})
        if callback is not None:
            extra_sql, cb_params = callback()

        sql = self._apply_extra_filter(query_template, extra_sql)
        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "project_keys": keys,
            "closed_issue_title": self.closed_issue_title,
            **cb_params,
        }

        df = pd.read_sql_query(sql, self.connection, params=params)
        data = df.to_dict("records")

        if group:
            types = self.get_types_by_project_keys(keys)
            if hasattr(self, "get_replacement_with_local_titles"):
                return self.get_replacement_with_local_titles(data, types)

        return data

    def get_jira_sla_by_project_keys(self, project_keys=None, callback=None, closed_issue_titles=None):
        keys = list(project_keys) if project_keys else (list(self.project_keys) if self.project_keys else [])
        if not keys:
            return []
        titles = list(closed_issue_titles) if closed_issue_titles else (
            [self.closed_issue_title] if self.closed_issue_title else []
        )
        if not titles:
            raise ValueError("closed_issue_title must be set or provided")

        if self.sla_custom_field_general is None or self.sla_custom_field_secondary is None:
            raise ValueError("SLA custom field ids must be set (sla_custom_field_general/secondary)")

        sql_path = self.base_sql_path / "jira" / "get_jira_sla_by_project_keys.sql"
        sql_template = sql_path.read_text(encoding="utf-8")

        extra_sql, cb_params = ("", {})
        if callback is not None:
            extra_sql, cb_params = callback()

        sql = self._apply_extra_filter(sql_template, extra_sql)

        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "project_keys": keys,
            "closed_issue_titles": titles,
            "sla_custom_field_general": self.sla_custom_field_general,
            "sla_custom_field_secondary": self.sla_custom_field_secondary,
            **cb_params,
        }

        df = pd.read_sql_query(sql, self.connection, params=params)
        rows = df.to_dict("records")
        return [self.sla_callback(r) for r in rows] if hasattr(self, "sla_callback") and self.sla_callback else rows

    def get_issues_custom_field_values_by_options(self, customfield: str, issuetypes):
        """
        Возвращает: List[Dict] с полями: date, id, title, total, not_resolved
        """
        # строгая эквивалентность: требуем заданный ClosedIssueTitle
        if not self.closed_issue_title:
            raise ValueError("closed_issue_title must be set before calling get_issues_custom_field_values_by_options")

        type_ids = self._to_type_ids(issuetypes)
        if not type_ids:
            return []

        sql_path = self.base_sql_path / "jira" / "get_issues_custom_field_values_by_options.sql"
        sql = sql_path.read_text(encoding="utf-8")

        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "issuetypes": type_ids,
            "customfield": customfield,
            "closed_issue_title": self.closed_issue_title,
        }

        df = pd.read_sql_query(sql, self.connection, params=params)
        return df.to_dict("records")

    def get_new_customers_issues_by_status_and_group(self, customfield: str, issue_types, status_id: int):
        """
        Возвращает: [{date, id, title, total}]
        """
        type_ids = self._to_type_ids(issue_types)
        if not type_ids:
            return []

        sql_path = self.base_sql_path / "jira" / "get_new_customers_issues_by_status_and_group.sql"
        sql = sql_path.read_text(encoding="utf-8")

        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "customfield": customfield,
            "status_id": int(status_id),
            "issue_types": type_ids,
        }

        df = pd.read_sql_query(sql, self.connection, params=params)
        return df.to_dict("records")

    def get_closed_issues_custom_field_values_by_options(self, customfield, issue_types):
        """
        Возвращает: [{date, id, title, total}]
        """
        if not self.closed_issue_title:
            raise ValueError(
                "closed_issue_title must be set before calling get_closed_issues_custom_field_values_by_options")

        type_ids = self._to_type_ids(issue_types)
        if not type_ids:
            return []

        sql_path = self.base_sql_path / "jira" / "get_closed_issues_custom_field_values_by_options.sql"
        sql = sql_path.read_text(encoding="utf-8")

        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "issue_types": type_ids,
            "customfield": customfield,
            "closed_issue_title": self.closed_issue_title,
        }

        df = pd.read_sql_query(sql, self.connection, params=params)
        return df.to_dict("records")

    def get_custom_field_options(self, customfield):
        """
        Возвращает: [{id, title}, ...]
        """
        sql_path = self.base_sql_path / "jira" / "get_custom_field_options.sql"
        sql = sql_path.read_text(encoding="utf-8")

        params = {"customfield": customfield}
        df = pd.read_sql_query(sql, self.connection, params=params)
        return df.to_dict("records")

    def get_issues_custom_field_values_by_custom_field_value(
            self,
            first_customfield: int,
            first_customfield_value: int,
            second_customfield: int,
            issue_types,
    ):
        """
        Возвращает: [{date, id, title, total, not_resolved}]
        """
        if not self.closed_issue_title:
            raise ValueError("closed_issue_title must be set before calling this method")

        type_ids = self._to_type_ids(issue_types)
        if not type_ids:
            return []

        sql_path = self.base_sql_path / "jira" / "get_issues_custom_field_values_by_custom_field_value.sql"
        sql = sql_path.read_text(encoding="utf-8")

        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "first_customfield": int(first_customfield),
            "first_customfield_value": int(first_customfield_value),
            "second_customfield": int(second_customfield),
            "issue_types": type_ids,
            "closed_issue_title": self.closed_issue_title,
        }

        df = pd.read_sql_query(sql, self.connection, params=params)
        return df.to_dict("records")

    def get_issues_total_created_minus_review_result(self, issuetypes, results):
        """
        Возврат: [{date, total, held}], где
          held = total - (review_result_total по той же дате)
        """
        type_ids = self._to_type_ids(issuetypes)
        if not type_ids:
            return []

        sql_rr_path = self.base_sql_path / "jira" / "issues_review_result_per_day.sql"
        sql_tot_path = self.base_sql_path / "jira" / "issues_total_created_per_day.sql"
        sql_rr = sql_rr_path.read_text(encoding="utf-8")
        sql_tot = sql_tot_path.read_text(encoding="utf-8")

        params_base = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "issuetypes": type_ids,
        }

        params_rr = {**params_base, "results": list(results or [])}
        df_rr = pd.read_sql_query(sql_rr, self.connection, params=params_rr)
        review_rows = df_rr.to_dict("records")
        review_by_date = {r["date"]: int(r["total"]) for r in review_rows}

        df_tot = pd.read_sql_query(sql_tot, self.connection, params=params_base)
        total_rows = df_tot.to_dict("records")

        out = []
        for r in total_rows:
            date = r["date"]
            total = int(r["total"])
            rr_total = review_by_date.get(date, 0)
            out.append({
                "date": date,
                "total": total,
                "held": total - rr_total,
            })

        return out

    def get_vd_statuses_data(self, issuetypes):
        """
        Возвращает: [{date, id, title, total}]
        """
        type_ids = self._to_type_ids(issuetypes)
        if not type_ids:
            return []

        sql_path = self.base_sql_path / "jira" / "get_vd_statuses_data.sql"
        sql = sql_path.read_text(encoding="utf-8")

        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "issuetypes": type_ids,
        }

        df = pd.read_sql_query(sql, self.connection, params=params)
        return df.to_dict("records")

    def get_vd_data_by_statuses(self):
        """
        Возвращает: [{date, id, title, issue, status, total}]
        """
        sql_path = self.base_sql_path / "jira" / "get_vd_data_by_statuses.sql"
        sql = sql_path.read_text(encoding="utf-8")

        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
        }

        df = pd.read_sql_query(sql, self.connection, params=params)
        return df.to_dict("records")

    def get_vd_failed_processing_result(self):
        """
        Возврат: dict[id] = {"title": <title>, "items": [ {date,id,title,total}, ... ]}
        """
        if not self.closed_issue_title:
            raise ValueError("closed_issue_title must be set before calling get_vd_failed_processing_result")

        sql_path = self.base_sql_path / "jira" / "get_vd_failed_processing_result.sql"
        sql = sql_path.read_text(encoding="utf-8")

        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "closed_issue_title": self.closed_issue_title,
            "excluded_values": ["17962", "17960"],
        }

        df = pd.read_sql_query(sql, self.connection, params=params)
        rows = df.to_dict("records")

        response = {}
        for item in rows:
            _id = item["id"]
            if _id not in response:
                response[_id] = {"title": item["title"], "items": [item]}
            else:
                response[_id]["items"].append(item)
        return response

    def get_vd_successful_processing_result(self):
        """
        Возвращает: [{date, id, title, total}]
        """
        if not self.closed_issue_title:
            raise ValueError("closed_issue_title must be set before calling get_vd_successful_processing_result")

        sql_path = self.base_sql_path / "jira" / "get_vd_successful_processing_result.sql"
        sql = sql_path.read_text(encoding="utf-8")

        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "closed_issue_title": self.closed_issue_title,
            "included_values": ["17960"],  # как whereIn('cv.stringvalue', ['17960'])
        }

        df = pd.read_sql_query(sql, self.connection, params=params)
        return df.to_dict("records")

    def get_issues_low_speed(self):
        """
        Возвращает List[Dict] с фильтром по group_title=='Контакт-центр' и правками полей.
        """
        sql_path = self.base_sql_path / "jira" / "get_issues_low_speed.sql"
        sql = sql_path.read_text(encoding="utf-8")

        params = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "created_format": "DD.MM.YYYY HH24:MI:SS",
            "pkeys1": ["NT", "COVER", "MS"],
            "issue_names": [
                "Naumen Низкая скорость", "Заявка на расширения зоны охвата",
                "Качество связи", "Не работает интернет",
                "Нет входящих вызовов", "Нет исходящих вызовов", "Низкая скорость",
                "Проблема отправки SMS", "Проблема получения SMS",
            ],
        }

        df = pd.read_sql_query(sql, self.connection, params=params)
        rows = df.to_dict("records")

        out = []
        for i in rows:
            if i.get("group_title") != "Контакт-центр":
                continue

            pm = i.get("phone_model")
            if pm is None or pm == "-":
                i["phone_model"] = "Не определено!"

            if i.get("district") == "Ош город":
                i["region"] = "г. Ош"
            if i.get("district") == "Бишкек город":
                i["region"] = "г. Бишкек"

            out.append(i)

        return out

    def get_replacement_with_local_titles(self, data: list[dict], types: list[dict]) -> list[dict]:
        """
        data  — записи с полями 'issuetype' (id) и 'project_key'
        types — записи со справочными полями 'issue_type_id', 'project_key', 'title'
        """
        lookup: dict[tuple[int, str], str] = {}
        for t in types:
            try:
                issue_type_id = int(t["issue_type_id"])
                project_key = t["project_key"]
                title = t.get("title")
                if project_key and title:
                    lookup[(issue_type_id, project_key)] = title
            except (KeyError, TypeError, ValueError):
                continue

        result: list[dict] = []
        for item in data:
            try:
                itype = int(item.get("issuetype"))
                pkey = item.get("project_key")
                new_title = lookup.get((itype, pkey))
                if not new_title:
                    continue
                row = dict(item)
                row["title"] = new_title
                result.append(row)
            except (TypeError, ValueError):
                continue

        return result

    def get_created_issues_by_types_with_id(self, types, group: bool = True, callback=None):
        sql_path = self.base_sql_path / "jira" / "get_created_issues_by_types_with_id.sql"
        tpl = sql_path.read_text(encoding="utf-8")

        # --- SELECT head fields ---
        if self.set_title:
            first_field = "issuetype.pname AS title"
        else:
            first_field = "to_char(date_trunc(%(trunc)s::text, jiraissue.created), %(date_format)s) AS date"

        group_fields = (
            "\n  , jiraissue.project AS id"
            "\n  , project.pkey AS project_key"
            "\n  , jiraissue.issuetype"
            if group else ""
        )

        title_field = "" if self.set_title else "\n  , project.pname || ' - ' || issuetype.pname AS title"

        # --- GROUP BY ---
        group_by = "1, 2, 3, 4, 5" if group else "1"
        if self.set_title:
            group_by = "1, 2, 3, 4"

        query = tpl.format(first_field=first_field, group_fields=group_fields, title_field=title_field,
                           group_by=group_by)

        extra_parts, cb_params = [], {}
        if callback is not None:
            cb_sql, cb_params = callback() or ("", {})
            if cb_sql:
                extra_parts.append(cb_sql)

        type_ids = self._to_type_ids(types)
        if type_ids:
            extra_parts.append("jiraissue.issuetype = ANY(%(types)s::text[])")

        if self.project_keys:
            extra_parts.append("project.pkey = ANY(%(project_keys)s::text[])")

        extra_sql = " AND ".join(extra_parts)
        sql = self._apply_extra_filter(query, extra_sql)

        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "closed_issue_title": self.closed_issue_title,
            **cb_params,
        }
        if type_ids:
            params["types"] = list(type_ids)
        if self.project_keys:
            params["project_keys"] = list(self.project_keys)

        get_dagster_logger().info(f"sql query = {sql}")
        get_dagster_logger().info(f"params = {params}")

        if self.jira == 'jirasd_saima':
            final_sql = self._interpolate_params(sql, params)
            data = self._cc_stat_execute("jira_saima", final_sql)
        else:
            final_sql = self._interpolate_params(sql, params)
            data = self._cc_stat_execute("jira_nur", final_sql)

        if group and not isinstance(types, (list, tuple, set)) and type_ids:
            return self.get_replacement_with_local_titles(data, types)

        get_dagster_logger().info(f"params = {params}")

        return data

    def get_created_issues_data_by_types(self, group: bool = True, unset_date: bool = False):
        sql_path = self.base_sql_path / "jira" / "get_created_issues_data_by_types.sql"
        tpl = sql_path.read_text(encoding="utf-8")

        head = []
        if not unset_date:
            head.append("to_char(date_trunc(%(trunc)s::text, ji.created), %(date_format)s) AS date")
        if group:
            head.extend([
                "project.pname || ' - ' || issuetype.pname AS title",
                "ji.project AS id",
                "project.pkey AS project_key",
                "ji.issuetype"
            ])
        head_fields = ""
        if head:
            head_fields = "  " + (",\n  ".join(head)) + ",\n  "

        if unset_date:
            group_by_clause = ""
            order_by_clause = ""
        else:
            group_by = "1, 2, 3, 4, 5" if group else "1"
            group_by_clause = f"GROUP BY {group_by}"
            order_by_clause = "ORDER BY 1"

        sql = tpl.format(
            head_fields=head_fields,
            group_by_clause=group_by_clause,
            order_by_clause=order_by_clause,
        )

        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "closed_issue_title": self.closed_issue_title,
            "issue_type": '14300',
            "pkey": ["QC"],
        }

        if self.jira == 'jirasd_saima':
            final_sql = self._interpolate_params(sql, params)
            data = self._cc_stat_execute("jira_saima", final_sql)
        else:
            final_sql = self._interpolate_params(sql, params)
            data = self._cc_stat_execute("jira_nur", final_sql)

        get_dagster_logger().info(f"get_created_issues_data_by_types: {data}")

        return data

    def get_issues(self):
        """
          - pkey='QC', issue_type=14300, customfield=16200
          - группировка по дате и значению customfieldoption.customvalue (title)
        """
        sql_path = self.base_sql_path / "jira" / "get_issues.sql"
        sql = sql_path.read_text(encoding="utf-8")

        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,  #
            "start_date": self.start_date,
            "end_date": self.end_date,
            "pkey": "QC",
            "issue_type": '14300',
            "customfield": '16200',
            "closed_issue_title": getattr(self, "closed_issue_title", "Закрытый"),
        }
        if self.jira == 'jirasd_saima':
            final_sql = self._interpolate_params(sql, params)
            data = self._cc_stat_execute("jira_saima", final_sql)
        else:
            final_sql = self._interpolate_params(sql, params)
            data = self._cc_stat_execute("jira_nur", final_sql)

        get_dagster_logger().info(f"get_issues: {data}")

        return data

    def get_issues_status_from_created_data_by_types(self, unset_date: bool = False):
        """
        Эквивалент PHP getIssuesStatusFromCreatedDataByTypes($unsetDate=false).
        """
        sql_path = self.base_sql_path / "jira" / "get_issues_status_from_created_data_by_types.sql"
        tpl = sql_path.read_text(encoding="utf-8")

        # --- заголовок SELECT (дата опциональна) ---
        if unset_date:
            head_fields = ""
            group_by_clause = ""
            order_by_clause = ""
        else:
            head_fields = "to_char(date_trunc(%(trunc)s::text, created_at), %(date_format)s) AS date,\n  "
            group_by_clause = "GROUP BY 1"
            order_by_clause = "ORDER BY 1"

        sql = tpl.format(
            head_fields=head_fields,
            group_by_clause=group_by_clause,
            order_by_clause=order_by_clause,
        )

        excluded = [
            "10443", "10441", "10471", "10468", "10464", "10462", "10461", "10454", "10452", "10451",
            "10368", "10361", "10535", "10533", "10529", "10515", "10483", "10446", "10449"
        ]

        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "pkey": "QC",
            "field": "status",
            "issue_type": '14300',
            "excluded_issuenum": excluded,
        }

        if self.jira == 'jirasd_saima':
            final_sql = self._interpolate_params(sql, params)
            data = self._cc_stat_execute("jira_saima", final_sql)
        else:
            final_sql = self._interpolate_params(sql, params)
            data = self._cc_stat_execute("jira_nur", final_sql)

        get_dagster_logger().info(f"get_issues_status_from_created_data_by_types: {data}")

        return data

    def get_issues_status_from_in_progress_data_by_types(
            self,
            status: list[str],
            unset_date: bool = False,
            count: str = "open_to_close_count",
            filed_name: str = "avg_in_progress_min",
            total: str = "sum_in_progress_close",
    ):
        """
        - status: список строк статусов (ci.newvalue)
        - Имена алиасов колонок настраиваются параметрами.
        - Если unset_date=True — без поля 'date', без GROUP BY/ORDER BY.
        """
        sql_path = self.base_sql_path / "jira" / "get_issues_status_from_in_progress_data_by_types.sql"
        tpl = sql_path.read_text(encoding="utf-8")

        if unset_date:
            head_fields = ""
            group_by_clause = ""
            order_by_clause = ""
        else:
            head_fields = "to_char(t.day, %(date_format)s) AS date,\n  "
            group_by_clause = "GROUP BY 1"
            order_by_clause = "ORDER BY 1"

        sql = tpl.format(
            head_fields=head_fields,
            count_alias=count,
            total_alias=total,
            field_alias=filed_name,
            group_by_clause=group_by_clause,
            order_by_clause=order_by_clause,
        )

        excluded = [
            "10443", "10441", "10471", "10468", "10464", "10462", "10461", "10454", "10452", "10451",
            "10368", "10361", "10535", "10533", "10529", "10515", "10483"
        ]

        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "pkey": "QC",
            "field": "status",
            "issue_type": '14300',
            "excluded_issuenum": excluded,
            "statuses": list(status or []),
        }

        if self.jira == 'jirasd_saima':
            final_sql = self._interpolate_params(sql, params)
            data = self._cc_stat_execute("jira_saima", final_sql)
        else:
            final_sql = self._interpolate_params(sql, params)
            data = self._cc_stat_execute("jira_nur", final_sql)

        get_dagster_logger().info(f"get_issues_status_from_in_progress_data_by_types: {data}")

        return data

    def get_issue_by_resolution(self):
        """
          - QC, issuetype 14300, customfield 16210
          - Группы: дата, status (parent option), title (child option)
          - Счётчики not_resolved / resolved относительно closed_issue_title
        """
        sql_path = self.base_sql_path / "jira" / "get_issue_by_resolution.sql"
        sql = sql_path.read_text(encoding="utf-8")

        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "pkey": "QC",
            "issue_types": ["14300"],
            "customfield": 16210,
            "closed_issue_title": getattr(self, "closed_issue_title", "Закрытый"),
        }
        if self.jira == 'jirasd_saima':
            final_sql = self._interpolate_params(sql, params)
            data = self._cc_stat_execute("jira_saima", final_sql)
        else:
            final_sql = self._interpolate_params(sql, params)
            data = self._cc_stat_execute("jira_nur", final_sql)

        get_dagster_logger().info(f"get_issue_by_resolution: {data}")

        return data

    def get_issue_by_resolution_count(self, unset_date: bool = False):
        sql_path = self.base_sql_path / "jira" / "get_issue_by_resolution_count.sql"
        tpl = sql_path.read_text(encoding="utf-8")

        if unset_date:
            head_fields = ""
            group_by_clause = ""
            order_by_clause = ""
        else:
            head_fields = "to_char(date_trunc(%(trunc)s::text, ji.created), %(date_format)s) AS date,\n  "
            group_by_clause = "GROUP BY 1"
            order_by_clause = "ORDER BY 1"

        sql = tpl.format(
            head_fields=head_fields,
            group_by_clause=group_by_clause,
            order_by_clause=order_by_clause,
        )

        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "pkey": "QC",
            "issue_types": ["14300"],
            "customfields": [16210],
            "ID_HELPED": 36790,
            "ID_NOT_HELPED": 36791,
            "ID_CLIENT_SIDE_REASON": 36833,
            "ID_INFRA_HARDWARE_ISSUE": 38127,
            "ID_TICKET_CANCEL": 38135,
            "resolved_set": [36790, 36791],
        }

        if self.jira == 'jirasd_saima':
            final_sql = self._interpolate_params(sql, params)
            data = self._cc_stat_execute("jira_saima", final_sql)
        else:
            final_sql = self._interpolate_params(sql, params)
            data = self._cc_stat_execute("jira_nur", final_sql)

        get_dagster_logger().info(f"get_issue_by_resolution_count: {data}")

        return data

    def get_jira_sla_group_by_project(self, project_keys=None, callback=None, closed_issue_titles=None):
        keys = list(project_keys) if project_keys else (list(self.project_keys) if self.project_keys else [])
        if not keys:
            return []

        titles = list(closed_issue_titles) if closed_issue_titles else (
            [self.closed_issue_title] if self.closed_issue_title else []
        )
        if not titles:
            raise ValueError("closed_issue_title must be set or provided")

        if self.sla_custom_field_general is None or self.sla_custom_field_secondary is None:
            raise ValueError("SLA custom field ids must be set (sla_custom_field_general/secondary)")

        sql_path = self.base_sql_path / "jira" / "get_jira_sla_group_by_project.sql"
        tpl = sql_path.read_text(encoding="utf-8")

        extra_sql, cb_params = ("", {})
        if callback is not None:
            extra_sql, cb_params = callback() or ("", {})

        sql = self._apply_extra_filter(tpl, extra_sql)

        params = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "project_keys": keys,
            "closed_issue_titles": titles,
            "sla_custom_field_general": self.sla_custom_field_general,
            "sla_custom_field_secondary": self.sla_custom_field_secondary,
            **cb_params,
        }

        df = pd.read_sql_query(sql, self.connection, params=params)
        rows = df.to_dict("records")
        return [self.sla_callback(r) for r in rows] if hasattr(self, "sla_callback") and self.sla_callback else rows

    def get_jira_sla_by_issue_type_and_subtask(self, project_keys=None) -> str:
        keys = list(project_keys) if project_keys else (list(self.project_keys) if self.project_keys else [])
        if not keys:
            return json.dumps([], ensure_ascii=False, indent=2)

        sql_path = self.base_sql_path / "jira" / "get_jira_sla_by_issue_type_and_subtask.sql"
        sql = sql_path.read_text(encoding="utf-8")

        params = {
            "project_keys": keys,
            "start_date": self.start_date,
            "end_date": self.end_date,
        }

        if self.jira == 'jirasd_saima':
            final_sql = self._interpolate_params(sql, params)
            rows = self._cc_stat_execute("jira_saima", final_sql)
        else:
            final_sql = self._interpolate_params(sql, params)
            rows = self._cc_stat_execute("jira_nur", final_sql)

        assert isinstance(rows, list) and all(isinstance(r, dict) for r in rows), "rows must be List[Dict]"



        out = []
        for r in rows:
            out.append({
                "parent_issue_type": r.get("parent_issue_type"),
                "total_count": int(r.get("total_count") or 0),
                "subtask_type": r.get("subtask_type"),
                "subtasks_count": int(r.get("subtasks_count") or 0),
                "open_subtasks_count": int(r.get("open_subtasks_count") or 0),
                "closed_subtasks_count": int(r.get("closed_subtasks_count") or 0),
                "ontime_closed_subtasks_count": int(r.get("ontime_closed_subtasks_count") or 0),
                "sla_plan_days": f"{float(r.get('sla_plan_days') or 0):.2f}",
                "sla_fact_days": f"{float(r.get('sla_fact_days') or 0):.2f}",
            })

        return json.dumps(out, ensure_ascii=False, indent=2)

    def get_jira_subtask_time_series(self, project_keys=None) -> str:
        keys = list(project_keys) if project_keys else (list(self.project_keys) if self.project_keys else [])
        if not keys:
            return json.dumps([], ensure_ascii=False, indent=2)

        sql_path = self.base_sql_path / "jira" / "get_jira_subtask_time_series.sql"
        sql = sql_path.read_text(encoding="utf-8")

        params = {
            "project_keys": keys,
            "start_date": self.start_date,
            "end_date": self.end_date,
        }

        get_dagster_logger().info(f"project_keys = {project_keys}")

        if self.jira == 'jirasd_saima':
            final_sql = self._interpolate_params(sql, params)
            rows = self._cc_stat_execute("jira_saima", final_sql)
        else:
            final_sql = self._interpolate_params(sql, params)
            rows = self._cc_stat_execute("jira_nur", final_sql)

        return json.dumps(rows, ensure_ascii=False, indent=2)

    def get_jira_issue_counts_by_date_and_type(self, project_keys=None) -> str:
        keys = list(project_keys) if project_keys else (list(self.project_keys) if self.project_keys else [])
        if not keys:
            return json.dumps([], ensure_ascii=False, indent=2)

        sql_path = self.base_sql_path / "jira" / "get_jira_issue_counts_by_date_and_type.sql"
        sql = sql_path.read_text(encoding="utf-8")

        params = {"project_keys": keys, "start_date": self.start_date, "end_date": self.end_date}

        if self.jira == 'jirasd_saima':
            final_sql = self._interpolate_params(sql, params)
            rows = self._cc_stat_execute("jira_saima", final_sql)
        else:
            final_sql = self._interpolate_params(sql, params)
            rows = self._cc_stat_execute("jira_nur", final_sql)

        return json.dumps(rows, ensure_ascii=False, indent=2)

    def get_unique_unresolved_issues_by_month_region_title(self, project_keys=None) -> str:
        keys = list(project_keys) if project_keys else (list(self.project_keys) if self.project_keys else [])
        if not keys:
            return json.dumps([], ensure_ascii=False, indent=2)

        sql_path = self.base_sql_path / "jira" / "get_unique_unresolved_issues_by_month_region_title.sql"
        sql = sql_path.read_text(encoding="utf-8")

        params = {
            "project_keys": keys,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "excluded_status_ids": [10112, 10208, 12500, 10103, 10215, 10206],
        }

        get_dagster_logger().info(f"unique_unresolved_issues query = {sql}")
        get_dagster_logger().info(f"params = {params}")

        if self.jira == 'jirasd_saima':
            final_sql = self._interpolate_params(sql, params)
            rows = self._cc_stat_execute("jira_saima", final_sql)
        else:
            final_sql = self._interpolate_params(sql, params)
            rows = self._cc_stat_execute("jira_nur", final_sql)

        return json.dumps(rows, ensure_ascii=False, indent=2)

    def get_linked_jira_issues_by_date(self, unset_date: bool = False):
        sql_path = self.base_sql_path / "jira" / "get_linked_jira_issues_by_date.sql"
        tpl = sql_path.read_text(encoding="utf-8")

        if unset_date:
            head_fields = ""
            group_by_clause = ""
            order_by_clause = ""
        else:
            head_fields = "date_trunc('day', ji.created)::timestamp(6) AS date,\n  "
            group_by_clause = "GROUP BY 1"
            order_by_clause = "ORDER BY 1"

        sql = tpl.format(
            head_fields=head_fields,
            group_by_clause=group_by_clause,
            order_by_clause=order_by_clause,
        )

        projects = ["10101", "10100", "10406"]
        issue_types = [
            "Затруднение в работе IPTV",
            "Затруднение в работе O!TV",
            "Массовая проблема - O!TV",
            "Массовая проблема - Не работает Интернет",
            "Массовая проблема - Низкая скорость Интернета",
            "Не работает проводной интернет",
            "Низкая скорость на проводном интернете",
            "Установка дополнительной O!TV приставки (выкуп)",
        ]

        params = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "projects": [int(x) for x in projects],
            "issue_types": issue_types,
            "grp_customfield": 10212,
            "group_customvalue": "Контакт-Центр",
        }

        if self.jira == 'jirasd_saima':
            final_sql = self._interpolate_params(sql, params)
            data = self._cc_stat_execute("jira_saima", final_sql)
        else:
            final_sql = self._interpolate_params(sql, params)
            data = self._cc_stat_execute("jira_nur", final_sql)

        get_dagster_logger().info(f"get_linked_jira_issues_by_date: {data}")

        return data

    def get_open_issues_subtasks_by_region_month(self, project_keys=None) -> str:
        keys = list(project_keys) if project_keys else (list(self.project_keys) if self.project_keys else [])
        if not keys:
            return json.dumps([], ensure_ascii=False, indent=2)

        sql_path = self.base_sql_path / "jira" / "get_open_issues_subtasks_by_region_month.sql"
        sql = sql_path.read_text(encoding="utf-8")

        params = {
            "project_keys": keys,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "excluded_status_ids": [10112, 10208, 12500, 10103, 10215, 10206],
        }
        get_dagster_logger().info(f"query = {sql}")
        get_dagster_logger().info(f"params = {params}")

        if self.jira == 'jirasd_saima':
            final_sql = self._interpolate_params(sql, params)
            rows = self._cc_stat_execute("jira_saima", final_sql)
        else:
            final_sql = self._interpolate_params(sql, params)
            rows = self._cc_stat_execute("jira_nur", final_sql)

        return json.dumps(rows, ensure_ascii=False, indent=2)



    def set_hours_trunked_data(self) -> None:
        self.trunc = "hour"
        self.date_format = "YYYY-MM-DD HH24:MI:SS"

    def set_days_trunked_data(self) -> None:
        self.trunc = "day"
        self.date_format = "YYYY-MM-DD HH24:MI:SS"

    def set_months_trunked_data(self) -> None:
        self.trunc = "month"
        self.date_format = "YYYY-MM-DD"

    def get_created_issues_by_responsible_groups_with_id(
            self,
            responsible_groups=None,
            group: bool = True,
            callback=None, ):
        rg = list(responsible_groups) if responsible_groups else (
            list(self.responsible_groups) if self.responsible_groups else []
        )
        get_dagster_logger().info(f"rg in group with id= {rg}")
        types = self.get_types_by_responsible_groups(rg)
        get_dagster_logger().info(f"get_types_by_responsible_groups = {types}")
        return self.get_created_issues_by_types_with_id(types=types, group=group, callback=callback)


def sla_counter(sum_: int, total: int):
    if sum_ == 0 and total == 0:
        return None
    elif sum_ == 0:
        return 0
    elif total == 0:
        return 100
    return (sum_ / total) * 100


def sla_callback(row: dict) -> dict:
    sum_ = int(row.get("sla_reached") or 0)
    total = int(row.get("all_resolved_issues") or 0)
    row = dict(row)
    row["sla"] = sla_counter(sum_, total)
    return row