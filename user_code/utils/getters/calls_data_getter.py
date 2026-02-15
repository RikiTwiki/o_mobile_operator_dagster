# utils/getters/calls_data_getter.py
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import re

import pandas as pd

from utils.path import BASE_SQL_PATH

import os

import numpy as np

import math

from utils.getters.occupancydatagetter import OccupancyDataGetter

from dagster import get_dagster_logger

import psycopg2.extras

class CallsDataGetter:

    Ou: str = "corebo00000000000n5rrusvrnd6vc0s"
    ProjectID: str = "corebo00000000000n9e9ma1906197mg"
    NotInTitles: List[str] = ["Абдыжапаров Султан"]

    trunc: str
    date_format: str

    def __init__(
        self,
        conn,

        start_date: str,
        end_date: str,

        conn_bpm = None,

        project_id: Optional[str] = None,
    ) -> None:
        self.conn = conn

        self.conn_bpm = conn_bpm

        self.start_date = start_date
        self.end_date = end_date
        if project_id:
            self.project_id = project_id

        self.trunc = "day"
        self.date_format = "YYYY-MM-DD"

        self.base_sql_path = BASE_SQL_PATH

    # --- helpers ---

    _IVR_PROJECT_IDS_DEFAULT: Sequence[str] = (
        "corebo00000000000ni3d5tgr2iffv24",
        "corebo00000000000nha1om5shqidq98",
        "corebo00000000000nnrdrlklmt48gjk",
        "corebo00000000000nha1ii9e4fddmu0",
        "corebo00000000000nhd5elip10bnp68",
        "corebo00000000000nhrnevpmhdmpib0",
        "corebo00000000000o6dk0apqn5m6hcc",
        "corebo00000000000nhrmqtr14nggsfk",
        "corebo00000000000nhrna8c54omdru4",
        "corebo00000000000ni3d75k61bti2es",
        "corebo00000000000oedipngl7u45t8o",
        "corebo00000000000nha1flctieef5hc",
        "corebo00000000000nhrncfjsmfkv0do",
        "corebo00000000000nhrnfi0c5bm20us",
        "corebo00000000000nk61tt6l743cl34",
        "corebo00000000000nha1i167lv182gs",
        "corebo00000000000nha1j4r316jk6u0",
        "corebo00000000000npqh4o5fiq36qb4",
        "corebo00000000000npqh8r5q4n36570",
        "corebo00000000000npqh6nt8js1fg2s",
        "corebo00000000000npqh1gid4hpm25o",
    )

    def _strip_strings(sql: str) -> str:

        _STR_LIT = re.compile(r"('(?:''|[^'])*')|(\"(?:\"\"|[^\"])*\")")

        return _STR_LIT.sub(lambda m: " " * len(m.group(0)), sql)

    def _read_sql(self, filename: str, conn: str = "None") -> str:
        log = get_dagster_logger()
        if conn == "bpm":
            path = os.path.join(self.base_sql_path, "bpm", filename)
        else:
            path = os.path.join(self.base_sql_path, "naumen", filename)
        log.info(f"[NAUMEN SQL PATH] {path}")  # <— добавь
        with open(path, 'r', encoding="utf-8") as f:
            return f.read()

    def _fetch_all(self, sql: str, params: Any = None) -> List[Dict[str, Any]]:
        log = get_dagster_logger()

        # Учитываем обычные и E'...' строки, а также двойные кавычки
        str_pat = re.compile(r"([eE]?'(?:''|[^'])*')|(\"(?:\"\"|[^\"])*\")")
        named_pat = re.compile(r"%\(\s*(\w+)\s*\)s")  # %(name)s
        pos_pat = re.compile(r"(?<!%)%s(?!\w)")  # %s (не %%s)

        strip_strings = lambda s: str_pat.sub(lambda m: " " * len(m.group(0)), s)
        sql_no_str = strip_strings(sql)

        names = named_pat.findall(sql_no_str)
        pos_count = len(pos_pat.findall(sql_no_str))
        has_named = bool(names)
        has_pos = pos_count > 0

        if has_named and has_pos:
            raise ValueError("SQL mixes named %(...)s and positional %s placeholders. Use one style.")

        # Готовим первую попытку
        if has_named:
            if not isinstance(params, dict):
                raise TypeError("Named placeholders require dict params")
            missing = [n for n in names if n not in params]
            if missing:
                raise KeyError(f"Missing params for named placeholders: {missing}")

            # 1) Дет. замена КАЖДОГО имени на %s ровно по одному разу, сохраняя порядок 'names'
            sql_exec = sql
            for key in names:
                # заменяем только одно вхождение конкретного ключа, вне строк (закономерно: ключи идут из sql_no_str)
                sql_exec, nrepl = re.subn(r"%\(\s*" + re.escape(key) + r"\s*\)s", "%s", sql_exec, count=1)
                if nrepl != 1:
                    raise RuntimeError(f"Placeholder %( {key} )s not replaced exactly once (replaced={nrepl}).")

            # 2) Строим tuple параметров в том же порядке
            params_exec = tuple(params[k] for k in names)

            # 3) Контроль: число %s после замены == числу параметров
            pos_count_after = len(pos_pat.findall(strip_strings(sql_exec)))
            if pos_count_after != len(params_exec):
                raise IndexError(f"tuple index out of range: placeholders={pos_count_after}, params={len(params_exec)}")

            expected = actual = len(params_exec)
            style = "forced-pos"
        elif has_pos:
            sql_exec = sql
            if isinstance(params, dict):
                # на всякий выпад — последовательность в порядке обнаружения
                params_exec = tuple(params[k] for k in ("start_date", "end_date") if k in params) \
                              or tuple(params.values())
            else:
                params_exec = params
            expected = pos_count
            actual = 0 if params_exec is None else (len(params_exec) if isinstance(params_exec, (list, tuple)) else 1)
            style = "positional"
        else:
            sql_exec = sql
            params_exec = None
            expected = actual = 0
            style = "none"

        if expected != actual:
            log.info(f"[NAUMEN SQL] placeholder/param mismatch: expected={expected}, got={actual}")
            raise IndexError(f"tuple index out of range: placeholders={expected}, params={actual}")

        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            log.info(f"[NAUMEN SQL] style={style}; placeholders={expected}; param_type={type(params_exec).__name__}")
            # Не используем mogrify для named-ветки — уже всё конвертировали в позиционный вид
            cur.execute(sql_exec, params_exec)
            rows = cur.fetchall()
            return [dict(r) for r in rows]

    def _round_half_up(self, x: float) -> int:
        return int(math.floor(x + 0.5))

    # --- основной метод ---

    def get_detailed_naumen_out_calls_data(self) -> List[Dict[str, Any]]:
        sql = self._read_sql("get_detailed_naumen_out_calls_data.sql")
        params = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "project_id": self.ProjectID,
        }
        rows = self._fetch_all(sql, params) or []
        if not rows:
            return []

        df = pd.DataFrame(rows, columns=["date", "row", "value"])
        df["value"] = pd.to_numeric(df["value"], errors="coerce").fillna(0).astype(int)
        df = df.groupby(["date", "row"], as_index=False)["value"].sum()

        dates = sorted(df["date"].unique().tolist())
        row_labels = sorted(df["row"].unique().tolist())

        idx = pd.MultiIndex.from_product([dates, row_labels], names=["date", "row"])
        df_full = df.set_index(["date", "row"]).reindex(idx, fill_value=0).reset_index()

        df_full["value"] = df_full["value"].where(df_full["value"] != 0, None)

        df_full["value"] = df_full["value"].astype('Int64')

        return df_full.to_dict(orient="records")

    def get_repeat_calls_by_ivr(
            self,
            start_date: str | None = None,
            end_date: str | None = None,
            project_ids: Sequence[str] | None = None,
    ) -> List[Dict[str, Any]]:
        sql = self._read_sql("get_repeat_calls_by_ivr.sql")

        start = start_date or self.start_date
        end = end_date or self.end_date
        proj = list(project_ids or getattr(self, "_IVR_PROJECT_IDS_DEFAULT", []))
        if not proj:
            raise ValueError("project_ids is empty: передай список или настрой _IVR_PROJECT_IDS_DEFAULT")

        params = {
            "start": start,
            "end": end,
            "project_ids": proj,
        }

        rows = self._fetch_all(sql, params) or []

        out: List[Dict[str, Any]] = []
        for r in rows:
            day = r.get("day")
            if hasattr(day, "isoformat"):
                day = day.isoformat()
            rc = r.get("repeat_calls")
            try:
                rc = float(rc) if rc is not None else None
            except (TypeError, ValueError):
                rc = None
            out.append({"day": day, "repeat_calls": rc})

        out.sort(key=lambda x: (x["day"] is None, x["day"] or ""))

        return out

    def get_naumen_out_calls_data(self) -> List[Dict[str, Any]]:
        """
        Аналог PHP getNaumenOutCallsData(), но агрегация выполняется в pandas.
        Возвращает [{"date":"YYYY-MM-DD","row":"Соединено|Недозвон","value":int}, ...]
        """
        # Берём "сырые" поля и группируем в pandas
        sql = self._read_sql("get_naumen_out_calls_data.sql")
        params = {
            "start_date": self.start_date,  # < end_date (правая граница исключительная)
            "end_date": self.end_date,
            "project_id": self.ProjectID,
        }

        rows = self._fetch_all(sql, params) or []
        if not rows:
            return []

        df = pd.DataFrame(rows)

        # Ожидаемые колонки: attempt_start, attempt_result, number_type
        # 1) Дата
        df["attempt_start"] = pd.to_datetime(df["attempt_start"], errors="coerce")
        df = df.dropna(subset=["attempt_start"])
        df["date"] = df["attempt_start"].dt.strftime("%Y-%m-%d")

        # 2) Метка ряда: connected → "Соединено", иначе → "Недозвон"
        df["row"] = (df["attempt_result"] == "connected").map({True: "Соединено", False: "Недозвон"})

        # 3) COUNT(number_type): считаем только не-NaN
        df["cnt"] = df["number_type"].notna().astype(int)

        out_df = (
            df.groupby(["date", "row"], as_index=False)["cnt"]
            .sum()
            .rename(columns={"cnt": "value"})
            .sort_values(["date", "row"])
        )

        # Выгружаем в список словарей
        return out_df[["date", "row", "value"]].to_dict(orient="records")

    def get_naumen_out_calls_data_new(self) -> List[Dict[str, Any]]:
        sql = self._read_sql("get_naumen_out_calls_data_new.sql")
        params = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "project_id": self.ProjectID,
        }

        rows = self._fetch_all(sql, params) or []
        if not rows:
            return []

        df = pd.DataFrame(rows)

        df["attempt_start"] = pd.to_datetime(df["attempt_start"], errors="coerce")
        df = df.dropna(subset=["attempt_start"])
        df["date"] = df["attempt_start"].dt.strftime("%Y-%m-%d")

        cond_connected = df["attempt_result"] == "connected"
        row = pd.Series("Недозвон", index=df.index)
        row = row.mask(cond_connected & (df["speaking_time"] >= 5000), "Дозвон более 5 сек.")
        row = row.mask(cond_connected & (df["speaking_time"] < 5000), "Дозвон менее 5 сек.")
        df["row"] = row

        df["cnt"] = df["number_type"].notna().astype(int)

        out_df = (
            df.groupby(["date", "row"], as_index=False)["cnt"]
            .sum()
            .rename(columns={"cnt": "value"})
            .sort_values(["date", "row"])
        )

        return out_df[["date", "row", "value"]].to_dict(orient="records")

    def get_naumen_out_calls_data_with_trunc(self, date_field: str = "date") -> List[Dict[str, Any]]:
        """
        Аналог PHP getNaumenOutCallsDataWithTrunc($dateField='date').
        Возвращает [{"<date_field>": "...", "row": "Соединено|Недозвон", "value": int}, ...]
        """
        sql = self._read_sql("get_naumen_out_calls_data_with_trunc.sql")
        params = {
            "start_date": self.start_date,  # правая граница исключительная (< end)
            "end_date": self.end_date,
            "project_id": self.ProjectID,
        }
        rows = self._fetch_all(sql, params) or []
        if not rows:
            return []

        df = pd.DataFrame(rows)
        # ожидаемые колонки в rows: attempt_start, attempt_result, number_type
        df["attempt_start"] = pd.to_datetime(df["attempt_start"], errors="coerce")
        df = df.dropna(subset=["attempt_start"])

        # Если время timezone-aware, снимем таймзону для векторных операций
        if getattr(df["attempt_start"].dt, "tz", None) is not None:
            df["attempt_start"] = df["attempt_start"].dt.tz_localize(None)

        # 1) Транк по self.trunc
        trunc = (getattr(self, "trunc", "day") or "day").lower()
        if trunc == "month":
            # ВАЖНО: месячный бакет — через период
            df["_bucket_dt"] = df["attempt_start"].dt.to_period("M").dt.to_timestamp()  # начало месяца 00:00:00
        elif trunc == "hour":
            df["_bucket_dt"] = df["attempt_start"].dt.floor("H")
        else:  # "day" (по умолчанию)
            df["_bucket_dt"] = df["attempt_start"].dt.floor("D")

        # 2) Формат как to_char(..., self.date_format)
        def _pgfmt_to_py(fmt: str | None) -> str:
            if not fmt:
                return "%Y-%m-%d"
            py = fmt
            # порядок замен — от длинных к коротким
            py = py.replace("HH24", "%H")
            py = py.replace("MI", "%M")
            py = py.replace("SS", "%S")
            py = py.replace("YYYY", "%Y")
            py = py.replace("MM", "%m")
            py = py.replace("DD", "%d")
            return py

        py_fmt = _pgfmt_to_py(getattr(self, "date_format", None))
        df[date_field] = df["_bucket_dt"].dt.strftime(py_fmt)

        # 3) Категория: connected → "Соединено", иначе → "Недозвон"
        df["row"] = (df["attempt_result"] == "connected").map({True: "Соединено", False: "Недозвон"})

        # 4) COUNT(number_type): считаем только ненулевые значения
        df["cnt"] = df["number_type"].notna().astype(int)

        out_df = (
            df.groupby([date_field, "row"], as_index=False)["cnt"]
            .sum()
            .rename(columns={"cnt": "value"})
            .sort_values([date_field])  # как orderBy(1) в PHP
        )

        return out_df[[date_field, "row", "value"]].to_dict(orient="records")

    def get_naumen_missed_out_calls_data(self, date_field: str = "date") -> List[Dict[str, Any]]:
        """
        Аналог PHP getNaumenMissedOutCallsData($dateField='date').
        Возвращает [{"<date_field>": "...", "row": "<причина>", "value": int}, ...]
        """
        sql = self._read_sql("get_naumen_missed_out_calls_data.sql")
        params = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "project_id": self.ProjectID,
        }
        rows = self._fetch_all(sql, params) or []
        if not rows:
            return []

        df = pd.DataFrame(rows)
        # Ожидаемые колонки: attempt_start, attempt_result, number_type
        df["attempt_start"] = pd.to_datetime(df["attempt_start"], errors="coerce")
        df = df.dropna(subset=["attempt_start"])

        # Снять таймзону (если есть), чтобы корректно бакетить
        try:
            if getattr(df["attempt_start"].dt, "tz", None) is not None:
                df["attempt_start"] = df["attempt_start"].dt.tz_localize(None)
        except Exception:
            pass

        # 1) Бакет по self.trunc: hour/day/month
        trunc = (getattr(self, "trunc", "day") or "day").lower()
        if trunc == "month":
            df["_bucket_dt"] = df["attempt_start"].dt.to_period("M").dt.to_timestamp()  # начало месяца
        elif trunc == "hour":
            df["_bucket_dt"] = df["attempt_start"].dt.floor("H")
        else:  # "day" по умолчанию
            df["_bucket_dt"] = df["attempt_start"].dt.floor("D")

        # 2) Формат как to_char(..., self.date_format)
        def _pgfmt_to_py(fmt: str | None) -> str:
            if not fmt:
                return "%Y-%m-%d"
            py = fmt
            py = py.replace("HH24", "%H").replace("MI", "%M").replace("SS", "%S")
            py = py.replace("YYYY", "%Y").replace("MM", "%m").replace("DD", "%d")
            return py

        py_fmt = _pgfmt_to_py(getattr(self, "date_format", None))
        df[date_field] = df["_bucket_dt"].dt.strftime(py_fmt)

        # 3) Маппинг причин (как в PHP CASE)
        reason_map = {
            "busy": "Абонент занят",
            "no_answer": "Абонент не принял вызов",
            "rejected": "Абонент отклонил вызов",
            "operator_busy": "Оператор занят",
            "operator_no_answer": "Оператор не принял вызов",
            "operator_rejected": "Оператор отклонил вызов",
            "not_found": "Вызываемый номер не существует",
            "UNKNOWN_ERROR": "Неизвестный код отбоя",
            "abandoned": "Потерянный вызов",
            "amd": "Автоответчик",
            "CRR_DISCONNECT": "Обрыв связи",
            "CRR_INVALID": "Неправильный номер",
            "message_not_played": "Абонент завершил вызов во время IVR",
            "CRR_UNAVAILABLE": "Не берут трубку",
        }
        df["row"] = df["attempt_result"].map(reason_map).fillna("Неизвестно")

        # 4) COUNT(number_type): считаем только ненулевые значения
        df["cnt"] = df["number_type"].notna().astype(int)

        out_df = (
            df.groupby([date_field, "row"], as_index=False)["cnt"]
            .sum()
            .rename(columns={"cnt": "value"})
            .sort_values([date_field])  # как orderBy(1) в PHP
        )

        return out_df[[date_field, "row", "value"]].to_dict(orient="records")

    def get_percent_naumen_out_calls_data(self) -> List[Dict[str, Any]]:
        """
        Аналог PHP getPercentNaumenOutCallsData().
        Возвращает [{"date":"YYYY-MM-DD","row":"Соединено|Недозвон","value": <percent int>}, ...]
        """
        # Используем тот же "сырой" SQL, что и get_naumen_out_calls_data:
        # он должен возвращать: attempt_start, attempt_result, number_type
        sql = self._read_sql("get_percent_naumen_out_calls_data.sql")
        params = {
            "start_date": self.start_date,  # < end_date (правая граница исключительная)
            "end_date": self.end_date,
            "project_id": self.ProjectID,
        }

        rows = self._fetch_all(sql, params) or []
        if not rows:
            return []

        df = pd.DataFrame(rows)

        # 1) Нормализация дат
        df["attempt_start"] = pd.to_datetime(df["attempt_start"], errors="coerce")
        df = df.dropna(subset=["attempt_start"])
        # если timezone-aware — убираем TZ для векторных операций
        try:
            if getattr(df["attempt_start"].dt, "tz", None) is not None:
                df["attempt_start"] = df["attempt_start"].dt.tz_localize(None)
        except Exception:
            pass
        df["date"] = df["attempt_start"].dt.strftime("%Y-%m-%d")

        # 2) Категория: connected → "Соединено", иначе → "Недозвон"
        df["row"] = (df["attempt_result"] == "connected").map({True: "Соединено", False: "Недозвон"})

        # 3) COUNT(number_type): считаем только ненулевые значения
        df["cnt"] = df["number_type"].notna().astype(int)

        # 4) Агрегат по дням и категориям
        agg = (
            df.groupby(["date", "row"], as_index=False)["cnt"]
            .sum()
            .rename(columns={"cnt": "value"})  # value = абсолютное количество
        )

        # 5) Дневные суммы
        totals = (
            agg.groupby("date", as_index=False)["value"]
            .sum()
            .rename(columns={"value": "total"})
        )

        # 6) Проценты = round_half_up(value/total*100)
        out_df = agg.merge(totals, on="date", how="left")
        with np.errstate(divide="ignore", invalid="ignore"):
            pct = (out_df["value"] / out_df["total"]) * 100.0
            # PHP round() по умолчанию = HALF_UP; эмулируем: floor(x + 0.5)
            pct = np.where(out_df["total"] > 0, np.floor(pct + 0.5), np.nan)
        out_df["value"] = pd.Series(pct).astype("Int64")  # значение = ПРОЦЕНТ (целое)

        # Порядок как в PHP: order by date (без сортировки по row)
        out_df = out_df.sort_values(["date"])[["date", "row", "value"]]

        return out_df.to_dict(orient="records")

    def get_aggregated_out_calls_last_30_days(self) -> List[Dict[str, Any]]:
        """
        Аналог PHP getAggregatedOutCallsLast30Days().
        Возвращает [{"date":"YYYY-MM-DD","value": int}, ...]
        """
        sql = self._read_sql("get_aggregated_out_calls_last_30_days.sql")
        params = {
            "start_date": self.start_date,  # правая граница исключительная (< end)
            "end_date": self.end_date,
            "project_id": self.ProjectID,
        }

        rows = self._fetch_all(sql, params) or []
        if not rows:
            return []

        df = pd.DataFrame(rows)

        # 1) Дата
        df["attempt_start"] = pd.to_datetime(df["attempt_start"], errors="coerce")
        df = df.dropna(subset=["attempt_start"])
        # если timezone-aware — снимем TZ
        try:
            if getattr(df["attempt_start"].dt, "tz", None) is not None:
                df["attempt_start"] = df["attempt_start"].dt.tz_localize(None)
        except Exception:
            pass
        df["date"] = df["attempt_start"].dt.strftime("%Y-%m-%d")

        # 2) COUNT(number_type): считаем только ненулевые значения
        df["cnt"] = df["number_type"].notna().astype(int)

        out_df = (
            df.groupby("date", as_index=False)["cnt"]
            .sum()
            .rename(columns={"cnt": "value"})
            .sort_values("date")
        )

        return out_df[["date", "value"]].to_dict(orient="records")

    def get_aggregated_call_conversion(self) -> List[Dict[str, Any]]:
        """
        Аналог PHP getAggregatedCallConversion().
        Возвращает [{"date":"YYYY-MM-DD","value": <percent int or None>}, ...]
        """
        sql = self._read_sql("get_aggregated_call_conversion.sql")
        params = {
            "start_date": self.start_date,  # правая граница исключительная (< end)
            "end_date": self.end_date,
            "project_id": self.ProjectID,
        }

        rows = self._fetch_all(sql, params) or []
        if not rows:
            return []

        df = pd.DataFrame(rows)
        # Ожидаемые колонки: attempt_start, attempt_result, number_type
        df["attempt_start"] = pd.to_datetime(df["attempt_start"], errors="coerce")
        df = df.dropna(subset=["attempt_start"])

        # Если timezone-aware — снимем TZ
        try:
            if getattr(df["attempt_start"].dt, "tz", None) is not None:
                df["attempt_start"] = df["attempt_start"].dt.tz_localize(None)
        except Exception:
            pass

        df["date"] = df["attempt_start"].dt.strftime("%Y-%m-%d")

        # Число connected (как SUM(CASE WHEN attempt_result='connected' THEN 1 ELSE 0 END))
        df["connected"] = (df["attempt_result"] == "connected").astype(int)

        # COUNT(number_type): считаем только ненулевые значения
        df["cnt"] = df["number_type"].notna().astype(int)

        agg = (
            df.groupby("date", as_index=False)[["connected", "cnt"]]
            .sum()
            .rename(columns={"cnt": "total"})
        )

        # % = round_half_up(connected / total * 100)
        with np.errstate(divide="ignore", invalid="ignore"):
            pct = np.where(
                agg["total"] > 0,
                (agg["connected"] / agg["total"]) * 100.0,
                np.nan,
            )
            pct = np.floor(pct + 0.5)  # HALF_UP до целого

        out_df = pd.DataFrame({
            "date": agg["date"],
            "value": pd.Series(pct).astype("Int64")  # целое или <NA>
        }).sort_values("date")

        # Int64(<NA>) → None в dict
        return out_df.to_dict(orient="records")

    def get_call_aht_new(self) -> List[Dict[str, Any]]:
        """
        Аналог PHP getCallAHTNew().
        AHT = round(AVG(operator_pickup_time + speaking_time + wrapup_time)) по дням,
        только для connected и speaking_time > 10. Единица измерения: "сек."
        Возвращает [{"date":"YYYY-MM-DD","value":int,"unit":"сек."}, ...]
        """
        sql = self._read_sql("get_call_aht_new.sql")
        params = {
            "start_date": self.start_date,  # < end_date (правая граница исключительная)
            "end_date": self.end_date,
            "project_id": self.ProjectID,
        }

        rows = self._fetch_all(sql, params) or []
        if not rows:
            return []

        df = pd.DataFrame(rows)  # cols: attempt_start, operator_pickup_time, speaking_time, wrapup_time

        # Дата
        df["attempt_start"] = pd.to_datetime(df["attempt_start"], errors="coerce")
        df = df.dropna(subset=["attempt_start"])
        try:
            if getattr(df["attempt_start"].dt, "tz", None) is not None:
                df["attempt_start"] = df["attempt_start"].dt.tz_localize(None)
        except Exception:
            pass
        df["date"] = df["attempt_start"].dt.strftime("%Y-%m-%d")

        # Сумма компонент AHT; если какая-то компонента NaN → вся строка NaN (как в SQL)
        for col in ["operator_pickup_time", "speaking_time", "wrapup_time"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df["aht_sum"] = df["operator_pickup_time"] + df["speaking_time"] + df["wrapup_time"]

        # Среднее по дням (mean игнорирует NaN — эквивалент AVG по выражению с NULL)
        mean_by_day = df.groupby("date", as_index=False)["aht_sum"].mean()

        # Округление HALF_UP до целого (как round() в Postgres)
        vals = np.floor(mean_by_day["aht_sum"] + 0.5)
        out_df = pd.DataFrame({
            "date": mean_by_day["date"],
            "value": vals.astype("Int64"),
            "unit": "сек.",
        }).sort_values("date")

        return out_df.to_dict(orient="records")

    def get_call_aht_all(self) -> List[Dict[str, Any]]:
        """
        Аналог PHP getCallAHTAll().
        AHT = round(AVG(operator_pickup_time + speaking_time + wrapup_time)) по дням,
        только для connected и speaking_time > 10. Возвращает [{"date","value","unit"}].
        """
        sql = self._read_sql("get_call_aht_all.sql")
        params = {
            "start_date": self.start_date,  # правая граница исключительная (< end)
            "end_date": self.end_date,
        }

        rows = self._fetch_all(sql, params) or []
        if not rows:
            return []

        df = pd.DataFrame(rows)  # cols: attempt_start, operator_pickup_time, speaking_time, wrapup_time

        # Дата
        df["attempt_start"] = pd.to_datetime(df["attempt_start"], errors="coerce")
        df = df.dropna(subset=["attempt_start"])
        try:
            if getattr(df["attempt_start"].dt, "tz", None) is not None:
                df["attempt_start"] = df["attempt_start"].dt.tz_localize(None)
        except Exception:
            pass
        df["date"] = df["attempt_start"].dt.strftime("%Y-%m-%d")

        # Компоненты AHT как числа; если какая-то NaN → сумма NaN (как в SQL)
        for col in ["operator_pickup_time", "speaking_time", "wrapup_time"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df["aht_sum"] = df["operator_pickup_time"] + df["speaking_time"] + df["wrapup_time"]

        # Среднее по дням; mean игнорирует NaN — эквивалент AVG(NULLABLE_EXPR)
        mean_by_day = df.groupby("date", as_index=False)["aht_sum"].mean()

        # Округление HALF_UP до целого
        vals = np.floor(mean_by_day["aht_sum"] + 0.5)

        out_df = pd.DataFrame({
            "date": mean_by_day["date"],
            "value": pd.Series(vals).astype("Int64"),
            "unit": "сек.",
        }).sort_values("date")

        return out_df.to_dict(orient="records")

    def get_call_art(self) -> List[Dict[str, Any]]:
        """
        Аналог PHP getCallART().
        ART = round(AVG(operator_pickup_time)) по дням,
        только для connected и speaking_time > 10, с фильтром по ProjectID.
        Возвращает [{"date":"YYYY-MM-DD","value":int,"unit":"сек."}, ...]
        """
        sql = self._read_sql("get_call_art.sql")
        params = {
            "start_date": self.start_date,  # правая граница исключительная (< end)
            "end_date": self.end_date,
            "project_id": self.ProjectID,
        }

        rows = self._fetch_all(sql, params) or []
        if not rows:
            return []

        df = pd.DataFrame(rows)  # cols: attempt_start, operator_pickup_time

        # Дата
        df["attempt_start"] = pd.to_datetime(df["attempt_start"], errors="coerce")
        df = df.dropna(subset=["attempt_start"])
        try:
            if getattr(df["attempt_start"].dt, "tz", None) is not None:
                df["attempt_start"] = df["attempt_start"].dt.tz_localize(None)
        except Exception:
            pass
        df["date"] = df["attempt_start"].dt.strftime("%Y-%m-%d")

        # Приведение operator_pickup_time к числу; NaN будет игнорироваться в mean() (как NULL в AVG)
        df["operator_pickup_time"] = pd.to_numeric(df["operator_pickup_time"], errors="coerce")

        # Среднее по дням
        mean_by_day = df.groupby("date", as_index=False)["operator_pickup_time"].mean()

        # Округление HALF_UP до целого (как round() в Postgres)
        vals = np.floor(mean_by_day["operator_pickup_time"] + 0.5)

        out_df = pd.DataFrame({
            "date": mean_by_day["date"],
            "value": pd.Series(vals).astype("Int64"),
            "unit": "сек.",
        }).sort_values("date")

        return out_df.to_dict(orient="records")

    def get_call_art_all(self) -> List[Dict[str, Any]]:
        """
        Аналог PHP getCallART().
        ART = round(AVG(operator_pickup_time)) по дням,
        только для connected и speaking_time > 10, с фильтром по ProjectID.
        Возвращает [{"date":"YYYY-MM-DD","value":int,"unit":"сек."}, ...]
        """
        sql = self._read_sql("get_call_art_all.sql")
        params = {
            "start_date": self.start_date,  # правая граница исключительная (< end)
            "end_date": self.end_date,
        }

        rows = self._fetch_all(sql, params) or []
        if not rows:
            return []

        df = pd.DataFrame(rows)  # cols: attempt_start, operator_pickup_time

        # Дата
        df["attempt_start"] = pd.to_datetime(df["attempt_start"], errors="coerce")
        df = df.dropna(subset=["attempt_start"])
        try:
            if getattr(df["attempt_start"].dt, "tz", None) is not None:
                df["attempt_start"] = df["attempt_start"].dt.tz_localize(None)
        except Exception:
            pass
        df["date"] = df["attempt_start"].dt.strftime("%Y-%m-%d")

        # Приведение operator_pickup_time к числу; NaN будет игнорироваться в mean() (как NULL в AVG)
        df["operator_pickup_time"] = pd.to_numeric(df["operator_pickup_time"], errors="coerce")

        # Среднее по дням
        mean_by_day = df.groupby("date", as_index=False)["operator_pickup_time"].mean()

        # Округление HALF_UP до целого (как round() в Postgres)
        vals = np.floor(mean_by_day["operator_pickup_time"] + 0.5)

        out_df = pd.DataFrame({
            "date": mean_by_day["date"],
            "value": pd.Series(vals).astype("Int64"),
            "unit": "сек.",
        }).sort_values("date")

        return out_df.to_dict(orient="records")

    def get_call_aht_with_trunc(self, date_field: str = "date") -> List[Dict[str, Any]]:
        """
        Аналог PHP getCallAHTWithTrunc($dateField='date').
        AHT = round(AVG(operator_pickup_time + speaking_time + wrapup_time)) по бакетам trunc.
        Возвращает [{"<date_field>":"...", "value": int}, ...]
        """
        sql = self._read_sql("get_call_aht_with_trunc.sql")
        params = {
            "start_date": self.start_date,  # правая граница исключительная (< end)
            "end_date": self.end_date,
            "project_id": self.ProjectID,
        }

        rows = self._fetch_all(sql, params) or []
        if not rows:
            return []

        df = pd.DataFrame(rows)  # ожид.: attempt_start, operator_pickup_time, speaking_time, wrapup_time
        df["attempt_start"] = pd.to_datetime(df["attempt_start"], errors="coerce")
        df = df.dropna(subset=["attempt_start"])

        # убрать TZ, если есть
        try:
            if getattr(df["attempt_start"].dt, "tz", None) is not None:
                df["attempt_start"] = df["attempt_start"].dt.tz_localize(None)
        except Exception:
            pass

        # 1) Бакет по self.trunc
        trunc = (getattr(self, "trunc", "day") or "day").lower()
        if trunc == "month":
            df["_bucket_dt"] = df["attempt_start"].dt.to_period("M").dt.to_timestamp()  # начало месяца
        elif trunc == "hour":
            df["_bucket_dt"] = df["attempt_start"].dt.floor("H")
        else:  # day
            df["_bucket_dt"] = df["attempt_start"].dt.floor("D")

        # 2) Форматирование как to_char(..., self.date_format)
        def _pgfmt_to_py(fmt: str | None) -> str:
            if not fmt:
                return "%Y-%m-%d"
            py = fmt
            py = py.replace("HH24", "%H").replace("MI", "%M").replace("SS", "%S")
            py = py.replace("YYYY", "%Y").replace("MM", "%m").replace("DD", "%d")
            return py

        py_fmt = _pgfmt_to_py(getattr(self, "date_format", None))
        df[date_field] = df["_bucket_dt"].dt.strftime(py_fmt)

        # 3) Сумма компонент AHT
        for col in ["operator_pickup_time", "speaking_time", "wrapup_time"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df["aht_sum"] = df["operator_pickup_time"] + df["speaking_time"] + df["wrapup_time"]

        # 4) Среднее по бакету и округление HALF_UP
        mean_by_bucket = df.groupby(date_field, as_index=False)["aht_sum"].mean()
        vals = np.floor(mean_by_bucket["aht_sum"] + 0.5).astype("Int64")

        out_df = pd.DataFrame({
            date_field: mean_by_bucket[date_field],
            "value": vals,
        }).sort_values([date_field])

        return out_df.to_dict(orient="records")

    def get_coffee(self) -> List[Dict[str, Any]]:
        """
        Аналог PHP getCoffee():
          date = YYYY-MM-DD,
          row  = title,
          value = round(SUM(duration) / 60000) минут по дням и title
        Фильтры: период, OU, status='custom1'
        """
        sql = self._read_sql("get_coffee.sql")
        params = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "ou": self.Ou,
        }
        rows = self._fetch_all(sql, params) or []
        if not rows:
            return []

        df = pd.DataFrame(rows)  # entered, title, duration
        df["entered"] = pd.to_datetime(df["entered"], errors="coerce")
        df = df.dropna(subset=["entered"])
        try:
            if getattr(df["entered"].dt, "tz", None) is not None:
                df["entered"] = df["entered"].dt.tz_localize(None)
        except Exception:
            pass
        df["date"] = df["entered"].dt.strftime("%Y-%m-%d")

        df["duration"] = pd.to_numeric(df["duration"], errors="coerce")
        # суммируем миллисекунды → переводим в минуты → округляем до целого (HALF_UP)
        agg = df.groupby(["date", "title"], as_index=False)["duration"].sum()
        minutes = np.floor((agg["duration"] / 60000.0) + 0.5).astype("Int64")

        out_df = pd.DataFrame({
            "date": agg["date"],
            "row": agg["title"].astype(str),
            "value": minutes,
        }).sort_values("date")

        return out_df.to_dict(orient="records")

    def get_dinner(self) -> List[Dict[str, Any]]:
        """
        Аналог PHP getDinner():
          date = YYYY-MM-DD,
          row  = title (исключая пустые и из NotInTitles),
          value = round(SUM(duration) / 60000) минут по дням и title
        Фильтры: период, OU, status='away'
        """
        sql = self._read_sql("get_dinner.sql")
        params = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "ou": self.Ou,
        }
        rows = self._fetch_all(sql, params) or []
        if not rows:
            return []

        df = pd.DataFrame(rows)  # entered, title, duration
        df["entered"] = pd.to_datetime(df["entered"], errors="coerce")
        df = df.dropna(subset=["entered"])
        try:
            if getattr(df["entered"].dt, "tz", None) is not None:
                df["entered"] = df["entered"].dt.tz_localize(None)
        except Exception:
            pass
        df["date"] = df["entered"].dt.strftime("%Y-%m-%d")

        # фильтры по title: не пусто и не в NotInTitles
        df["title"] = df["title"].astype(str)
        df = df[df["title"].str.len() > 0]
        if getattr(self, "NotInTitles", None):
            df = df[~df["title"].isin(self.NotInTitles)]

        df["duration"] = pd.to_numeric(df["duration"], errors="coerce")
        agg = df.groupby(["date", "title"], as_index=False)["duration"].sum()
        minutes = np.floor((agg["duration"] / 60000.0) + 0.5).astype("Int64")

        out_df = pd.DataFrame({
            "date": agg["date"],
            "row": agg["title"],
            "value": minutes,
        }).sort_values("date")

        return out_df.to_dict(orient="records")

    def get_dnd(self) -> List[Dict[str, Any]]:
        """
        Аналог PHP getDnd():
          date = YYYY-MM-DD,
          row  = title (исключая NotInTitles),
          value = round(SUM(duration) / 60000) минут по дням и title
        Фильтры: период, OU, status='dnd'
        """
        sql = self._read_sql("get_dnd.sql")
        params = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "ou": self.Ou,
        }
        rows = self._fetch_all(sql, params) or []
        if not rows:
            return []

        df = pd.DataFrame(rows)  # entered, title, duration
        df["entered"] = pd.to_datetime(df["entered"], errors="coerce")
        df = df.dropna(subset=["entered"])
        try:
            if getattr(df["entered"].dt, "tz", None) is not None:
                df["entered"] = df["entered"].dt.tz_localize(None)
        except Exception:
            pass
        df["date"] = df["entered"].dt.strftime("%Y-%m-%d")

        df["title"] = df["title"].astype(str)
        if getattr(self, "NotInTitles", None):
            df = df[~df["title"].isin(self.NotInTitles)]

        df["duration"] = pd.to_numeric(df["duration"], errors="coerce")
        agg = df.groupby(["date", "title"], as_index=False)["duration"].sum()
        minutes = np.floor((agg["duration"] / 60000.0) + 0.5).astype("Int64")

        out_df = pd.DataFrame({
            "date": agg["date"],
            "row": agg["title"],
            "value": minutes,
        }).sort_values("date")

        return out_df.to_dict(orient="records")

    def get_occupancy(self) -> List[Dict[str, Any]]:
        sql = self._read_sql("get_occupancy.sql")
        params = {"start_date": self.start_date, "end_date": self.end_date, "ou": self.Ou}
        rows = self._fetch_all(sql, params) or []
        if not rows:
            return []

        df = pd.DataFrame(rows)
        df["entered"] = pd.to_datetime(df["entered"], errors="coerce")
        df = df.dropna(subset=["entered"])
        try:
            if getattr(df["entered"].dt, "tz", None) is not None:
                df["entered"] = df["entered"].dt.tz_localize(None)
        except Exception:
            pass
        df["date"] = df["entered"].dt.strftime("%Y-%m-%d")

        df["title"] = df["title"].astype(str)
        if getattr(self, "NotInTitles", None):
            df = df[~df["title"].isin(self.NotInTitles)]

        df["duration"] = pd.to_numeric(df["duration"], errors="coerce").fillna(0)

        busy_set = {"ringing", "speaking", "wrapup", "custom2"}
        denom_set = busy_set | {"normal"}

        df["num"] = np.where(df["status"].isin(busy_set), df["duration"], 0)
        df["den"] = np.where(df["status"].isin(denom_set), df["duration"], 0)

        agg = df.groupby(["date", "title"], as_index=False)[["num", "den"]].sum()

        # % = round_half_up(num/den*100), но 0 если num==0
        ratio = np.where(agg["den"] > 0, agg["num"] / agg["den"], 0.0)
        value = pd.Series(
            np.where(agg["num"] > 0, np.floor(ratio * 100.0 + 0.5), 0),
            index=agg.index,
            dtype="Int64",  # <-- теперь это pandas Series с nullable int
        )

        out_df = pd.DataFrame({
            "date": agg["date"],
            "row": agg["title"],
            "value": value,
        }).sort_values("date")

        return out_df.to_dict(orient="records")

    def get_occupancy_graphic(self) -> List[Dict[str, Any]]:
        """
        Аналог PHP getOccupancyGraphic().
        Возвращает [{"date":"YYYY-MM-DD","value":<int %>}, ...]
        """
        sql = self._read_sql("get_occupancy.sql")
        params = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "ou": self.Ou,
        }
        rows = self._fetch_all(sql, params) or []
        if not rows:
            return []

        df = pd.DataFrame(rows)
        df["entered"] = pd.to_datetime(df["entered"], errors="coerce")
        df = df.dropna(subset=["entered"])
        try:
            if getattr(df["entered"].dt, "tz", None) is not None:
                df["entered"] = df["entered"].dt.tz_localize(None)
        except Exception:
            pass
        df["date"] = df["entered"].dt.strftime("%Y-%m-%d")

        df["title"] = df["title"].astype(str)
        if getattr(self, "NotInTitles", None):
            df = df[~df["title"].isin(self.NotInTitles)]

        df["duration"] = pd.to_numeric(df["duration"], errors="coerce").fillna(0)

        busy_set = {"ringing", "speaking", "wrapup", "custom2"}
        denom_set = busy_set | {"normal"}

        df["num"] = np.where(df["status"].isin(busy_set), df["duration"], 0)
        df["den"] = np.where(df["status"].isin(denom_set), df["duration"], 0)

        agg = df.groupby(["date"], as_index=False)[["num", "den"]].sum()

        ratio = np.where(agg["den"] > 0, agg["num"] / agg["den"], 0.0)
        value = pd.Series(
            np.where(agg["num"] > 0, np.floor((np.where(agg["den"] > 0, agg["num"] / agg["den"], 0.0)) * 100 + 0.5), 0),
            index=agg.index,
            dtype="Int64",
        )

        out_df = pd.DataFrame({
            "date": agg["date"],
            "value": value,
        }).sort_values("date")

        return out_df.to_dict(orient="records")

    def get_occupancy_graphic_with_trunc(self, date_field: str = "date") -> List[Dict[str, Any]]:
        """
        Аналог PHP getOccupancyGraphicWithTrunc($dateField='date').
        Бакет по self.trunc (hour/day/month), формат по self.date_format.
        Возвращает [{"<date_field>":"...", "value":<int %>}, ...]
        """
        sql = self._read_sql("get_occupancy.sql")
        params = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "ou": self.Ou,
        }
        rows = self._fetch_all(sql, params) or []
        if not rows:
            return []

        df = pd.DataFrame(rows)
        df["entered"] = pd.to_datetime(df["entered"], errors="coerce")
        df = df.dropna(subset=["entered"])

        try:
            if getattr(df["entered"].dt, "tz", None) is not None:
                df["entered"] = df["entered"].dt.tz_localize(None)
        except Exception:
            pass

        # бакет
        trunc = (getattr(self, "trunc", "day") or "day").lower()
        if trunc == "month":
            df["_bucket_dt"] = df["entered"].dt.to_period("M").dt.to_timestamp()
        elif trunc == "hour":
            df["_bucket_dt"] = df["entered"].dt.floor("H")
        else:
            df["_bucket_dt"] = df["entered"].dt.floor("D")

        # формат
        def _pgfmt_to_py(fmt: str | None) -> str:
            if not fmt:
                return "%Y-%m-%d"
            py = fmt
            py = py.replace("HH24", "%H").replace("MI", "%M").replace("SS", "%S")
            py = py.replace("YYYY", "%Y").replace("MM", "%m").replace("DD", "%d")
            return py

        py_fmt = _pgfmt_to_py(getattr(self, "date_format", None))
        df[date_field] = df["_bucket_dt"].dt.strftime(py_fmt)

        df["title"] = df["title"].astype(str)
        if getattr(self, "NotInTitles", None):
            df = df[~df["title"].isin(self.NotInTitles)]

        df["duration"] = pd.to_numeric(df["duration"], errors="coerce").fillna(0)

        busy_set = {"ringing", "speaking", "wrapup", "custom2"}
        denom_set = busy_set | {"normal"}

        df["num"] = np.where(df["status"].isin(busy_set), df["duration"], 0)
        df["den"] = np.where(df["status"].isin(denom_set), df["duration"], 0)

        agg = df.groupby([date_field], as_index=False)[["num", "den"]].sum()

        ratio = np.where(agg["den"] > 0, agg["num"] / agg["den"], 0.0)
        value = pd.Series(
            np.where(agg["num"] > 0, np.floor((np.where(agg["den"] > 0, agg["num"] / agg["den"], 0.0)) * 100 + 0.5), 0),
            index=agg.index,
            dtype="Int64",
        )

        out_df = pd.DataFrame({
            date_field: agg[date_field],
            "value": value,
        }).sort_values([date_field])

        return out_df.to_dict(orient="records")

    def get_request_time(self) -> List[Dict[str, Any]]:
        """
        Аналог PHP getRequestTime().
        date=YYYY-MM-DD, row=title (из mv_employee), value=округлённые минуты (SUM(duration)/60000).
        Фильтры: период, me.ou=self.Ou, status='custom2', исключение NotInTitles.
        """
        sql = self._read_sql("get_request_time.sql")

        print(sql)

        params = {"start_date": self.start_date, "end_date": self.end_date, "ou": self.Ou}
        rows = self._fetch_all(sql, params) or []
        if not rows:
            return []

        df = pd.DataFrame(rows)  # entered, title, duration

        print(f"do{df}")

        df["entered"] = pd.to_datetime(df["entered"], errors="coerce")
        df = df.dropna(subset=["entered"])
        df["date"] = df["entered"].dt.strftime("%Y-%m-%d")
        df["title"] = df["title"].astype(str)
        if getattr(self, "NotInTitles", None):
            df = df[~df["title"].isin(self.NotInTitles)]
        df["duration"] = pd.to_numeric(df["duration"], errors="coerce").fillna(0)

        agg = df.groupby(["date", "title"], as_index=False)["duration"].sum()
        minutes = pd.Series(np.floor(agg["duration"] / 60000.0 + 0.5), index=agg.index, dtype="Int64")

        out_df = pd.DataFrame({"date": agg["date"], "row": agg["title"], "value": minutes}).sort_values("date")

        print(f"posle{out_df}")

        return out_df.to_dict(orient="records")

    def get_summary_status_duration(self) -> List[Dict[str, Any]]:
        """
        Аналог PHP getSummaryStatusDuration().
        UNION из:
          (A) status='away' AND reason NOT NULL → row="away - <reason>"
          (B) все статусы, кроме ['speaking#voice','wrapup#voice','ringing#voice','offline'] → row=status
        Везде: value = округлённые минуты (SUM(duration)/60000), исключаем NotInTitles.
        """
        sql = self._read_sql("get_summary_status_duration.sql")
        params = {"start_date": self.start_date, "end_date": self.end_date, "ou": self.Ou}
        rows = self._fetch_all(sql, params) or []
        if not rows:
            return []

        df = pd.DataFrame(rows)  # entered, title, status, reason, duration
        df["entered"] = pd.to_datetime(df["entered"], errors="coerce")
        df = df.dropna(subset=["entered"])
        df["date"] = df["entered"].dt.strftime("%Y-%m-%d")
        df["title"] = df["title"].astype(str)
        if getattr(self, "NotInTitles", None):
            df = df[~df["title"].isin(self.NotInTitles)]
        df["duration"] = pd.to_numeric(df["duration"], errors="coerce").fillna(0)
        df["status"] = df["status"].astype(str)
        df["reason"] = df["reason"].astype(str)

        # (A) away с reason
        df_away = df[(df["status"] == "away") & (df["reason"].str.len() > 0)].copy()
        if not df_away.empty:
            df_away["row"] = df_away.apply(lambda r: f'{r["status"]} - {r["reason"]}', axis=1)
            agg_a = df_away.groupby(["date", "row"], as_index=False)["duration"].sum()
            agg_a["value"] = pd.Series(np.floor(agg_a["duration"] / 60000.0 + 0.5), index=agg_a.index, dtype="Int64")
            agg_a = agg_a[["date", "row", "value"]]
        else:
            agg_a = pd.DataFrame(columns=["date", "row", "value"])

        # (B) все, кроме voice/offline
        exclude = {"speaking#voice", "wrapup#voice", "ringing#voice", "offline"}
        df_b = df[~df["status"].isin(exclude)].copy()
        if not df_b.empty:
            agg_b = df_b.groupby(["date", "status"], as_index=False)["duration"].sum().rename(columns={"status": "row"})
            agg_b["value"] = pd.Series(np.floor(agg_b["duration"] / 60000.0 + 0.5), index=agg_b.index, dtype="Int64")
            agg_b = agg_b[["date", "row", "value"]]
        else:
            agg_b = pd.DataFrame(columns=["date", "row", "value"])

        out_df = pd.concat([agg_b, agg_a], ignore_index=True).sort_values("date")
        return out_df.to_dict(orient="records")

    def get_calls_duration(self) -> List[Dict[str, Any]]:
        """
        Аналог PHP getCallsDuration().
        Бакеты по speaking_time (в секундах) → name, значение = COUNT(session_id) по дням и name.
        Фильтры: period, project_id, attempt_result='connected', me.ou, исключение NotInTitles.
        """
        sql = self._read_sql("get_calls_duration.sql")
        params = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "project_id": self.ProjectID,
            "ou": self.Ou,
        }
        rows = self._fetch_all(sql, params) or []
        if not rows:
            return []

        df = pd.DataFrame(rows)  # attempt_start, speaking_time, session_id, title
        df["attempt_start"] = pd.to_datetime(df["attempt_start"], errors="coerce")
        df = df.dropna(subset=["attempt_start"])
        df["date"] = df["attempt_start"].dt.strftime("%Y-%m-%d")

        df["title"] = df["title"].astype(str)
        if getattr(self, "NotInTitles", None):
            df = df[~df["title"].isin(self.NotInTitles)]

        # speaking_time считаем секундами (как в оригинале для detail_outbound_sessions)
        st = pd.to_numeric(df["speaking_time"], errors="coerce").fillna(-1)

        bins = [
            (st >= 0) & (st <= 5),
            (st >= 6) & (st <= 10),
            (st >= 11) & (st <= 30),
            (st >= 31) & (st <= 60),
            (st >= 61) & (st <= 120),
            (st >= 121) & (st <= 300),
        ]
        labels = [
            "От 0 до 5 секунд",
            "От 5 до 10 секунд",
            "От 10 до 30 секунд",
            "От 30 до 60 секунд",
            "От 60 до 120 секунд",
            "От 120 до 300 секунд",
        ]
        df["name"] = np.select(bins, labels, default="Более 300 секунд")

        # COUNT(session_id) по дням и name
        val = (df.groupby(["date", "name"])["session_id"]
               .count()
               .reset_index()
               .rename(columns={"session_id": "value"})
               .sort_values("date"))

        return val[["date", "name", "value"]].to_dict(orient="records")

    def get_work_time_data(self) -> List[Dict[str, Any]]:
        """
        Аналог PHP getWorkTimeData():
          date=YYYY-MM-DD, row=title (из mv_employee), value=round(SUM(duration)/60000) минут.
          Фильтры: период, me.ou, status in ['available','notavailable'], исключая NotInTitles.
        """
        sql = self._read_sql("get_work_time_data.sql")
        params = {"start_date": self.start_date, "end_date": self.end_date, "ou": self.Ou}
        rows = self._fetch_all(sql, params) or []
        if not rows:
            return []

        df = pd.DataFrame(rows)  # entered, title, duration
        df["entered"] = pd.to_datetime(df["entered"], errors="coerce")
        df = df.dropna(subset=["entered"])
        df["date"] = df["entered"].dt.strftime("%Y-%m-%d")

        df["title"] = df["title"].astype(str)
        if getattr(self, "NotInTitles", None):
            df = df[~df["title"].isin(self.NotInTitles)]

        df["duration"] = pd.to_numeric(df["duration"], errors="coerce").fillna(0)

        agg = df.groupby(["date", "title"], as_index=False)["duration"].sum()
        minutes = pd.Series(np.floor(agg["duration"] / 60000.0 + 0.5), index=agg.index, dtype="Int64")

        out_df = pd.DataFrame({"date": agg["date"], "row": agg["title"], "value": minutes}).sort_values("date")
        return out_df.to_dict(orient="records")

    def get_personal_aht(self) -> List[Dict[str, Any]]:
        """
        Аналог PHP getPersonalAHT():
          date=YYYY-MM-DD, row=title (из mv_employee), value=round(AVG(op_pickup + speaking + wrapup)).
          Фильтры: период, attempt_result='connected', speaking_time>10, me.ou, project_id, исключая NotInTitles.
        """
        sql = self._read_sql("get_personal_aht.sql")
        params = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "ou": self.Ou,
            "project_id": self.ProjectID,
        }
        rows = self._fetch_all(sql, params) or []
        if not rows:
            return []

        df = pd.DataFrame(rows)  # attempt_start, operator_pickup_time, speaking_time, wrapup_time, title
        df["attempt_start"] = pd.to_datetime(df["attempt_start"], errors="coerce")
        df = df.dropna(subset=["attempt_start"])
        df["date"] = df["attempt_start"].dt.strftime("%Y-%m-%d")

        df["title"] = df["title"].astype(str)
        if getattr(self, "NotInTitles", None):
            df = df[~df["title"].isin(self.NotInTitles)]

        for col in ["operator_pickup_time", "speaking_time", "wrapup_time"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df["aht_sum"] = df["operator_pickup_time"] + df["speaking_time"] + df["wrapup_time"]

        # среднее по сотруднику и дню
        mean_df = df.groupby(["date", "title"], as_index=False)["aht_sum"].mean()
        value = pd.Series(np.floor(mean_df["aht_sum"] + 0.5), index=mean_df.index, dtype="Int64")

        out_df = pd.DataFrame({"date": mean_df["date"], "row": mean_df["title"], "value": value}).sort_values("date")
        return out_df.to_dict(orient="records")

    def get_employee_quantity(self) -> List[Dict[str, Any]]:
        """
        Аналог PHP getEmployeeQuantity():
          date=YYYY-MM-DD, value=COUNT(DISTINCT title) при operator_pickup_time IS NOT NULL.
          Фильтры: период, me.ou, project_id.
        """
        sql = self._read_sql("get_employee_quantity.sql")
        params = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "ou": self.Ou,
            "project_id": self.ProjectID,
        }
        rows = self._fetch_all(sql, params) or []
        if not rows:
            return []

        df = pd.DataFrame(rows)  # attempt_start, operator_pickup_time, title
        df["attempt_start"] = pd.to_datetime(df["attempt_start"], errors="coerce")
        df = df.dropna(subset=["attempt_start"])
        df["date"] = df["attempt_start"].dt.strftime("%Y-%m-%d")

        # только строки с pickup_time не NULL
        df = df[df["operator_pickup_time"].notna()]
        df["title"] = df["title"].astype(str)

        out = (
            df.groupby("date")["title"]
            .nunique()
            .reset_index()
            .rename(columns={"title": "value"})
            .sort_values("date")
        )

        return out[["date", "value"]].to_dict(orient="records")

    def get_income_calls(
            self,
            projects: Sequence[str] = (
                    "corebo00000000000navun4ib27664kg",
                    "corebo00000000000navundqn4raplhk",
                    "corebo00000000000navun8614k6dg2c",
                    "corebo00000000000navuni104pvnubo",
            ),
    ) -> List[Dict[str, Any]]:
        """
        Аналог PHP getIncomeCalls(): количество входящих по дням (10:00–19:59).
        Возвращает [{"date":"YYYY-MM-DD","value":int}, ...]
        """
        sql = self._read_sql("get_income_calls.sql")
        params = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "projects": list(projects),
        }
        rows = self._fetch_all(sql, params) or []
        if not rows:
            return []

        df = pd.DataFrame(rows)  # enqueued_time, session_id
        df["enqueued_time"] = pd.to_datetime(df["enqueued_time"], errors="coerce")
        df = df.dropna(subset=["enqueued_time"])
        df["date"] = df["enqueued_time"].dt.strftime("%Y-%m-%d")

        out = (
            df.groupby("date")["session_id"]
            .count()
            .reset_index()
            .rename(columns={"session_id": "value"})
            .sort_values("date")
        )
        return out.to_dict(orient="records")

    def get_income_sl(
            self,
            projects: Sequence[str] = (
                    "corebo00000000000navun4ib27664kg",
                    "corebo00000000000navundqn4raplhk",
                    "corebo00000000000navun8614k6dg2c",
                    "corebo00000000000navuni104pvnubo",
            ),
    ) -> List[Dict[str, Any]]:
        """
        Аналог PHP getIncomeSL(): по дням (10:00–19:59) считаем:
          hcr = % от всех, где final_stage='operator'
          sl  = % от всех, где final_stage='operator' и (dequeued_time - unblocked_time) <= 40 сек
        Возвращает [{"date":"YYYY-MM-DD","hcr":int,"sl":int}, ...]
        """
        sql = self._read_sql("get_income_sl.sql")
        params = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "projects": list(projects),
        }
        rows = self._fetch_all(sql, params) or []
        if not rows:
            return []

        df = pd.DataFrame(rows)  # enqueued_time, final_stage, dequeued_time, unblocked_time, session_id
        to_dt = lambda s: pd.to_datetime(s, errors="coerce")
        df["enqueued_time"] = to_dt(df["enqueued_time"])
        df["dequeued_time"] = to_dt(df["dequeued_time"])
        df["unblocked_time"] = to_dt(df["unblocked_time"])
        df = df.dropna(subset=["enqueued_time"])
        df["date"] = df["enqueued_time"].dt.strftime("%Y-%m-%d")

        # Метки
        df["is_op"] = (df["final_stage"] == "operator").astype(int)

        secs = (df["dequeued_time"] - df["unblocked_time"]).dt.total_seconds()
        df["is_sl"] = ((df["final_stage"] == "operator") & (secs.notna()) & (secs <= 40)).astype(int)

        # Агрегаты по дням
        agg = (
            df.groupby("date", as_index=False)[["is_op", "is_sl", "session_id"]]
            .agg({"is_op": "sum", "is_sl": "sum", "session_id": "count"})
            .rename(columns={"session_id": "total"})
            .sort_values("date")
        )

        # Проценты с HALF_UP до целого; при total==0 → 0
        hcr = np.where(agg["total"] > 0, np.floor(agg["is_op"] / agg["total"] * 100.0 + 0.5), 0).astype(int)
        sl = np.where(agg["total"] > 0, np.floor(agg["is_sl"] / agg["total"] * 100.0 + 0.5), 0).astype(int)

        out_df = pd.DataFrame({"date": agg["date"], "hcr": hcr, "sl": sl})
        return out_df.to_dict(orient="records")

    def get_outcoming_total_amount_calls_data(self, project_id: str) -> List[Dict[str, Any]]:
        """
        Аналог PHP getOutComingTotalAmountCallsData($projectId):
          количество исходящих по дням (COUNT(number_type)), для заданного project_id.
        Возвращает [{"date":"YYYY-MM-DD","value":int}, ...]
        """
        sql = self._read_sql("get_outcoming_total_amount_calls_data.sql")
        params = {"start_date": self.start_date, "end_date": self.end_date, "project_id": project_id}
        rows = self._fetch_all(sql, params) or []
        if not rows:
            return []

        df = pd.DataFrame(rows)  # attempt_start, number_type
        df["attempt_start"] = pd.to_datetime(df["attempt_start"], errors="coerce")
        df = df.dropna(subset=["attempt_start"])
        df["date"] = df["attempt_start"].dt.strftime("%Y-%m-%d")

        # COUNT(number_type) — считаем только не-NULL
        df["cnt"] = df["number_type"].notna().astype(int)
        out = (
            df.groupby("date", as_index=False)["cnt"]
            .sum()
            .rename(columns={"cnt": "value"})
            .sort_values("date")
        )
        return out.to_dict(orient="records")

    def get_successful_out_coming_calls(self, project_id: str) -> List[Dict[str, Any]]:
        """
        Аналог PHP getSuccessfulOutComingCalls($projectId):
          COUNT(number_type) по дням для attempt_result='connected'.
        """
        sql = self._read_sql("get_successful_out_coming_calls.sql")
        params = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "project_id": project_id,
        }
        rows = self._fetch_all(sql, params) or []
        if not rows:
            return []

        df = pd.DataFrame(rows)  # attempt_start, number_type
        df["attempt_start"] = pd.to_datetime(df["attempt_start"], errors="coerce")
        df = df.dropna(subset=["attempt_start"])
        df["date"] = df["attempt_start"].dt.strftime("%Y-%m-%d")

        # COUNT(number_type) = считаем только не-NULL
        df["cnt"] = df["number_type"].notna().astype(int)
        out = (
            df.groupby("date", as_index=False)["cnt"]
            .sum()
            .rename(columns={"cnt": "value"})
            .sort_values("date")
        )
        return out.to_dict(orient="records")

    def get_failed_out_coming_calls(self, project_id: str) -> List[Dict[str, Any]]:
        """
        Аналог PHP getFailedOutComingCalls($projectId):
          Для НЕ connected → маппим attempt_result в русские категории, считаем COUNT(number_type).
          Возвращаем декартово произведение (date × row) с 0 → None.
        """
        sql = self._read_sql("get_failed_out_coming_calls.sql")
        params = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "project_id": project_id,
        }
        rows = self._fetch_all(sql, params) or []
        if not rows:
            return []

        df = pd.DataFrame(rows)  # attempt_start, attempt_result, number_type
        df["attempt_start"] = pd.to_datetime(df["attempt_start"], errors="coerce")
        df = df.dropna(subset=["attempt_start"])
        df["date"] = df["attempt_start"].dt.strftime("%Y-%m-%d")

        # Маппинг причин (как в PHP)
        reason_map = {
            "busy": "Абонент занят",
            "no_answer": "Абонент не принял вызов",
            "rejected": "Абонент отклонил вызов",
            "operator_busy": "Оператор занят",
            "operator_no_answer": "Оператор не принял вызов",
            "operator_rejected": "Оператор отклонил вызов",
            "not_found": "Вызываемый номер не существует",
            "UNKNOWN_ERROR": "Неизвестный код отбоя",
            "abandoned": "Потерянный вызов",
            "amd": "Автоответчик",
            "CRR_DISCONNECT": "Обрыв связи",
            "CRR_INVALID": "Неправильный номер",
            "message_not_played": "Абонент завершил вызов во время IVR",
            "CRR_UNAVAILABLE": "Не берут трубку",
        }
        df["row"] = df["attempt_result"].map(reason_map).fillna("Неизвестно")

        # COUNT(number_type)
        df["cnt"] = df["number_type"].notna().astype(int)
        agg = (
            df.groupby(["date", "row"], as_index=False)["cnt"]
            .sum()
            .rename(columns={"cnt": "value"})
        )

        # Сетка date × row из фактических уникальных значений (как в PHP)
        dates = sorted(agg["date"].unique().tolist())
        rows_labels = sorted(agg["row"].unique().tolist())
        idx = pd.MultiIndex.from_product([dates, rows_labels], names=["date", "row"])

        full_df = (
            agg.set_index(["date", "row"])
            .reindex(idx, fill_value=0)
            .reset_index()
        )
        # 0 → None
        full_df["value"] = full_df["value"].where(full_df["value"] != 0, None)

        return full_df[["date", "row", "value"]].to_dict(orient="records")

    def get_queued_calls_by_projects(
            self,
            projects: Sequence[str],
            final_stage: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Аналог PHP getQueuedCallsByProjects($projects, $finalStage=false):
          COUNT(session_id) по дням, опционально только final_stage='operator'.
        """
        sql = self._read_sql("get_queued_calls_by_projects.sql")
        params = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "projects": list(projects),
        }
        rows = self._fetch_all(sql, params) or []
        if not rows:
            return []

        df = pd.DataFrame(rows)  # enqueued_time, session_id, final_stage
        df["enqueued_time"] = pd.to_datetime(df["enqueued_time"], errors="coerce")
        df = df.dropna(subset=["enqueued_time"])
        if final_stage:
            df = df[df["final_stage"] == "operator"]

        df["date"] = df["enqueued_time"].dt.strftime("%Y-%m-%d")

        out = (
            df.groupby("date", as_index=False)["session_id"]
            .count()
            .rename(columns={"session_id": "value"})
            .sort_values("date")
        )
        return out.to_dict(orient="records")

    def get_missed_calls_by_operators(self, department_uuid: str, date_field: str = "date") -> List[Dict[str, Any]]:
        """
        Аналог PHP getMissedCallsByOperators($departmentUUID, $dateField='date').
        Считает пропущенные входящие по оператору:
          missed = SUM( qc.project_id NOT NULL AND cl.voip_reason IN ('603') )
        Группировка: (bucketed date, emp.login, user_title). Сортировка по missed DESC.
        """
        sql = self._read_sql("get_missed_calls_by_operators.sql")
        params = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "ou": department_uuid,
        }
        rows = self._fetch_all(sql, params) or []
        if not rows:
            return []

        df = pd.DataFrame(rows)  # created, login, firstname, lastname, voip_reason, project_id
        # время
        df["created"] = pd.to_datetime(df["created"], errors="coerce")
        df = df.dropna(subset=["created"])

        # бакет по self.trunc
        trunc = (getattr(self, "trunc", "day") or "day").lower()
        if trunc == "month":
            df["_bucket_dt"] = df["created"].dt.to_period("M").dt.to_timestamp()
        elif trunc == "hour":
            df["_bucket_dt"] = df["created"].dt.floor("H")
        else:
            df["_bucket_dt"] = df["created"].dt.floor("D")

        # формат как to_char(..., self.date_format)
        def _pgfmt_to_py(fmt: str | None) -> str:
            if not fmt:
                return "%Y-%m-%d"
            return (
                fmt.replace("HH24", "%H")
                .replace("MI", "%M")
                .replace("SS", "%S")
                .replace("YYYY", "%Y")
                .replace("MM", "%m")
                .replace("DD", "%d")
            )

        py_fmt = _pgfmt_to_py(getattr(self, "date_format", None))
        df[date_field] = df["_bucket_dt"].dt.strftime(py_fmt)

        # логин + ФИО
        df["login"] = df["login"].astype(str)
        ln = df.get("lastname").fillna("").astype(str)
        fn = df.get("firstname").fillna("").astype(str)
        df["user_title"] = (ln + " " + fn).str.strip()

        # missed-флаг
        df["voip_reason"] = df["voip_reason"].astype(str)
        df["missed_flag"] = ((df["project_id"].notna()) & (df["voip_reason"].isin(["603"]))).astype(int)

        agg = (
            df.groupby([date_field, "login", "user_title"], as_index=False)["missed_flag"]
            .sum()
            .rename(columns={"missed_flag": "missed"})
        )

        out_df = agg.sort_values(["missed"], ascending=False)
        return out_df[[date_field, "login", "user_title", "missed"]].to_dict(orient="records")

    def set_hours_trunked_data(self) -> None:
        self.trunc = 'hour'
        self.date_format = "YYYY-MM-DD HH24:MI:SS"

    def set_days_trunked_data(self) -> None:
        self.trunc = 'day'
        self.date_format = "YYYY-MM-DD"

    def set_months_trunked_data(self) -> None:
        self.trunc = 'month'
        self.date_format = "YYYY-MM-DD"

    def get_project_spent_time_last_30_days(self) -> List[Dict[str, Any]]:
        """
        Аналог PHP getProjectSpentTimeLast30Days():
          по дням и проектам суммируется (op_pickup + speaking + wrapup), перевод в минуты (÷60000),
          округление HALF_UP. Затем исключения и маппинг названий, итоговая агрегация.
        Возвращает [{"date":"YYYY-MM-DD","name":"<project>", "value": int}, ...]
        """
        sql = self._read_sql("get_project_spent_time_last_30_days.sql")
        params = {"start_date": self.start_date, "end_date": self.end_date}
        rows = self._fetch_all(sql, params) or []
        if not rows:
            return []

        df = pd.DataFrame(rows)  # attempt_start, operator_pickup_time, speaking_time, wrapup_time, project_title
        df["attempt_start"] = pd.to_datetime(df["attempt_start"], errors="coerce")
        df = df.dropna(subset=["attempt_start"])
        df["date"] = df["attempt_start"].dt.strftime("%Y-%m-%d")

        for c in ["operator_pickup_time", "speaking_time", "wrapup_time"]:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

        df["sum_ms"] = df["operator_pickup_time"] + df["speaking_time"] + df["wrapup_time"]

        agg = (
            df.groupby(["date", "project_title"], as_index=False)["sum_ms"]
            .sum()
        )
        # минуты с HALF_UP
        agg["value"] = np.floor(agg["sum_ms"] / 60000.0 + 0.5).astype(int)

        # Исключения
        excluded = {
            '[МКК] ТЕСТ2 - После 3',
            '[МКК] Soft Collection (O!Bank clients)',
            '[МКК] MKK - После 3 дней',
            '[МКК] AKCHABULAK',
            '[МКК] O! Bank',
            '[МКК] AKCHABULAK (Копия)',
            '[МКК] O! Bank (Копия)',
        }
        agg = agg[~agg["project_title"].isin(excluded)].copy()

        # Маппинг
        mapping = {
            # [NUR]-[АНКЕТИРОВАНИЕ] Индекс удовлетворенности
            '[NUR]-[АНКЕТИРОВАНИЕ] Бишкек': '[NUR]-[АНКЕТИРОВАНИЕ] Индекс удовлетворенности',
            '[NUR]-[АНКЕТИРОВАНИЕ] Чуй': '[NUR]-[АНКЕТИРОВАНИЕ] Индекс удовлетворенности',
            '[NUR]-[АНКЕТИРОВАНИЕ] Иссык-Куль': '[NUR]-[АНКЕТИРОВАНИЕ] Индекс удовлетворенности',
            '[NUR]-[АНКЕТИРОВАНИЕ] Джалал-Абад': '[NUR]-[АНКЕТИРОВАНИЕ] Индекс удовлетворенности',
            '[NUR]-[АНКЕТИРОВАНИЕ] Ошская область': '[NUR]-[АНКЕТИРОВАНИЕ] Индекс удовлетворенности',
            '[NUR]-[АНКЕТИРОВАНИЕ] Баткен': '[NUR]-[АНКЕТИРОВАНИЕ] Индекс удовлетворенности',
            '[NUR]-[АНКЕТИРОВАНИЕ] Талас': '[NUR]-[АНКЕТИРОВАНИЕ] Индекс удовлетворенности',
            '[NUR]-[АНКЕТИРОВАНИЕ] Ош': '[NUR]-[АНКЕТИРОВАНИЕ] Индекс удовлетворенности',
            '[NUR]-[АНКЕТИРОВАНИЕ] Нарын': '[NUR]-[АНКЕТИРОВАНИЕ] Индекс удовлетворенности',
            # [O!Bank]-[АНКЕТИРОВАНИЕ] NPS
            '[O!Bank]-[АНКЕТИРОВАНИЕ] NPS-O!Bank - Кенч': '[O!Bank]-[АНКЕТИРОВАНИЕ] NPS',
            '[O!Bank]-[АНКЕТИРОВАНИЕ] NPS-O!Bank - Бишкек': '[O!Bank]-[АНКЕТИРОВАНИЕ] NPS',
            '[O!Bank]-[АНКЕТИРОВАНИЕ] NPS-O!Bank - Ош': '[O!Bank]-[АНКЕТИРОВАНИЕ] NPS',
            '[O!Bank]-[АНКЕТИРОВАНИЕ] NPS-O!Bank - Жалал-Абад': '[O!Bank]-[АНКЕТИРОВАНИЕ] NPS',
            '[O!Bank]-[АНКЕТИРОВАНИЕ] NPS-O!Bank - Чолпон-Ата': '[O!Bank]-[АНКЕТИРОВАНИЕ] NPS',
            '[O!Bank]-[АНКЕТИРОВАНИЕ] NPS-O!Bank - Кара-Суу': '[O!Bank]-[АНКЕТИРОВАНИЕ] NPS',
            '[O!Bank]-[АНКЕТИРОВАНИЕ] NPS-O!Bank - Азия': '[O!Bank]-[АНКЕТИРОВАНИЕ] NPS',
            '[O!Bank]-[АНКЕТИРОВАНИЕ] NPS-O!Bank - Жайыл': '[O!Bank]-[АНКЕТИРОВАНИЕ] NPS',
            '[O!Bank]-[АНКЕТИРОВАНИЕ] NPS-O!Bank - Каракол': '[O!Bank]-[АНКЕТИРОВАНИЕ] NPS',
            '[O!Bank]-[АНКЕТИРОВАНИЕ] NPS-O!Bank - Юг': '[O!Bank]-[АНКЕТИРОВАНИЕ] NPS',
        }
        agg["name"] = agg["project_title"].map(mapping).fillna(agg["project_title"])

        # возможные слияния названий → агрегируем повторно
        out = (
            agg.groupby(["date", "name"], as_index=False)["value"]
            .sum()
            .sort_values(["date", "name"])
        )
        return out.to_dict(orient="records")

    def get_users(self) -> List[str]:
        """
        PHP getUsers(): DISTINCT логины за окно [start, end) из detail_outbound_sessions_ms.
        Возвращает List[str] (отсортирован по login ASC).
        """
        sql = self._read_sql("get_users.sql")  # из sql/naumen/
        rows = self._fetch_all(sql, {"start_date": self.start_date, "end_date": self.end_date}) or []
        return [r["login"] for r in rows if r.get("login")]

    def get_spent_time(self) -> List[Dict[str, Any]]:
        """
        PHP getSpentTime(): берём только пользователей из getUsers(),
        из BPM считаем SUM(total_hours) (inclusive по датам).
        Возвращает [{"date":"YYYY-MM-DD","operator_login":str,"total_hours":float}, ...]
        """
        logins = self.get_users()
        if not logins:
            return []

        sql = self._read_sql("get_spent_time.sql", "bpm")  # из sql/bpm/
        params = {
            "logins": logins,
            "start_date": self.start_date,
            "end_date": self.end_date,
        }

        # тут читаем именно через BPM-коннект
        with self.conn_bpm.cursor() as cur:
            cur.execute(sql, params)
            cols = [c[0] for c in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]

        return rows or []

    def get_time_ratio(self, position_ids: Sequence[int] = (19,)) -> List[Dict[str, Any]]:
        """
        1) total_hours — из get_spent_time()
        2) spent_time  — из OccupancyDataGetter.getProductTimeByPositionIds([...], end_date)
           (duration в секундах по (date, login))
        3) value = round((spent/3600) / total_hours * 100), оставить только value > 0
        """
        # --- 1) total_hours ---
        total_rows = self.get_spent_time()
        if not total_rows:
            return []

        total_map: Dict[tuple, float] = {}
        for r in total_rows:
            date = r.get("date")
            login = r.get("operator_login")
            th = float(r.get("total_hours") or 0)
            if date and login:
                total_map[(date, login)] = total_map.get((date, login), 0.0) + th

        occ = OccupancyDataGetter(
            bpm_conn=self.conn_bpm,
            conn_naumen_1=self.conn,
            conn_naumen_3=self.conn,
            start_date=self.start_date,
            end_date=self.end_date,
        )

        occ.set_days_trunked_data()

        product_times = occ.get_product_time_by_position_ids([19], self.end_date)

        spent_map: Dict[tuple, int] = {}
        for r in product_times or []:
            date = r.get("date")
            login = r.get("login")
            dur = int(r.get("duration") or 0)  # секунды
            if date and login:
                key = (date, login)
                spent_map[key] = spent_map.get(key, 0) + dur

        # --- 3) расчёт процента и фильтр > 0 ---
        out: List[Dict[str, Any]] = []
        for (date, login), total_hours in sorted(total_map.items()):
            if total_hours > 0:
                spent_sec = spent_map.get((date, login), 0)
                value = self._round_half_up((spent_sec / 3600.0) / total_hours * 100.0)
                if value > 0:
                    out.append({"date": date, "login": login, "value": value})

        return out

    def fetch_telesales_upsales_stats(self) -> List[Dict[str, Any]]:
        log = get_dagster_logger()
        sql = self._read_sql("fetch_telesales_upsales_stats.sql")
        log.info(f"sql: {sql}")
        params = (self.start_date, self.end_date)
        rows = self._fetch_all(sql, params)
        return rows

    def get_ivr_9999_by_hour(self, projectName: str) -> List[Dict[str, Any]]:
        metricMap = {
            "Линия 707": "COUNT(DISTINCT session_id) FILTER (WHERE has_5656)    AS value",
            "Saima": "COUNT(DISTINCT session_id) FILTER (WHERE has_8844)    AS value",
            "О!Банк": "COUNT(DISTINCT session_id) FILTER (WHERE has_9999942) AS value",
            "Акча-Булак": "COUNT(DISTINCT session_id) FILTER (WHERE has_9999943) AS value",
        }

        metricSql = metricMap.get(projectName)
        if not metricSql:
            return []

        sql = f"""
        WITH ivr_9999_sessions AS (
            SELECT
                cl.session_id,
                MIN(cl.created) AS ivr_entered_at
            FROM call_legs cl
            WHERE cl.dst_id   = '9999'
              AND cl.incoming = 1
              AND cl.created >= %(start_date)s
              AND cl.created <  %(end_date)s
            GROUP BY cl.session_id
        ),
        routed_to_5656 AS (
            SELECT DISTINCT cd.session_id
            FROM call_dtmf cd
            WHERE cd.destination_id = '5656'
              AND cd.changed >= %(start_date)s
              AND cd.changed <  %(end_date)s
        ),
        routed_to_8844 AS (
            SELECT DISTINCT cd.session_id
            FROM call_dtmf cd
            WHERE cd.destination_id = '8844'
              AND cd.changed >= %(start_date)s
              AND cd.changed <  %(end_date)s
        ),
        routed_to_9999942 AS (
            SELECT DISTINCT cd.session_id
            FROM call_dtmf cd
            WHERE cd.destination_id = '9999942'
              AND cd.changed >= %(start_date)s
              AND cd.changed <  %(end_date)s
        ),
        routed_to_9999943 AS (
            SELECT DISTINCT cd.session_id
            FROM call_dtmf cd
            WHERE cd.destination_id = '9999943'
              AND cd.changed >= %(start_date)s
              AND cd.changed <  %(end_date)s
        ),
        hourly AS (
            SELECT
                -- если нужно локальное время: date_trunc('hour', ivr.ivr_entered_at + interval '6 hours')
                date_trunc('hour', ivr.ivr_entered_at) AS hour_bucket,
                ivr.session_id,
                (r5656.session_id    IS NOT NULL) AS has_5656,
                (r8844.session_id    IS NOT NULL) AS has_8844,
                (r9942.session_id    IS NOT NULL) AS has_9999942,
                (r9943.session_id    IS NOT NULL) AS has_9999943
            FROM ivr_9999_sessions ivr
            LEFT JOIN routed_to_5656    r5656 ON r5656.session_id = ivr.session_id
            LEFT JOIN routed_to_8844    r8844 ON r8844.session_id = ivr.session_id
            LEFT JOIN routed_to_9999942 r9942 ON r9942.session_id = ivr.session_id
            LEFT JOIN routed_to_9999943 r9943 ON r9943.session_id = ivr.session_id
        )
        SELECT
            to_char(hour_bucket, 'HH24:MI:SS') AS hour,
            to_char(hour_bucket, 'DD.MM.YYYY') AS date,
            {metricSql}
        FROM hourly
        GROUP BY hour_bucket
        ORDER BY hour_bucket
        """

        params = {"start_date": self.start_date, "end_date": self.end_date}
        rows = self._fetch_all(sql, params) or []

        result: List[Dict[str, Any]] = []
        for r in rows:
            result.append(
                {
                    "date": r.get("date"),
                    "hour": r.get("hour"),
                    "value": int(r.get("value") or 0),
                }
            )
        return result

    def get_ivr_9999_by_day(self, projectName: str) -> List[Dict[str, Any]]:
        projectAliases = {
            "ОСНОВНАЯ ЛИНИЯ - 707": "General",
            "General": "General",
            "САЙМА - 0706 909 000": "Saima",
            "Saima": "Saima",
            "О! ДЕНЬГИ + O! BANK - 999, 9999, 8008, 0700000999": "O!Bank",
            "O!Bank": "O!Bank",
            "АКЧАБУЛАК": "AkchaBulak",
            "AkchaBulak": "AkchaBulak",
        }

        normalizedProject = projectAliases.get(projectName)
        if not normalizedProject:
            return []

        metricMap = {
            "General": "COUNT(DISTINCT session_id) FILTER (WHERE has_5656)    AS value",
            "Saima": "COUNT(DISTINCT session_id) FILTER (WHERE has_8844)    AS value",
            "O!Bank": "COUNT(DISTINCT session_id) FILTER (WHERE has_9999942) AS value",
            "AkchaBulak": "COUNT(DISTINCT session_id) FILTER (WHERE has_9999943) AS value",
        }
        metricSql = metricMap[normalizedProject]

        sql = f"""
        WITH ivr_9999_sessions AS (
            SELECT
                cl.session_id,
                MIN(cl.created) AS ivr_entered_at
            FROM call_legs cl
            WHERE cl.dst_id   = '9999'
              AND cl.incoming = 1
              AND cl.created >= %(start_date)s
              AND cl.created <  %(end_date)s
            GROUP BY cl.session_id
        ),
        routed_to_5656 AS (
            SELECT DISTINCT cd.session_id
            FROM call_dtmf cd
            WHERE cd.destination_id = '5656'
              AND cd.changed >= %(start_date)s
              AND cd.changed <  %(end_date)s
        ),
        routed_to_8844 AS (
            SELECT DISTINCT cd.session_id
            FROM call_dtmf cd
            WHERE cd.destination_id = '8844'
              AND cd.changed >= %(start_date)s
              AND cd.changed <  %(end_date)s
        ),
        routed_to_9999942 AS (
            SELECT DISTINCT cd.session_id
            FROM call_dtmf cd
            WHERE cd.destination_id = '9999942'
              AND cd.changed >= %(start_date)s
              AND cd.changed <  %(end_date)s
        ),
        routed_to_9999943 AS (
            SELECT DISTINCT cd.session_id
            FROM call_dtmf cd
            WHERE cd.destination_id = '9999943'
              AND cd.changed >= %(start_date)s
              AND cd.changed <  %(end_date)s
        ),
        hourly AS (
            SELECT
                date_trunc('hour', ivr.ivr_entered_at) AS hour_bucket,
                ivr.session_id,
                (r5656.session_id    IS NOT NULL) AS has_5656,
                (r8844.session_id    IS NOT NULL) AS has_8844,
                (r9942.session_id    IS NOT NULL) AS has_9999942,
                (r9943.session_id    IS NOT NULL) AS has_9999943
            FROM ivr_9999_sessions ivr
            LEFT JOIN routed_to_5656    r5656 ON r5656.session_id = ivr.session_id
            LEFT JOIN routed_to_8844    r8844 ON r8844.session_id = ivr.session_id
            LEFT JOIN routed_to_9999942 r9942 ON r9942.session_id = ivr.session_id
            LEFT JOIN routed_to_9999943 r9943 ON r9943.session_id = ivr.session_id
        )
        SELECT
            to_char(date_trunc('day', hour_bucket), 'DD.MM.YYYY') AS date,
            {metricSql}
        FROM hourly
        GROUP BY date_trunc('day', hour_bucket)
        ORDER BY date_trunc('day', hour_bucket)
        """

        params = {"start_date": self.start_date, "end_date": self.end_date}
        rows = self._fetch_all(sql, params) or []

        result: List[Dict[str, Any]] = []
        for r in rows:
            result.append(
                {
                    "date": r.get("date"),
                    "ivr_from_9999": int(r.get("value") or 0),
                }
            )
        return result