from __future__ import annotations

from typing import Any, Dict, List, Sequence, Tuple

from resources import source_naumen_1_resource

import os

from utils.path import BASE_SQL_PATH

import math


class ConnectionDataService:
    """
    Python-порт PHP-класса `ConnectionDataService`.

    Ожидается DB-API 2.0-совместимый коннект к базе Naumen (`detail_outbound_sessions`, `mv_employee`).

    Публичные поля (совместимость с PHP):
      - StartDate: str  — левая граница периода (включительно)
      - EndDate:   str  — правая граница периода (включительно, как в BETWEEN)
      - Connection: str — имя подключения (по умолчанию 'naumen', носит информативный характер)
      - ProjectID: str  — идентификатор проекта для getCombinedNaumenData
    """

    StartDate: str
    EndDate: str
    Connection: str
    ProjectID: str

    def __init__(
        self,
        start_date: str,
        end_date: str,
        *,
        project_id: str = "corebo00000000000n9e9ma1906197mg",
        connection_name: str = "naumen",
        conn = source_naumen_1_resource,
    ) -> None:
        self.conn = conn
        self.StartDate = start_date
        self.EndDate = end_date
        self.Connection = connection_name
        self.ProjectID = project_id

        self.base_sql_path = BASE_SQL_PATH

    # -------------------- helpers --------------------

    def _read_sql(self, filename: str, conn: str = "None") -> str:

        if conn == "bpm":
            path = os.path.join(self.base_sql_path, "bpm", filename)
            with open(path, 'r', encoding="utf-8") as f:
                return f.read()

        path = os.path.join(self.base_sql_path, "naumen", filename)
        with open(path, 'r', encoding="utf-8") as f:
            return f.read()

    def _fetch_all(self, sql: str, params: Sequence[Any]) -> List[Dict[str, Any]]:
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]

    @staticmethod
    def _to_map_by_date(rows: Sequence[Dict[str, Any]], value_key: str = "value") -> Dict[str, int]:
        out: Dict[str, int] = {}
        for r in rows:
            out[str(r["date"])[:10]] = int(r.get(value_key) or 0)
        return out

    # -------------------- ports --------------------

    def get_combined_naumen_data(self) -> List[Dict[str, Any]]:
        """
        % дозвонов >5с от всех попыток для текущего ProjectID.
        Формула: round_half_up( connected_over_5 / (connected + not_connected) * 100 ).
        """
        params = {
            "start_date": self.StartDate,
            "end_date": self.EndDate,
            "project_id": self.ProjectID,
        }

        # 1) connected_over_5
        sql1 = self._read_sql("connected_over_5.sql")  # sql/naumen/...
        over5_rows = self._fetch_all(sql1, params) or []
        connected_over5_by_date: Dict[str, int] = {
            str(r["date"])[:10]: int(r.get("value") or 0) for r in over5_rows
        }

        # 2) connected / not connected
        sql2 = self._read_sql("connected_vs_not_connected.sql")  # sql/naumen/...
        rows2 = self._fetch_all(sql2, params) or []
        by_date_two: Dict[str, Dict[str, int]] = {}
        for r in rows2:
            d = str(r.get("date", ""))[:10]
            row = str(r.get("row") or "")
            val = int(r.get("value") or 0)
            bucket = by_date_two.setdefault(d, {"Соединено": 0, "Недозвон": 0})
            if row in bucket:
                bucket[row] = val

        # 3) объединение дат
        all_dates = sorted(set(connected_over5_by_date.keys()) | set(by_date_two.keys()))

        def _round_half_up(x: float) -> int:
            return int(math.floor(x + 0.5))

        # 4) расчёт
        result: List[Dict[str, Any]] = []
        for d in all_dates:
            connected_over5 = connected_over5_by_date.get(d, 0)
            connected = by_date_two.get(d, {}).get("Соединено", 0)
            not_connected = by_date_two.get(d, {}).get("Недозвон", 0)
            denom = connected + not_connected
            value = _round_half_up((connected_over5 / denom) * 100) if denom else 0
            result.append({"date": d, "value": value, "unit": "%"})

        return result

    def get_combined_naumen_data_all(self) -> List[Dict[str, Any]]:
        """
        То же, что get_combined_naumen_data, но исключая проекты:
          - corebo00000000000okarbb3di6m3o00
          - corebo00000000000paj0r6jhi5nta38
        Формула: round_half_up( connected_over_5 / (connected + not_connected) * 100 ).
        """
        excluded = [
            "corebo00000000000okarbb3di6m3o00",
            "corebo00000000000paj0r6jhi5nta38",
        ]
        params = {
            "start_date": self.StartDate,
            "end_date": self.EndDate,
            "excluded": excluded,
        }

        def _round_half_up(x: float) -> int:
            return int(math.floor(x + 0.5))

        # 1) connected_over_5 (без исключённых проектов)
        sql1 = self._read_sql("connected_over_5_excluding.sql")  # sql/naumen/...
        over5_rows = self._fetch_all(sql1, params) or []
        connected_over5_by_date: Dict[str, int] = {}
        for r in over5_rows:
            d = str(r.get("date", ""))[:10]
            connected_over5_by_date[d] = int(r.get("value") or 0)

        # 2) connected / not connected (без исключённых проектов)
        sql2 = self._read_sql("connected_vs_not_connected_excluding.sql")  # sql/naumen/...
        rows2 = self._fetch_all(sql2, params) or []
        by_date_two: Dict[str, Dict[str, int]] = {}
        for r in rows2:
            d = str(r.get("date", ""))[:10]
            row = str(r.get("row") or "")
            val = int(r.get("value") or 0)
            bucket = by_date_two.setdefault(d, {"Соединено": 0, "Недозвон": 0})
            if row in bucket:
                bucket[row] = val

        # 3) объединение и расчёт
        all_dates = sorted(set(connected_over5_by_date.keys()) | set(by_date_two.keys()))
        result: List[Dict[str, Any]] = []
        for d in all_dates:
            connected_over5 = connected_over5_by_date.get(d, 0)
            connected = by_date_two.get(d, {}).get("Соединено", 0)
            not_connected = by_date_two.get(d, {}).get("Недозвон", 0)
            denom = connected + not_connected
            value = _round_half_up((connected_over5 / denom) * 100) if denom else 0
            result.append({"date": d, "value": value, "unit": "%"})

        return result

    def get_connections(self) -> List[Dict[str, Any]]:
        sql = self._read_sql("get_connections.sql")
        params = {"start_date": self.StartDate, "end_date": self.EndDate}
        return self._fetch_all(sql, params) or []