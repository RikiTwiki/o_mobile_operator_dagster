from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

from resources import source_dwh_resource


class KnowledgeDataGetter:
    """
    Python-порт PHP-класса `KnowledgeDataGetter`.

    Ожидаются DB-API 2.0 совместимые коннекты:
      - `bpm_conn` — для `attestation_results` и выборки staff units
      - `kpi_conn` — для `kpi_results_data` (если None, используем bpm_conn)

    Параметры поведения вынесены в атрибуты: Positions (позиции) и ScriptID.
    Методы (snake_case) + PHP-алиасы:
      - get_knowledge_data_by_unit_id / getKnowledgeDataByUnitID
      - get_general_knowledge_data / getGeneralKnowledgeData
    """

    # публичные поля, как в PHP
    Positions: Sequence[int]
    ScriptID: int

    def __init__(
        self,
        bpm_conn = source_dwh_resource,
        *,
        positions: Optional[Sequence[int]] = None,
        script_id: int = 19,
        attestation_table: str = "attestation_results",
        kpi_table: str = "kpi_results_data",
        staff_units_table: str = "bpm.staff_units",
    ) -> None:
        self.bpm_conn = bpm_conn
        self.Positions = list(positions) if positions is not None else [10, 19, 35, 36, 37, 38, 39, 41]
        self.ScriptID = script_id
        self._attestation_table = attestation_table
        self._kpi_table = kpi_table
        self._staff_units_table = staff_units_table

    # -------------------- helpers --------------------

    def _fetch_one(self, conn: Any, sql: str, params: Sequence[Any]) -> Optional[Dict[str, Any]]:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            if not row:
                return None
            cols = [c[0] for c in cur.description]
            return dict(zip(cols, row))

    def _fetch_all(self, conn: Any, sql: str, params: Sequence[Any]) -> List[Dict[str, Any]]:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]

    # -------------------- ports --------------------

    def get_knowledge_data_by_unit_id(self, staff_unit_id: int, date_start: str, date_end: str) -> Dict[str, Any]:
        """Полный порт SQL-CTE из PHP-версии (параметризованный)."""
        sql = "\n".join([
            "WITH jsonb_extract AS (",
            f"  SELECT attempt, jsonb_array_elements(data) AS questions",
            f"  FROM {self._attestation_table}",
            f"  WHERE date >= %s AND date < %s AND staff_unit_id = %s",
            ")",
            ", detailed_answers AS (",
            "  SELECT",
            "    attempt,",
            "    questions ->> 'question_id'    AS question_id,",
            "    questions ->> 'question_title' AS question_title,",
            "    questions ->> 'topic_id'       AS topic_id,",
            "    questions ->> 'ticket_id'      AS ticket_id,",
            "    questions ->> 'result'         AS answer_result",
            "  FROM jsonb_extract",
            ")",
            ", topics_result AS (",
            "  SELECT",
            "    attempt,",
            "    topic_id,",
            "    SUM(CASE WHEN answer_result = 'false' THEN 1 ELSE 0 END)::numeric AS sum",
            "  FROM detailed_answers",
            "  GROUP BY 1,2",
            "  ORDER BY 1",
            ")",
            "SELECT",
            "  SUM(CASE WHEN sum > 0 THEN 1 ELSE 0 END)                           AS mistaken_topics,",
            "  100 - SUM(CASE WHEN sum > 0 THEN 1 ELSE 0 END)::numeric / 40 * 100 AS knowledge",
            "FROM topics_result",
        ])
        rec = self._fetch_one(self.bpm_conn, sql, [date_start, date_end, staff_unit_id])
        if not rec or rec.get("knowledge") is None:
            raise Exception(f"Отсутствует Knowledge у {staff_unit_id}")
        return {
            "unit_id": staff_unit_id,
            "knowledge": rec["knowledge"],
            "mistaken_topics": rec["mistaken_topics"],
        }

    # alias
    def getKnowledgeDataByUnitID(self, staff_unit_id: int, date_start: str, date_end: str) -> Dict[str, Any]:
        return self.get_knowledge_data_by_unit_id(staff_unit_id, date_start, date_end)

    def _get_active_staff_unit_ids(self, date_start: str) -> List[int]:
        """Эмуляция StaffUnit::activeUnits($dateStart)->whereIn(position_id, Positions).
        Схема может отличаться: используем распространённые поля `hired_at`/`fired_at` или `active`.
        При необходимости подправьте SQL под вашу модель StaffUnit.
        """
        # Пытаемся через наём/увольнение, иначе — по флагу active, иначе — только фильтр по position_id.
        # 1) Найм/увольнение
        sql1 = "\n".join([
            f"SELECT id FROM {self._staff_units_table}",
            "WHERE position_id = ANY(%s)",
            "  AND (COALESCE(hired_at, DATE '1900-01-01') <= %s)",
            "  AND (fired_at IS NULL OR fired_at > %s)",
        ])
        try:
            rows = self._fetch_all(self.bpm_conn, sql1, [list(self.Positions), date_start, date_start])
            if rows:
                return [int(r["id"]) for r in rows]
        except Exception:
            pass

        # 2) Флаг active
        sql2 = "\n".join([
            f"SELECT id FROM {self._staff_units_table}",
            "WHERE position_id = ANY(%s)",
            "  AND active = TRUE",
        ])
        try:
            rows = self._fetch_all(self.bpm_conn, sql2, [list(self.Positions)])
            if rows:
                return [int(r["id"]) for r in rows]
        except Exception:
            pass

        # 3) Фоллбек — только по position_id
        sql3 = f"SELECT id FROM {self._staff_units_table} WHERE position_id = ANY(%s)"
        rows = self._fetch_all(self.bpm_conn, sql3, [list(self.Positions)])
        return [int(r["id"]) for r in rows]

    def get_general_knowledge_data(self, date_start: str) -> Dict[str, Any]:
        staff_unit_ids = self._get_active_staff_unit_ids(date_start)
        if not staff_unit_ids:
            raise Exception("Отсутствует список активных StaffUnit для расчёта Knowledge")

        sql = "\n".join([
            "SELECT",
            "  ROUND(AVG(kpi_fact::numeric), 2) AS avg_knowledge,",
            "  SUM((data->>'mistaken_topics')::numeric) AS total_mistaken_topics",
            f"FROM {self._kpi_table}",
            "WHERE date = %s",
            "  AND script_id = %s",
            "  AND staff_unit_id = ANY(%s)",
        ])
        rec = self._fetch_one(self.bpm_conn, sql, [date_start, self.ScriptID, staff_unit_ids])
        if not rec or rec.get("avg_knowledge") is None:
            raise Exception("Отсутствует Knowledge")
        return {
            "avg_knowledge": float(rec["avg_knowledge"]),
            "total_mistaken_topics": int(rec["total_mistaken_topics"] or 0),
        }

    # alias
    def getGeneralKnowledgeData(self, date_start: str) -> Dict[str, Any]:
        return self.get_general_knowledge_data(date_start)