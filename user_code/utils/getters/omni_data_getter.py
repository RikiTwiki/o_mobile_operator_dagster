from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from dagster import get_dagster_logger


class OmniDataGetter:
    """
    Переписанный с PHP класс для подключения к БД 'omni' и выборок:
      - get_requests_data
      - get_omni_actual_technical_works_data
      - get_all_aggregated_requests
      - get_all_aggregated_requests_new

    Ожидается psycopg2-подобный conn (postgres), но подойдёт любой DB-API 2.0.
    """

    # Публичные поля для совместимости с PHP версией
    start_date: str
    end_date: str
    splits: List[int]
    project_ids: List[int]

    # Внутренние настройки агрегации
    trunc: str
    date_format: str

    def __init__(
        self,
        conn,
        start_date=None,
        end_date = None,
        project_ids: Optional[Sequence[int]] = None,
        splits: Optional[Sequence[int]] = None
    ) -> None:
        """
        :param omni_conn: psycopg2-совместимый connection к БД omni (DB-API 2.0)
        :param start_date: ISO-строка даты/времени (используется в WHERE)
        :param end_date:   ISO-строка даты/времени (используется в WHERE, правая граница исключительная)
        :param project_ids: список project_id для фильтрации
        :param splits: список split_id для фильтрации
        """
        self.conn = conn
        self.start_date = start_date
        self.end_date = end_date
        self.project_ids = list(project_ids) if project_ids else [1]
        self.splits = list(splits) if splits else []

        # По умолчанию — дневная агрегация, как в PHP setDaysTrunkedData()
        self.trunc = "day"
        self.date_format = "YYYY-MM-DD HH24:MI:SS"

    # ---------- Вспомогательные ----------

    def _fetch_all(self, sql: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Выполнить SQL и вернуть список словарей (колонка→значение)."""
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    @staticmethod
    def _safe_date_sub_one_day(date_str: str) -> str:
        """end_date - 1 день → 'YYYY-MM-DD'."""
        # Пытаемся разобрать разные форматы
        try:
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        except Exception:
            # Fallback — только дата
            dt = datetime.strptime(date_str[:10], "%Y-%m-%d")
        return (dt - timedelta(days=1)).strftime("%Y-%m-%d")

    # ---------- Порты методов из PHP ----------

    def get_requests_data(
            self,
            callback: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
    ) -> List[Dict[str, Any]]:
        """
        1:1 к PHP getRequestsData(): считаем COUNT(cr.request_id) с LEFT JOIN'ами.
        Фильтр по сплитам добавляется только если self.splits задан.
        """

        splits_filter = "AND ss.split_id = ANY(%(splits)s::int[])" if self.splits else ""

        sql = f"""
        SELECT
            to_char(date_trunc(%(trunc)s::text, cr.created_at), %(date_format)s) AS date,
            rr.id                                AS id,
            p.title                              AS project_title,
            r.title                              AS request_title,
            rr.title                             AS reason_title,
            rl.title                             AS request_label_title,
            rs.title                             AS request_status_title,
            COUNT(cr.request_id)                 AS count
        FROM cp.client_account_requests                AS cr
        LEFT JOIN cp.requests                           AS r   ON r.id  = cr.request_id
        LEFT JOIN cp.request_statuses                   AS rs  ON rs.id = cr.request_status_id
        LEFT JOIN cp.client_account_request_escalations AS cre ON cre.client_account_request_id = cr.id
        LEFT JOIN cp.session_stages                     AS ss  ON ss.id = cre.session_stage_id
        LEFT JOIN cp.request_reasons                    AS rr  ON rr.id = cr.request_reason_id
        LEFT JOIN cp.request_reason_labels              AS rrl ON rr.id = rrl.request_reason_id
        LEFT JOIN cp.request_labels                      AS rl  ON rl.id = rrl.request_label_id
        LEFT JOIN cp.request_views                       AS rv  ON rv.id = cr.request_view_id
        LEFT JOIN cp.projects                            AS p   ON p.id  = rv.project_id
        WHERE cr.created_at >= %(start_date)s
          AND cr.created_at  < %(end_date)s
          AND p.id = ANY(%(project_ids)s::int[])
          {splits_filter}
          AND rl.purpose = ANY(%(purposes)s)
        GROUP BY 1,2,3,4,5,6,7
        ORDER BY 1;
        """

        params: Dict[str, Any] = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "project_ids": self.project_ids,
            "purposes": ["Вспомогательная группа причины"],
        }
        if self.splits:
            params["splits"] = self.splits

        rows = self._fetch_all(sql, params)
        if callback:
            rows = [callback(r) for r in rows]
        return rows

    def get_omni_actual_technical_works_data(self) -> list[dict]:
        logger = get_dagster_logger()
        tz = "Asia/Bishkek"

        if isinstance(self.end_date, str):
            end_dt = datetime.fromisoformat(self.end_date)
        else:
            end_dt = self.end_date
        report_day = (end_dt - timedelta(days=1)).date().isoformat()

        # Convert start_date to date string if it's a datetime
        if isinstance(self.start_date, str):
            try:
                start_dt = datetime.fromisoformat(self.start_date)
                start_date_only = start_dt.date().isoformat()
            except:
                start_date_only = self.start_date
        else:
            start_date_only = self.start_date.date().isoformat() if hasattr(self.start_date, 'date') else str(self.start_date)

        logger.info(f"get_omni_actual_technical_works_data - start_date: {self.start_date}, start_date_only: {start_date_only}")
        logger.info(f"get_omni_actual_technical_works_data - end_date: {self.end_date}, report_day: {report_day}")
        logger.info(f"get_omni_actual_technical_works_data - project_ids: {getattr(self, 'project_ids', None)}")

        project_filter = "AND ss.project_id = ANY(%(project_ids)s)" if getattr(self, "project_ids", None) else ""

        sql = f"""
        SELECT
            tws.title AS service,
            tw.description AS description,
            SUM(
              CASE
                WHEN DATE(COALESCE(ss.unblocked_at, twi.created_at)) = %(report_day)s
                THEN 1 ELSE 0
              END
            ) AS technical_work_count,
            CASE
              WHEN tw.finished_at IS NULL
                THEN TO_CHAR(timezone(%(tz)s, now()) - tw.started_at, 'DDд. HH24ч. MIм.')
              ELSE TO_CHAR(tw.finished_at - tw.started_at, 'DDд. HH24ч. MIм.')
            END AS downtime,
            tw.started_at AS date_begin,
            tw.finished_at AS date_end
        FROM cp.technical_works AS tw
        LEFT JOIN cp.technical_work_impacts AS twi
          ON tw.id = twi.technical_work_id
        LEFT JOIN cp.session_stages AS ss
          ON ss.id = twi.session_stage_id
        LEFT JOIN cp.technical_work_services AS tws
          ON tw.service_id = tws.id
        WHERE
          (
            DATE(tw.started_at) = %(start_date)s::date
            OR (
                 tw.finished_at IS NULL
             AND tw.started_at >= (timezone(%(tz)s, now())::date - INTERVAL '31 days')
             AND tw.started_at < %(end_date)s
               )
            OR DATE(tw.finished_at) = %(start_date)s::date
          )
          {project_filter}
        GROUP BY 1,2,5,6
        ORDER BY tw.started_at;
        """

        params = {
            "tz": tz,
            "report_day": report_day,
            "start_date": start_date_only,
            "end_date": self.end_date,
        }
        if getattr(self, "project_ids", None):
            params["project_ids"] = self.project_ids

        logger.info(f"get_omni_actual_technical_works_data - SQL params: {params}")
        
        result = self._fetch_all(sql, params)
        logger.info(f"get_omni_actual_technical_works_data - result count: {len(result)}")
        logger.info(f"get_omni_actual_technical_works_data - result: {result}")
        
        return result

    def get_all_aggregated_requests(self, unset_date: bool = False) -> List[Dict[str, Any]]:
        """
        Переписанная версия PHP getAllAggregatedRequests($unsetDate = false) для client_account_*.

        Метрики:
          - created_other_jira_issue : COUNT(cart.external_id) при status_id = 4 и care.attempt = 1
          - linked_jira_issue        : COUNT(cart.external_id) при status_id = 4 и care.attempt != 1 (и не NULL)
          - closed_by_first_line     : SUM(1) при status_id IN (1,5,6)
          - closed_by_second_line    : COUNT(cart.external_id) при status_id = 7
          - total                    : COUNT(cart.external_id) при status_id = 4
                                       + SUM(1) при status_id IN (1,5,6)
                                       + COUNT(cart.external_id) при status_id = 7
                                       (эквивалентно сумме четырёх метрик выше)
        """
        select_date = "DATE_TRUNC('day', car.created_at) AS date," if not unset_date else ""
        group_order = "GROUP BY 1 ORDER BY 1" if not unset_date else ""

        sql = f"""
        SELECT
            {select_date}
            COUNT(
              CASE
                WHEN carsc.request_status_id IN (4) AND care.attempt = 1
                THEN cart.external_id
              END
            ) AS created_other_jira_issue,
            COUNT(
              CASE
                WHEN carsc.request_status_id IN (4) AND (care.attempt IS NOT NULL AND care.attempt <> 1)
                THEN cart.external_id
              END
            ) AS linked_jira_issue,
            SUM(
              CASE WHEN carsc.request_status_id IN (1,5,6) THEN 1 ELSE 0 END
            ) AS closed_by_first_line,
            COUNT(
              CASE WHEN carsc.request_status_id IN (7) THEN cart.external_id END
            ) AS closed_by_second_line,
            (
                COUNT(CASE WHEN carsc.request_status_id IN (4) THEN cart.external_id END)
              + SUM(CASE WHEN carsc.request_status_id IN (1,5,6) THEN 1 ELSE 0 END)
              + COUNT(CASE WHEN carsc.request_status_id IN (7) THEN cart.external_id END)
            ) AS total
        FROM cp.client_account_requests AS car
        LEFT JOIN cp.client_account_request_status_changes AS carsc
          ON carsc.client_account_request_id = car.id
         AND carsc.request_reason_id IS NOT NULL
         AND carsc.id = (
                SELECT MIN(id)
                FROM cp.client_account_request_status_changes
                WHERE client_account_request_id = car.id
                  AND request_reason_id IS NOT NULL
            )
        LEFT JOIN cp.client_account_request_escalations AS care
          ON car.id = care.client_account_request_id
        LEFT JOIN cp.client_account_request_tickets AS cart
          ON car.id = cart.client_account_request_id
        LEFT JOIN cp.request_views AS rv
          ON car.request_view_id = rv.id
        WHERE rv.project_id = ANY(%(project_ids)s)
          AND car.created_at BETWEEN %(start_date)s AND %(end_date)s
        {group_order};
        """

        params = {
            "project_ids": self.project_ids,
            "start_date": self.start_date,
            "end_date": self.end_date,
        }
        return self._fetch_all(sql, params)

    def get_all_aggregated_requests_new(self, unset_date: bool = False) -> List[Dict[str, Any]]:
        """
        Аналог PHP getAllAggregatedRequestsNew($unsetDate = false).
        Подсчитывает агрегированную статистику по заявкам клиентов.
        """
        select_date = "DATE_TRUNC('day', car.created_at) AS date," if not unset_date else ""
        group_order = "GROUP BY 1 ORDER BY 1" if not unset_date else ""

        sql = f"""
        SELECT
            {select_date}
            COUNT(DISTINCT 
              CASE WHEN carsc.request_status_id = 4
                   AND COALESCE(cart.is_cancelled, false) = false
              THEN cart.id END
            ) AS created_other_jira_issue,
            SUM(
              CASE WHEN carsc.request_status_id IN (1,5,6)
              THEN 1 ELSE 0 END
            ) AS closed_by_first_line,
            COUNT(DISTINCT 
              CASE WHEN carsc.request_status_id = 7
                   AND COALESCE(cart.is_cancelled, false) = false
              THEN cart.id END
            ) AS closed_by_second_line,
            (
              COUNT(
                CASE WHEN carsc.request_status_id = 4
                THEN cart.external_id END
              )
              + SUM(
                  CASE WHEN carsc.request_status_id IN (1,5,6)
                  THEN 1 ELSE 0 END
                )
              + COUNT(
                  CASE WHEN carsc.request_status_id = 7
                  THEN cart.external_id END
                )
            ) AS total
        FROM cp.client_account_requests AS car
        LEFT JOIN cp.client_account_request_status_changes AS carsc
          ON carsc.client_account_request_id = car.id
         AND carsc.request_reason_id IS NOT NULL
         AND carsc.id = (
              SELECT MIN(id)
              FROM cp.client_account_request_status_changes
              WHERE client_account_request_id = car.id
                AND request_reason_id IS NOT NULL
         )
        LEFT JOIN cp.client_account_request_escalations AS care
          ON care.client_account_request_id = car.id
        LEFT JOIN cp.client_account_request_tickets AS cart
          ON cart.client_account_request_id = car.id
        LEFT JOIN cp.request_views AS rv
          ON rv.id = car.request_view_id
        WHERE rv.project_id = ANY(%(project_ids)s)
          AND car.created_at BETWEEN %(start_date)s AND %(end_date)s
        {group_order};
        """

        params = {
            "project_ids": self.project_ids,
            "start_date": self.start_date,
            "end_date": self.end_date,
        }
        return self._fetch_all(sql, params)

    # ---------- Настройка "trunc" и формата дат (совместимость с PHP) ----------

    def set_hours_trunked_data(self) -> None:
        self.trunc = "hour"
        self.date_format = "YYYY-MM-DD HH24:MI:SS"

    def set_days_trunked_data(self) -> None:
        self.trunc = "day"
        self.date_format = "YYYY-MM-DD HH24:MI:SS"

    def set_months_trunked_data(self) -> None:
        self.trunc = "month"
        self.date_format = "YYYY-MM-DD"

