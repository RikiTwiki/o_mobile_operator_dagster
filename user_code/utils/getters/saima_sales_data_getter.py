from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence

from resources import source_dwh_resource


class SaimaSalesDataGetter:
    """
    Python-порт PHP-класса `SaimaSalesDataGetter`.

    Ожидается DB-API 2.0 совместимый коннект (например, psycopg2) в `bpm/omni` БД.

    Публичные поля:
      - StartDate, EndDate (строки даты/времени: левая граница включительно, правая — исключительная)

    Методы (snake_case) + алиасы с именами как в PHP:
      - cancellations_title / CancellationsTitle
      - cancellation_outcoming_calls / CancellationOutComingCalls
      - get_cancellation_data / getCancellationData
      - get_cancellations_by_project_id / getCancellationsByProjectId
      - get_reports / getReports
      - get_manual_statistics / getManualStatistics
      - get_jivo_dialogues / getJivoDialogues
      - incoming_total_amount_calls / IncomingTotalAmountCalls
      - get_jivo_dialogues_by_widged_id / getJivoDialoguesByWidgedId
      - get_jivo_dialogues_topics / getJivoDialoguesTopics
    """

    StartDate: str
    EndDate: str

    # фиксированный список ID тем отмены (как в PHP)
    _cancellation_topics: Sequence[int] = (
        59,  # Вне зоны охвата
        167, # Не интересует предложение
        229, # Не устраивает цена
        230, # Не устраивают условия
        225, # Нет ответа/Отключен
        301, # Подключение по партнерам
        310, # Подумает
        316, # Пользователь Saima Telecom
        317, # Пользуется услугами другого провайдера
        318, # Пользуется услугами НУР Телекома
        321, # Попросил перезвонить/ занят
        337, # Просит исключить из обзвона/рассылки
    )

    def __init__(self, start_date: str, end_date: str, conn = source_dwh_resource) -> None:
        self.conn = conn
        self.StartDate = start_date
        self.EndDate = end_date

    # --------------------
    # helpers
    # --------------------

    def _fetch_all(self, sql: str, params: Sequence[Any]) -> List[Dict[str, Any]]:
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    def _fetch_vals(self, sql: str, params: Sequence[Any]) -> List[Any]:
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            return [r[0] for r in cur.fetchall()]

    # --------------------
    # ports
    # --------------------

    def cancellations_title(self) -> List[str]:
        sql = "\n".join([
            "SELECT title",
            "FROM bpm.topics",
            "WHERE id = ANY(%s)",
            "ORDER BY title",
        ])
        return self._fetch_vals(sql, [list(self._cancellation_topics)])

    # alias (PHP-style)
    def CancellationsTitle(self) -> List[str]:
        return self.cancellations_title()

    def cancellation_outcoming_calls(self) -> List[Dict[str, Any]]:
        sql = "\n".join([
            "SELECT",
            "  date_trunc('day', created_at) AS date,",
            "  topic_title AS title,",
            "  COUNT(session_id) AS value",
            "FROM bpm.topic_selects",
            "WHERE created_at >= %s",
            "  AND created_at < %s",
            "  AND topic_view_id = 2",
            "  AND topic_id = ANY(%s)",
            "GROUP BY 1,2",
            "ORDER BY 1",
        ])
        return self._fetch_all(sql, [self.StartDate, self.EndDate, list(self._cancellation_topics)])

    # alias
    def CancellationOutComingCalls(self) -> List[Dict[str, Any]]:
        return self.cancellation_outcoming_calls()

    def get_cancellation_data(self) -> List[Dict[str, Any]]:
        sql = "\n".join([
            "SELECT",
            "  date_trunc('day', ts.created_at) AS date,",
            "  oisr.project_id AS project_id,",
            "  t.title AS title,",
            "  COUNT(t.id) AS total",
            "FROM bpm.topic_selects AS ts",
            "LEFT JOIN bpm.topics AS t ON t.id = ts.topic_id",
            "LEFT JOIN omni_inbound_subscribers_requests AS oisr",
            "  ON ts.session_id = oisr.session_id AND oisr.direction = 'inbound'",
            "WHERE ts.active = TRUE",
            "  AND t.auxiliary_group = 'Результат'",
            "  AND t.general_group = 'Потенциальный абонент'",
            "  AND ts.created_at >= %s",
            "  AND ts.created_at < %s",
            "  AND oisr.project_id IS NOT NULL",
            "GROUP BY 1,2,3",
            "ORDER BY 1",
        ])
        return self._fetch_all(sql, [self.StartDate, self.EndDate])

    # alias
    def getCancellationData(self) -> List[Dict[str, Any]]:
        return self.get_cancellation_data()

    @staticmethod
    def get_cancellations_by_project_id(data: Sequence[Dict[str, Any]], project_id: Any) -> List[Dict[str, Any]]:
        resp: List[Dict[str, Any]] = []
        for item in data:
            if item.get("project_id") == project_id:
                resp.append({
                    "date": item.get("date"),
                    "title": item.get("title"),
                    "value": item.get("total"),
                })
        return resp

    # alias
    def getCancellationsByProjectId(self, data: Sequence[Dict[str, Any]], project_id: Any) -> List[Dict[str, Any]]:
        return self.get_cancellations_by_project_id(data, project_id)

    @staticmethod
    def get_reports() -> List[Dict[str, str]]:
        return [
            {"title": "Исходящий обзвон (ГТМ)", "id": "10802"},
            {"title": "SMS рассылка (ГТМ)", "id": "10807"},
            {"title": "IVR обзвон (ГТМ)", "id": "10806"},
            {"title": "Анкета на подключение - Сайт saimatelecom.kg (ГТМ)", "id": "10803"},
            {"title": "Анкета на подключение - Сайт ohvat.saimatelecom.kg (ГАП Сайма)", "id": "13083"},
            {"title": "Анкета на подключение - Мой О! (ГТМ)", "id": "10801"},
            {"title": "Входящая линия (ГОО)", "id": "10804"},
            {"title": "WhatsApp (ГПО)", "id": "13084"},
            {"title": "Telegram (ГПО)", "id": "13085"},
            {"title": "Чат на сайте - Сайты saimatelecom.kg, saima4g.kg, stat.saimatelecom.kg (ГПО)", "id": "13086"},
            {"title": "Instagram (ГПО)", "id": "13087"},
            {"title": "Facebook (ГПО)", "id": "13088"},
            {"title": "Обход жильцов (ГАП Сайма)", "id": "10822"},
            {"title": "Обход жильцов (ГАП Нур)", "id": "10810"},
            {"title": "Обращение в офис (ОПИО Сайма)", "id": "10823"},
            {"title": "Обращение в офис (ОПИО Нур)", "id": "10808"},
            {"title": "Обращение в офис (ОКП Сайма)", "id": "10809"},
            {"title": "Входящая линия [ВД 2.0] (ГТМ)", "id": "10827"},
            {"title": "Исходящий обзвон [ВД 2.0] (ГТМ)", "id": "10828"},
        ]

    # alias
    def getReports(self) -> List[Dict[str, str]]:
        return self.get_reports()

    def get_manual_statistics(self, type_id: int) -> List[Dict[str, Any]]:
        sql = "\n".join([
            "SELECT date, value",
            "FROM bpm.manual_statistics",
            "WHERE date >= %s",
            "  AND date < %s",
            "  AND type_id = %s",
            "GROUP BY 1,2",
            "ORDER BY 1",
        ])
        return self._fetch_all(sql, [self.StartDate, self.EndDate, type_id])

    # alias
    def getManualStatistics(self, type_id: int) -> List[Dict[str, Any]]:
        return self.get_manual_statistics(type_id)

    def get_jivo_dialogues(self) -> List[Dict[str, Any]]:
        """Агрегируем сразу на SQL: число уникальных сессий (по логике PHP группировки) на дату/проект."""
        sql = "\n".join([
            "SELECT",
            "  date_trunc('day', ts.created_at) AS date,",
            "  oisr.project_id AS project,",
            "  COUNT(*) AS contact",
            "FROM bpm.topic_selects AS ts",
            "LEFT JOIN bpm.topics AS topics ON topics.id = ts.topic_id",
            "LEFT JOIN omni_inbound_subscribers_requests AS oisr",
            "  ON ts.session_id = oisr.session_id AND oisr.direction = 'inbound'",
            "WHERE ts.created_at >= %s",
            "  AND ts.created_at < %s",
            "  AND ts.active = TRUE",
            "  AND topics.auxiliary_group = ANY(%s)",
            "GROUP BY 1,2",
            "ORDER BY 1",
        ])
        aux_groups = ["Интерес к тарифу", "Новое подключение"]
        return self._fetch_all(sql, [self.StartDate, self.EndDate, aux_groups])

    # alias
    def getJivoDialogues(self) -> List[Dict[str, Any]]:
        return self.get_jivo_dialogues()

    def incoming_total_amount_calls(self, naumen_getter: Optional[Any] = None) -> Any:
        """
        Порт метода IncomingTotalAmountCalls().
        В PHP создаётся `NaumenServiceLevelDataGetter` и вызываются методы:
          - setDaysTrunkedData()
          - filterProjectsByGroupArray(["Saima"])  # фильтр проектов по группе
          - getAggregatedData()
        В Python — ожидаем готовый объект/инстанс с той же API (или совместимой) и
        возвращаем его `getAggregatedData()`.
        """
        if naumen_getter is None:
            raise NotImplementedError(
                "Передайте инстанс naumen_getter с методами setDaysTrunkedData, "
                "filterProjectsByGroupArray и getAggregatedData"
            )
        naumen_getter.StartDate = self.StartDate
        naumen_getter.EndDate = self.EndDate
        naumen_getter.setDaysTrunkedData()
        naumen_getter.filterProjectsByGroupArray(["Saima"])  # как в PHP
        return naumen_getter.getAggregatedData()

    # alias
    def IncomingTotalAmountCalls(self, naumen_getter: Optional[Any] = None) -> Any:
        return self.incoming_total_amount_calls(naumen_getter)

    def get_jivo_dialogues_by_widged_id(self, widgets: Sequence[Any]) -> List[Dict[str, Any]]:
        sql = "\n".join([
            "SELECT",
            "  date_trunc('day', sjartd.chat_finished_at) AS date,",
            "  COUNT(sjartd.widget_id) AS value",
            "FROM aggregation_jivo_handling_time_data AS sjartd",
            "WHERE sjartd.agent_leg_number = 1",
            "  AND sjartd.type = 'agent'",
            "  AND sjartd.chat_finished_at >= %s",
            "  AND sjartd.chat_finished_at < %s",
            "  AND sjartd.widget_id = ANY(%s)",
            "GROUP BY 1",
            "ORDER BY 1",
        ])
        return self._fetch_all(sql, [self.StartDate, self.EndDate, list(widgets)])

    # alias (сохраняем оригинальную опечатку Widged)
    def getJivoDialoguesByWidgedId(self, widgets: Sequence[Any]) -> List[Dict[str, Any]]:
        return self.get_jivo_dialogues_by_widged_id(widgets)

    @staticmethod
    def get_jivo_dialogues_topics(data: Sequence[Dict[str, Any]], widgets: Sequence[Any] = ()) -> List[Dict[str, Any]]:
        """Агрегирует контакты по датам только для указанных widgets (как в PHP)."""
        acc: Dict[Any, int] = {}
        widgets_set = set(widgets)
        for item in data:
            if not widgets_set or item.get("project") in widgets_set:
                # date может прийти типом datetime/date/str — нормализуем к 'YYYY-MM-DD'
                d = item.get("date")
                if isinstance(d, (datetime, )):
                    key = d.strftime("%Y-%m-%d")
                else:
                    s = str(d)
                    key = s[:10]  # 'YYYY-MM-DD...'
                acc[key] = acc.get(key, 0) + int(item.get("contact") or 0)
        return [{"date": k, "value": v} for k, v in acc.items()]

    # alias
    def getJivoDialoguesTopics(self, data: Sequence[Dict[str, Any]], widgets: Sequence[Any] = ()) -> List[Dict[str, Any]]:
        return self.get_jivo_dialogues_topics(data, widgets)