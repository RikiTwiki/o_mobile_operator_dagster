import re
from pathlib import Path
from typing import List, Dict, Optional, Any

import pandas as pd
import psycopg2
from dagster import get_dagster_logger
from psycopg2.extras import RealDictCursor

from utils.path import BASE_SQL_PATH

from datetime import datetime

class MessagingPlatformDataGetter:
    def __init__(
        self,
        conn: psycopg2.extensions.connection,
        trunc: str = "day",
        date_format: Optional[str] = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        report_date: datetime | None = None,
        project_ids: List[int] = [1],

        pst_positions: List[int] = [10, 34, 41, 46],

        timezone_adjustment_date: str = "Asia/Bishkek",
        date_field: str = "date",
    ):

        self.trunc = trunc

        self.start_date = start_date
        self.end_date = end_date
        self.report_date=report_date
        self.conn = conn

        self.base_sql_path = Path(BASE_SQL_PATH)

        # DateFormat — можно задать при конструировании
        if date_format is None:
            self.date_format = "YYYY-MM-DD"
        else:
            self.date_format = date_format

        # Аналог public $DateField = 'date';
        self.date_field = date_field

        self.timezone_adjustment_date: str = timezone_adjustment_date

        # ProjectIds и PstPositions
        self.project_ids = project_ids
        self.pst_positions = pst_positions

        # ExcludedDays
        self.excluded_days = []

        # Поля для объединения
        self.fields_to_merge: Dict[str, str] = {
            'O! - WhatsApp': 'О! - WhatsApp',
            'O! - Telegram': 'О! - Telegram',
            'O! Money - WhatsApp': 'О!Деньги - WhatsApp',
            'О! - o.kg': 'О! - Сайт | o.kg',
            'O! - SMS | 705': 'О! - SMS | 705',
            'O! Money - Telegram': 'О!Деньги - Telegram',
            'O! Store - ostore.kg': 'ИМ - Сайт | ostore.kg',
            'Akcha Bulak - WhatsApp': 'МКК Акча Булак - WhatsApp',
            'Akcha Bulak - Telegram': 'МКК Акча Булак - Telegram',
            'О! - SMS | 708': 'O! - SMS | 708',
            'O! Store - Telegram': 'ИМ - Telegram',
            'Saima - saimatelecom.kg': 'Saima - Сайт | saimatelecom.kg',
            'Saima - stat.saimatelecom.kg': 'Saima - Сайт | stat.saimatelecom.kg',
            'Saima - wiki.saima.kg': 'Saima - Сайт | wiki.saima.kg',
        }
    logger = get_dagster_logger()
    def get_excluded_chats(self) -> List[int]:
        # 1) Читаем SQL
        sql_path = self.base_sql_path / "mp" / "get_excluded_chats.sql"
        query = sql_path.read_text(encoding="utf-8")

        # 2) Выполняем
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Если в SQL вы используете синтаксис:
            #   WHERE amphtd.project_id = ANY(%(project_ids)s)
            params = {
                "start_date": self.start_date,
                "end_date":   self.end_date,
                "project_ids": self.project_ids,
            }

            if hasattr(self, 'context'):
                sql_debug = cur.mogrify(query, params)
                self.context.log.info(f"[get_excluded_chats] SQL → {sql_debug}")

                # Логируем сами параметры
            if hasattr(self, 'context'):
                self.context.log.info(f"[get_excluded_chats] params → {params}")

            cur.execute(query, params)
            rows = cur.fetchall()

            # Логируем, сколько строк вернулось
            if hasattr(self, 'context'):
                self.context.log.info(f"[get_excluded_chats] fetched rows count → {len(rows)}")
                self.context.log.debug(f"[get_excluded_chats] rows detail → {rows}")

        # 3) Возвращаем только chat_id
        chat_ids = [row["chat_id"] for row in rows]
        if hasattr(self, 'context'):
            self.context.log.info(f"[get_excluded_chats] returning chat_ids → {chat_ids}")

        return chat_ids

    def get_aggregated(self) -> List[Dict[str, Any]]:
        sql_path = self.base_sql_path / "mp" / "get_aggregated.sql"

        sql_template = sql_path.read_text(encoding="utf-8")

        query = sql_template.format(
            trunc=self.trunc,
            date_format=self.date_format,
            date_field=self.date_field,
        )

        base_excluded = list(self.get_excluded_chats())

        extra_excluded = [
            1481690, 1490498, 4363973, 4986924, 4987932, 4988154, 4985686, 4985878,
            4988418, 4998057, 4998069, 4998129, 4998138, 4998238, 4998250, 4998260,
            4998263, 4991371, 4995622, 4998325, 4998340, 4998342, 4998344, 4998347,
            4998359, 4998366, 4998372, 4998375, 4998454, 4998461, 5003043, 5003059,
            5004141, 5007373, 5008001, 5012748, 5012748, 5013181, 5013589, 5014229,
            5016528, 5016562, 5017239, 5025168, 5026458, 5023588, 5023588, 5025455,
            5025736, 5026339, 5026473, 5026502, 5026505, 5026650, 5026740, 5027112,
            5027166, 5027260, 5027288, 5027423, 5027457, 5027457, 5027719, 5027815,
            5028154, 5028463, 5028518, 5028607, 5028608, 5028723, 5028778, 5029034,
            5029198, 5028702, 5028702, 5035944, 5037750, 5038046, 5039089, 5039185,
            5039697, 5040042, 5040752, 5041535, 5042346, 5042380, 5043886, 5044448,
            5045554, 5046296, 5046322, 5046397, 5046681, 5046978, 5047091, 5047336,
            5047439, 5048875, 5049055, 5049275, 5050489, 5050678, 5050844, 5050919,
            5051115, 5051136, 5051313, 5051356, 5051374, 5051569, 5051598, 5051609,
            5051657, 5051875, 5051897, 5051906, 5051983, 5052592, 5052861, 5052898,
            5045781, 5046743, 5047376, 5049271, 5049320, 5049365, 5050067, 5050149,
            5050946, 5051239, 5051330, 5051901, 5052180, 5052467, 5053532, 5053603,
            5053758, 5053762, 5053768, 5053807, 5053951, 5054038, 5054059, 5054080,
            5054117, 5054180, 5054215, 5054321, 5054321, 5054379, 5054383, 5054457,
            5054506, 5054771, 5054799, 5054893, 5055008, 5055044, 5055083, 5055115,
            5055125, 5055275, 5055290, 5055319, 5055394, 5055425, 5055567, 5055649,
            5055649, 5055682, 5055693, 5055884, 5055894, 5055921, 5055926, 5055928,
            5055996, 5056136, 5058502, 5058527, 5060381, 5060412, 5037889, 5037889,
            5071026, 5072355, 5072393, 5072761, 5074321, 5074922, 5075360, 5075852,
            5081868, 5082138, 5082312, 5082410, 5083670, 5084635, 5084666, 5084718,
            5084753, 5085241, 5086738, 5042093, 5042798, 5085274, 5092296, 5091199,
            5091228, 5094641, 5095043, 5095171, 5095186, 5096922, 5097115, 5097345,
            5097482, 5097574, 5097876, 5097896, 5096225, 5100065, 5104452, 5106116,
            5106209, 5063280, 5118007, 5118198, 5118301, 5118398, 5127025, 5127458,
            5163021, 5163207, 5165637, 5175391, 5175473, 5184538, 5185510, 5183180,
            5188581, 5190139, 5201660, 5201833, 5201857, 5096225, 5100065, 5104452,
            5106116, 5106209, 5063280, 5118007, 5118198, 5118301, 5118398, 5127458,
            5183180, 5188581, 5190139, 5201660, 5201833, 5201857,
        ]

        merged = list(base_excluded) + list(extra_excluded)

        seen = set()
        excluded_chats: List[int] = []
        for cid in merged:
            try:
                cid_int = int(cid)
            except (TypeError, ValueError):
                continue
            if cid_int not in seen:
                seen.add(cid_int)
                excluded_chats.append(cid_int)

        params = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "project_ids": self.project_ids,
            "pst_positions": self.pst_positions,
            "excluded_chats": excluded_chats,
        }

        if hasattr(self, 'context'):
            self.context.log.info(f"[get_aggregated] params → {params}")

        df = pd.read_sql_query(query, self.conn, params=params)

        return df.to_dict(orient="records")

    def get_aggregated_data_detailed(self, exclude_dates = None, exclude_splits: bool=False):
        excluded_chats = self.get_excluded_chats() or []

        sql_path = self.base_sql_path / "stat" / "aggregated_data_detailed.sql"
        sql_template = sql_path.read_text(encoding="utf-8")

        excluded_chats_filter = ""
        if excluded_chats:
            excluded_chats_filter = "AND NOT (amphtd.chat_id = ANY(%(excluded_chats)s::bigint[]))"

        exclude_dates_filter = ""
        if exclude_dates:
            exclude_dates_filter = "AND TO_CHAR(amphtd.chat_finished_at, 'YYYY-MM-DD') <> ALL(%(exclude_dates)s::text[])"

        exclude_splits_filter = ""
        if exclude_splits:
            exclude_splits_filter = """AND amphtd.split_title NOT IN (
                'O! - SMS | 705700700',
                'Saima - Saima direct | saimatelecom.kg ',
                'O! - Store direct | ostore.kg',
                'O! - Bank direct | obank.kg'
            )"""

        query = sql_template.format(
            date_field=self.date_field,
            excluded_chats_filter=("  " + excluded_chats_filter if excluded_chats_filter else ""),
            exclude_dates_filter=("  " + exclude_dates_filter if exclude_dates_filter else ""),
            exclude_splits_filter=("  " + exclude_splits_filter if exclude_splits_filter else ""),
        )

        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "pst_positions": self.pst_positions,
            "project_ids": self.project_ids,
        }
        if excluded_chats:
            params["excluded_chats"] = excluded_chats
        if exclude_dates:
            params["exclude_dates"] = exclude_dates

        get_dagster_logger().info(f"get_aggregated_data_detailed params: {params}")
        get_dagster_logger().info(f"get_aggregated_data_detailed query: {query}")

        df = pd.read_sql_query(query, self.conn, params=params)
        return df.to_dict("records")

    def get_aggregated_data_detailed_with_started_at(self, exclude_splits: bool = False):
        """
        Агрегация по COALESCE(chat_created_at, chat_finished_at) с поправкой времени до 2024-08-21 (+6h).
        Фильтры: excluded_chats, (необязательно) self.excluded_days, exclude_splits.
        Группировка по self.date_field.
        """
        excluded_chats = self.get_excluded_chats() or []

        sql_path = self.base_sql_path / "stat" / "aggregated_data_detailed_with_started_at.sql"
        sql_template = sql_path.read_text(encoding="utf-8")

        # Алиас итоговой выборки — "c". Все фильтры должны ссылаться на него.
        excluded_chats_filter = ""
        if excluded_chats:
            excluded_chats_filter = "AND NOT (c.chat_id = ANY(%(excluded_chats)s::bigint[]))"

        exclude_dates_filter = ""
        # Если в объекте есть self.excluded_days — поддержим (необязательный функционал).
        if getattr(self, "excluded_days", None):
            exclude_dates_filter = "AND TO_CHAR(c.coalesced_local, 'YYYY-MM-DD') <> ALL(%(excluded_days)s::text[])"

        exclude_splits_filter = ""
        if exclude_splits:
            exclude_splits_filter = """AND c.split_title NOT IN (
                'O! - SMS | 705700700',
                'Saima - Saima direct | saimatelecom.kg ',
                'O! - Store direct | ostore.kg',
                'O! - Bank direct | obank.kg'
            )"""

        query = sql_template.format(
            date_field=self.date_field,
            excluded_chats_filter=("  " + excluded_chats_filter if excluded_chats_filter else ""),
            exclude_dates_filter=("  " + exclude_dates_filter if exclude_dates_filter else ""),
            exclude_splits_filter=("  " + exclude_splits_filter if exclude_splits_filter else ""),
        )

        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "pst_positions": self.pst_positions,
            "project_ids": self.project_ids,
        }
        if excluded_chats:
            params["excluded_chats"] = excluded_chats
        if getattr(self, "excluded_days", None):
            params["excluded_days"] = self.excluded_days

        get_dagster_logger().info(f"get_aggregated_data_detailed_with_started_at params: {params}")
        get_dagster_logger().info(f"get_aggregated_data_detailed_with_started_at query: {query}")

        df = pd.read_sql_query(query, self.conn, params=params)
        return df.to_dict("records")

    def get_bot_aggregated(self) -> List[Dict[str, Any]]:
        """
        Выполняет SQL из файла get_bot_aggregated.sql,
        подставляя параметры из атрибутов экземпляра.
        """
        # 1) Строим путь к файлу SQL
        sql_path = self.base_sql_path / "mp" / "get_bot_aggregated.sql"
        sql_template = sql_path.read_text(encoding="utf-8")

        query = sql_template.format(
            trunc=self.trunc,
            date_format=self.date_format,
            date_field=self.date_field,
        )

        # 2) Параметры для запроса — приводим все списки к кортежам
        params = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "project_ids": self.project_ids,
        }

        # 3) Выполняем запрос и возвращаем результат
        df = pd.read_sql_query(query, self.conn, params=params)

        get_dagster_logger().info(f"get_bot_aggregated: {df.to_dict(orient='records')}")
        return df.to_dict(orient="records")

    def get_entered_chats(self, exclude_splits: bool = False) -> List[Dict[str, Any]]:
        sql_path = self.base_sql_path / "mp" / "get_entered_chats.sql"
        sql_template = sql_path.read_text(encoding="utf-8")

        exclude_days_filter = ""
        params: dict[str, Any] = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "project_ids": self.project_ids,
        }

        if self.excluded_days:
            exclude_days_filter = (
                "AND NOT EXISTS ("
                "  SELECT 1 FROM unnest(%(excluded_days)s::date[]) d(day)"
                "  WHERE s.started_at >= d AND s.started_at < d + interval '1 day'"
                ")"
            )
            params["excluded_days"] = self.excluded_days

        exclude_splits_filter = ""
        if exclude_splits:
            exclude_splits_filter = "AND NOT (s.init_split_id = ANY(%(excluded_splits)s::int[]))"
            params["excluded_splits"] = [113, 169, 170, 171, 172]

        query = sql_template.format(
            date_field=self.date_field,
            exclude_days_filter=("  " + exclude_days_filter if exclude_days_filter else ""),
            exclude_splits_filter=("  " + exclude_splits_filter if exclude_splits_filter else ""),
        )

        df = pd.read_sql_query(query, self.conn, params=params)
        return df.to_dict("records")

    def get_all_aggregated(self):
        """
        Выполняет SQL из get_all_aggregated.sql и возвращает полный список записей.
        """
        sql_path = self.base_sql_path / "mp" / "get_all_aggregated.sql"
        sql_template = sql_path.read_text(encoding="utf-8")

        base_excluded = list(self.get_excluded_chats())

        extra_excluded = [
            1481690, 1490498, 4363973, 4986924, 4987932, 4988154, 4985686, 4985878,
            4988418, 4998057, 4998069, 4998129, 4998138, 4998238, 4998250, 4998260,
            4998263, 4991371, 4995622, 4998325, 4998340, 4998342, 4998344, 4998347,
            4998359, 4998366, 4998372, 4998375, 4998454, 4998461, 5003043, 5003059,
            5004141, 5007373, 5008001, 5012748, 5012748, 5013181, 5013589, 5014229,
            5016528, 5016562, 5017239, 5025168, 5026458, 5023588, 5023588, 5025455,
            5025736, 5026339, 5026473, 5026502, 5026505, 5026650, 5026740, 5027112,
            5027166, 5027260, 5027288, 5027423, 5027457, 5027457, 5027719, 5027815,
            5028154, 5028463, 5028518, 5028607, 5028608, 5028723, 5028778, 5029034,
            5029198, 5028702, 5028702, 5035944, 5037750, 5038046, 5039089, 5039185,
            5039697, 5040042, 5040752, 5041535, 5042346, 5042380, 5043886, 5044448,
            5045554, 5046296, 5046322, 5046397, 5046681, 5046978, 5047091, 5047336,
            5047439, 5048875, 5049055, 5049275, 5050489, 5050678, 5050844, 5050919,
            5051115, 5051136, 5051313, 5051356, 5051374, 5051569, 5051598, 5051609,
            5051657, 5051875, 5051897, 5051906, 5051983, 5052592, 5052861, 5052898,
            5045781, 5046743, 5047376, 5049271, 5049320, 5049365, 5050067, 5050149,
            5050946, 5051239, 5051330, 5051901, 5052180, 5052467, 5053532, 5053603,
            5053758, 5053762, 5053768, 5053807, 5053951, 5054038, 5054059, 5054080,
            5054117, 5054180, 5054215, 5054321, 5054321, 5054379, 5054383, 5054457,
            5054506, 5054771, 5054799, 5054893, 5055008, 5055044, 5055083, 5055115,
            5055125, 5055275, 5055290, 5055319, 5055394, 5055425, 5055567, 5055649,
            5055649, 5055682, 5055693, 5055884, 5055894, 5055921, 5055926, 5055928,
            5055996, 5056136, 5058502, 5058527, 5060381, 5060412, 5037889, 5037889,
            5071026, 5072355, 5072393, 5072761, 5074321, 5074922, 5075360, 5075852,
            5081868, 5082138, 5082312, 5082410, 5083670, 5084635, 5084666, 5084718,
            5084753, 5085241, 5086738, 5042093, 5042798, 5085274, 5092296, 5091199,
            5091228, 5094641, 5095043, 5095171, 5095186, 5096922, 5097115, 5097345,
            5097482, 5097574, 5097876, 5097896, 5096225, 5100065, 5104452, 5106116,
            5106209, 5063280, 5118007, 5118198, 5118301, 5118398, 5127025, 5127458,
            5163021, 5163207, 5165637, 5175391, 5175473, 5184538, 5185510, 5183180,
            5188581, 5190139, 5201660, 5201833, 5201857, 5096225, 5100065, 5104452,
            5106116, 5106209, 5063280, 5118007, 5118198, 5118301, 5118398, 5127458,
            5183180, 5188581, 5190139, 5201660, 5201833, 5201857,
        ]

        merged = list(base_excluded) + list(extra_excluded)

        seen = set()
        excluded_chats: List[int] = []
        for cid in merged:
            try:
                cid_int = int(cid)
            except (TypeError, ValueError):
                continue
            if cid_int not in seen:
                seen.add(cid_int)
                excluded_chats.append(cid_int)

        pst_filter = (
            "  AND su.position_id = ANY(%(pst_positions)s::int[])"
            if self.pst_positions
            else ""
        )
        excluded_filter = (
            "  AND NOT (amphtd.chat_id = ANY(COALESCE(%(excluded_chats)s::int[], '{}')))"

            if excluded_chats
            else ""
        )

        query = sql_template.format(
            trunc=self.trunc,
            date_format=self.date_format,
            date_field=self.date_field,
            pst_positions_filter=pst_filter,
            excluded_chats_filter=excluded_filter,
        )

        params = {
            "start_date":      self.start_date,
            "end_date":        self.end_date,
            "project_ids":     self.project_ids,
        }

        if self.pst_positions:
            params["pst_positions"] = self.pst_positions
        if excluded_chats:
            params["excluded_chats"] = list(excluded_chats)

        df = pd.read_sql_query(query, self.conn, params=params)

        get_dagster_logger().info(f"get_all_aggregated: {df.to_dict(orient='records')}")
        return df.to_dict(orient="records")

    def get_bot_aggregated_by_project_ids(self, exclude_splits: bool = False):
        """
        Фильтры:
          - даты, user_id=129, is_bot_only=TRUE
          - project_id IN self.project_ids
          - исключённые дни self.excluded_days (если заданы)
          - исключение сплитов по флагу exclude_splits
        """
        sql_path = self.base_sql_path / "stat" / "bot_aggregated_by_project_ids.sql"
        sql_template = sql_path.read_text(encoding="utf-8")

        excluded_days_filter = ""
        if getattr(self, "excluded_days", None):
            excluded_days_filter = "AND TO_CHAR(amphtd.chat_finished_at, 'YYYY-MM-DD') <> ALL(%(excluded_days)s::text[])"

        exclude_splits_filter = ""
        if exclude_splits:
            exclude_splits_filter = (
                "AND NOT (amphtd.split_title = ANY(%(excluded_split_titles)s::text[]))"
            )

        query = sql_template.format(
            date_field = self.date_field,
            excluded_days_filter=("  " + excluded_days_filter if excluded_days_filter else ""),
            exclude_splits_filter=("  " + exclude_splits_filter if exclude_splits_filter else ""),
        )

        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "project_ids": self.project_ids,
        }
        if getattr(self, "excluded_days", None):
            params["excluded_days"] = self.excluded_days
        if exclude_splits:
            params["excluded_split_titles"] = [
                "O! - SMS | 705700700",
                "Saima - Saima direct | saimatelecom.kg ",
                "O! - Store direct | ostore.kg",
                "O! - Bank direct | obank.kg",
            ]

        df = pd.read_sql_query(query, self.conn, params=params)
        return df.to_dict("records")


    def get_bot_aggregated_messages_by_project_ids(
            self,
            exclude_splits: bool = False,
    ):
        """
        Возвращает агрегат по датам и project_id: sum(user_count_replies) как total.
        """

        # читаем шаблон
        sql_path = self.base_sql_path / "stat" / "get_bot_aggregated_messages_by_project_ids.sql"
        sql_template = sql_path.read_text(encoding="utf-8")

        # --- опциональный фильтр по исключённым дням ---
        exclude_days_filter = ""
        if getattr(self, "excluded_days", None):
            exclude_days_filter = (
                "AND TO_CHAR(amphtd.chat_finished_at, 'YYYY-MM-DD') <> ALL(%(excluded_days)s::text[])"
            )

        exclude_splits_filter = ""
        if exclude_splits:
            exclude_splits_filter = """AND TRIM(amphtd.split_title) NOT IN (
                'O! - SMS | 705700700',
                'Saima - Saima direct | saimatelecom.kg',
                'O! - Store direct | ostore.kg',
                'O! - Bank direct | obank.kg'
            )"""

        query = sql_template.format(
            date_field=self.date_field,
            exclude_days_filter=("  " + exclude_days_filter if exclude_days_filter else ""),
            exclude_splits_filter=("  " + exclude_splits_filter if exclude_splits_filter else ""),
        )

        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "project_ids": self.project_ids,
        }
        if getattr(self, "excluded_days", None):
            params["excluded_days"] = self.excluded_days

        # get_dagster_logger().info(f"params={params}")
        # get_dagster_logger().info(f"query=\n{query}")

        df = pd.read_sql_query(query, self.conn, params=params)
        return df.to_dict("records")

    def get_full_aggregated(self):
        sql_path = self.base_sql_path / "mp" / "get_full_aggregated.sql"
        sql_template = sql_path.read_text(encoding="utf-8")
        query = sql_template.format(
            date_field=self.date_field
        )
        get_dagster_logger().info(f'self.date_format = {self.date_format}')

        params = {
            "trunc" : self.trunc,
            "date_format": self.date_format,
            "start_date":      self.start_date,
            "end_date":        self.end_date,
            "project_ids":     self.project_ids,
        }
        get_dagster_logger().info(f'params = {params}')
        df = pd.read_sql_query(query, self.conn, params=params)

        get_dagster_logger().info(f"get_full_aggregated: {df.to_dict(orient='records')}")
        return df.to_dict(orient="records")

    def get_detailed(self):
        if not getattr(self, "pst_positions", None):
            raise ValueError("pst_positions must be set (to match PHP getDetailed behavior).")

        sql_path = self.base_sql_path / "mp" / "get_project_detailed.sql"
        sql_template = sql_path.read_text(encoding="utf-8")

        # ВСЕГДА включаем оба фильтра, как в PHP
        pst_filter = "  AND su.position_id = ANY(%(pst_positions)s::int[])"
        excluded_filter = "  AND NOT (amphtd.chat_id = ANY(COALESCE(%(excluded_chats)s::int[], '{}')))"

        query = sql_template.format(
            date_field=self.date_field,
            pst_positions_filter=pst_filter,
            excluded_chats_filter=excluded_filter,
        )

        excluded_chats = self.get_excluded_chats() or []

        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "project_ids": self.project_ids,
            "pst_positions": self.pst_positions,  # обязателен
            "excluded_chats": list(excluded_chats),  # может быть пустым
        }

        get_dagster_logger().info(
            f"get_detailed params: trunc={self.trunc}, start={self.start_date}, end={self.end_date}, "
            f"projects={self.project_ids}, pst_positions={self.pst_positions}, excluded_chats={len(excluded_chats)}"
        )

        df = pd.read_sql_query(query, self.conn, params=params)
        return df.to_dict(orient="records")


    def get_project_entered_chats(self):
        sql_path = self.base_sql_path / "mp" / "get_project_entered_chats.sql"
        sql_template = sql_path.read_text(encoding="utf-8")
        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "project_ids": self.project_ids,
        }
        df = pd.read_sql_query(sql_template, self.conn, params=params)
        return df.to_dict(orient="records")

    def get_users_detailed(self):
        sql_path = self.base_sql_path / "mp" / "get_users_detailed.sql"
        sql_template = sql_path.read_text(encoding="utf-8")

        excluded_chats = self.get_excluded_chats() or []
        pst_filter = (
            "  AND su.position_id = ANY(%(pst_positions)s::int[])"
            if self.pst_positions
            else ""
        )
        excluded_filter = (
            "  AND NOT (amphtd.chat_id = ANY(COALESCE(%(excluded_chats)s::int[], '{}')))"

            if excluded_chats
            else "")

        query = sql_template.format(
            date_field = self.date_field,
            pst_positions_filter=pst_filter,
            excluded_chats_filter = excluded_filter)

        params = {
            "trunc": self.trunc,
            "date_format" : self.date_format,
            "start_date" : self.start_date,
            "end_date": self.end_date,
            "project_ids": self.project_ids
        }

        if self.pst_positions:
            get_dagster_logger().info(f'pst_positions: {self.pst_positions}')
            params["pst_positions"] = self.pst_positions
        if excluded_chats:
            get_dagster_logger().info(f'result e_chats: {excluded_chats}')
            params["excluded_chats"] = list(excluded_chats)

        df = pd.read_sql_query(query, self.conn, params=params)

        return df.to_dict(orient="records")

    def get_users_detailed_by_view(self):
        sql_path = self.base_sql_path / "mp" / "get_users_detailed_byview.sql"
        sql_template = sql_path.read_text(encoding="utf-8")
        excluded_chats = self.get_excluded_chats() or []

        pst_filter = (
            "  AND su.position_id = ANY(%(pst_positions)s::int[])"
            if self.pst_positions
            else ""
        )
        excluded_filter = (
            "  AND NOT (amphtd.chat_id = ANY(COALESCE(%(excluded_chats)s::int[], '{}')))"

            if excluded_chats
            else "")

        query = sql_template.format(
            date_field=self.date_field,
            pst_positions_filter=pst_filter,
            excluded_chats_filter=excluded_filter)

        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "project_ids": self.project_ids
        }
        if self.pst_positions:
            get_dagster_logger().info(f'pst_positions: {self.pst_positions}')
            params["pst_positions"] = self.pst_positions
        if excluded_chats:
            get_dagster_logger().info(f'result e_chats: {excluded_chats}')
            params["excluded_chats"] = list(excluded_chats)

        df = pd.read_sql_query(query, self.conn, params=params)

        get_dagster_logger().info(f"get_users_detailed_byview: {df.to_dict(orient='records')}")
        return df.to_dict(orient="records")

    def get_self_service_rate(self):
        sql_path = self.base_sql_path / "mp" / "self_service_rate.sql"
        sql_template = sql_path.read_text(encoding="utf-8")
        query = sql_template.format(
            date_field=self.date_field
        )

        params = {
            "trunc" : self.trunc,
            "date_format": self.date_format,
            "start_date":      self.start_date,
            "end_date":        self.end_date,
            "project_ids":     self.project_ids,
        }
        get_dagster_logger().info(f'query = {query}')
        get_dagster_logger().info(f'params = {params}')
        df = pd.read_sql_query(query, self.conn, params=params)
        get_dagster_logger().info(f"project_ids: {params['project_ids']}")
        get_dagster_logger().info(f"self_service_rate: {df.to_dict(orient='records')}")
        return df.to_dict(orient="records")

    @staticmethod
    def prepare_mp_data_getter(
            trunc: str,
            conn,
            report_date: datetime|None=None,
            start_date: datetime|None=None,
            end_date: datetime|None=None,
            project_ids=None,
            date_format=None,
    ) -> "MessagingPlatformDataGetter":
        """
        Factory: создаёт и настраивает экземпляр в зависимости от `trunc`.
        """
        getter = MessagingPlatformDataGetter(
            trunc=trunc,
            report_date=report_date,
            start_date=start_date,
            end_date=end_date,
            conn=conn,
            date_format=date_format,
            project_ids=project_ids
        )

        if trunc == 'hour':
            getter.set_hours_trunked_data()
        elif trunc == 'day':
            getter.set_days_trunked_data()
        elif trunc == 'month':
            getter.set_months_trunked_data()

        return getter

    def prepare_date(self, trunc, date_field):
        if trunc == 'hour':
            self.set_hours_trunked_data()
        elif trunc == 'day':
            self.set_days_trunked_data()
        elif trunc == 'month':
            self.set_months_trunked_data()

        if date_field == 'hour':
            self.date_format = 'HH24:MI:SS'

    def set_hours_trunked_data(self) -> None:
        """
        Настройка агрегации по часам.
        """
        self.trunc = 'hour'
        self.date_format = "YYYY-MM-DD HH24:MI:SS"

    def set_days_trunked_data(self) -> None:
        """
        Настройка агрегации по дням.
        """
        self.trunc = 'day'
        self.date_format = "YYYY-MM-DD HH24:MI:SS"

    def set_months_trunked_data(self) -> None:
        """
        Настройка агрегации по месяцам.
        """
        self.trunc = 'month'
        self.date_format = "YYYY-MM-DD"

    def get_mp_aggregated(self) -> List[Dict[str, Any]]:
        return self.get_aggregated()

    def get_mp_entered_chats(
            self,
            trunc: str,
            start_date,
            end_date,
            project_ids: List[int],
            conn: psycopg2.extensions.connection,
            date_field: str = "date",
            date_format: Optional[str] = "YYYY-MM-DD HH24:MI:SS",
    ):
        """
        Factory‑метод: создаёт и настраивает MessagingPlatformDataGetter
        для получения списка вошедших чатов, и возвращает результат get_entered_chats().
        """
        getter = self.prepare_mp_data_getter(
            trunc=trunc, conn=conn, start_date=start_date, end_date=end_date, date_format=date_format
        )
        getter.project_ids = project_ids
        getter.date_field = date_field
        if date_field == "hour":
            getter.date_format = "HH24:MI:SS"
        return getter.get_entered_chats()

    def get_bot_aggregated_percentage_with_project(self, exclude_splits: bool = False):
        sql_path = self.base_sql_path / "stat" / "bot_aggregated_percentage_with_project.sql"
        sql_template = sql_path.read_text(encoding="utf-8")

        excluded_days_filter = ""
        if getattr(self, "excluded_days", None):
            excluded_days_filter = (
                "AND TO_CHAR(amphtd.chat_finished_at, 'YYYY-MM-DD') <> ALL(%(excluded_days)s::text[])"
            )

        exclude_splits_filter = ""
        if exclude_splits:
            exclude_splits_filter = "AND NOT (amphtd.split_title = ANY(%(excluded_split_titles)s::text[]))"

        query = sql_template.format(
            date_field=self.date_field,
            excluded_days_filter=("  " + excluded_days_filter if excluded_days_filter else ""),
            exclude_splits_filter=("  " + exclude_splits_filter if exclude_splits_filter else ""),
        )

        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "project_ids": self.project_ids,
        }
        if getattr(self, "excluded_days", None):
            params["excluded_days"] = self.excluded_days
        if exclude_splits:
            params["excluded_split_titles"] = [
                "O! - SMS | 705700700",
                "Saima - Saima direct | saimatelecom.kg ",
                "O! - Store direct | ostore.kg",
                "O! - Bank direct | obank.kg",
            ]

        df = pd.read_sql_query(query, self.conn, params=params)
        return df.to_dict("records")

    def get_projects_map(self):
        sql_path = self.base_sql_path / "cp" / "get_projects_map.sql"
        sql = sql_path.read_text(encoding="utf-8")

        df = pd.read_sql_query(sql, self.conn, params={})
        if df.empty:
            return {}
        return dict(zip(df["id"].astype(int), df["title"].astype(str)))

    def get_bot_aggregated_percentage(self, exclude_splits: bool = False) -> list[dict]:
        sql_path = self.base_sql_path / "stat" / "bot_aggregated_percentage.sql"
        sql_template = sql_path.read_text(encoding="utf-8")

        excluded_days_filter = ""
        if getattr(self, "excluded_days", None):
            excluded_days_filter = \
                "AND TO_CHAR(amphtd.chat_finished_at, 'YYYY-MM-DD') <> ALL(%(excluded_days)s::text[])"

        exclude_splits_filter = ""
        if exclude_splits:
            # точные названия, как в PHP, включая пробел после saimatelecom.kg
            exclude_splits_filter = (
                "AND NOT (amphtd.split_title = ANY(%(excluded_split_titles)s::text[]))"
            )

        query = sql_template.format(
            date_field = self.date_field,
            excluded_days_filter=("  " + excluded_days_filter if excluded_days_filter else ""),
            exclude_splits_filter=("  " + exclude_splits_filter if exclude_splits_filter else ""),
        )
        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "project_ids": self.project_ids,
        }
        if getattr(self, "excluded_days", None):
            params["excluded_days"] = self.excluded_days
        if exclude_splits:
            params["excluded_split_titles"] = [
                "O! - SMS | 705700700",
                "Saima - Saima direct | saimatelecom.kg ",
                "O! - Store direct | ostore.kg",
                "O! - Bank direct | obank.kg",
            ]

        df = pd.read_sql_query(query, self.conn, params=params)
        return df.to_dict("records")

    def get_all_handled_chats(self) -> List[Dict[str, Any]]:
        return self.get_all_aggregated()

    def get_bot_handled_chats(self,
                              trunc,
                              start_date,
                              end_date,
                              projects,
                              date_field='date'):
        getter = MessagingPlatformDataGetter(conn=self.conn)
        getter.prepare_date(trunc=trunc, date_field=date_field)
        getter.start_date=start_date
        getter.end_date=end_date
        getter.project_ids=projects
        getter.date_field=date_field

        return getter.get_bot_aggregated_percentage()

    def get_bot_handled_chats_percentage(self, trunc, start_date, end_date,
                                         projects, date_field='date'):
        getter = MessagingPlatformDataGetter(conn=self.conn)
        getter.prepare_date(trunc=trunc, date_field=date_field)
        getter.start_date=start_date
        getter.end_date=end_date
        getter.project_ids=projects
        getter.date_field=date_field
        return getter.get_bot_aggregated_percentage()

    def get_bot_handled_chats_percentage_with_project(self, trunc, start_date, end_date, projects, date_field='date '):
        getter = MessagingPlatformDataGetter(conn=self.conn)
        getter.prepare_date(trunc=trunc, date_field=date_field)
        getter.start_date=start_date
        getter.end_date=end_date
        getter.project_ids = projects
        getter.date_field=date_field
        return getter.get_bot_aggregated_percentage_with_project()

    def projects_map(self):
        getter = MessagingPlatformDataGetter(conn=self.conn)
        return getter.get_projects_map()

    def get_full_aggregate(
            self,
            trunc: str,
            start_date,
            end_date,
            project_ids: List[int],
            conn: psycopg2.extensions.connection,
            date_field: str = "date",
            date_format: Optional[str] = "YYYY-MM-DD HH24:MI:SS",
    ):
        getter = self.prepare_mp_data_getter(
            trunc=trunc, conn=conn, start_date=start_date, end_date=end_date, date_format=date_format
        )
        getter.project_ids = project_ids
        getter.date_field = date_field
        if date_field == "hour":
            getter.date_format = "HH24:MI:SS"
        return getter.get_full_aggregated()

    def get_detailed_with_bot(self) -> list[dict]:

        extra_excluded = [
            1481690, 1490498, 4363973, 4986924, 4987932, 4988154, 4985686, 4985878, 4988418, 4998057, 4998069, 4998129,
            4998138, 4998238, 4998250, 4998260, 4998263, 4991371, 4995622, 4998325, 4998340, 4998342, 4998344, 4998347,
            4998359, 4998366, 4998372, 4998375, 4998454, 4998461, 5003043, 5003059, 5004141, 5007373, 5008001, 5012748,
            5012748, 5013181, 5013589, 5014229, 5016528, 5016562, 5017239, 5025168, 5026458, 5023588, 5023588, 5025455,
            5025736, 5026339, 5026473, 5026502, 5026505, 5026650, 5026740, 5027112, 5027166, 5027260, 5027288, 5027423,
            5027457, 5027457, 5027719, 5027815, 5028154, 5028463, 5028518, 5028607, 5028608, 5028723, 5028778, 5029034,
            5029198, 5028702, 5028702, 5035944, 5037750, 5038046, 5039089, 5039185, 5039697, 5040042, 5040752, 5041535,
            5042346, 5042380, 5043886, 5044448, 5045554, 5046296, 5046322, 5046397, 5046681, 5046978, 5047091, 5047336,
            5047439, 5048875, 5049055, 5049275, 5050489, 5050678, 5050844, 5050919, 5051115, 5051136, 5051313, 5051356,
            5051374, 5051569, 5051598, 5051609, 5051657, 5051875, 5051897, 5051906, 5051983, 5052592, 5052861, 5052898,
            5045781, 5046743, 5047376, 5049271, 5049320, 5049365, 5050067, 5050149, 5050946, 5051239, 5051330, 5051901,
            5052180, 5052467, 5053532, 5053603, 5053758, 5053762, 5053768, 5053807, 5053951, 5054038, 5054059, 5054080,
            5054117, 5054180, 5054215, 5054321, 5054321, 5054379, 5054383, 5054457, 5054506, 5054771, 5054799, 5054893,
            5055008, 5055044, 5055083, 5055115, 5055125, 5055275, 5055290, 5055319, 5055394, 5055425, 5055567, 5055649,
            5055649, 5055682, 5055693, 5055884, 5055894, 5055921, 5055926, 5055928, 5055996, 5056136, 5058502, 5058527,
            5060381, 5060412, 5037889, 5037889, 5071026, 5072355, 5072393, 5072761, 5074321, 5074922, 5075360, 5075852,
            5081868, 5082138, 5082312, 5082410, 5083670, 5084635, 5084666, 5084718, 5084753, 5085241, 5086738, 5042093,
            5042798, 5085274, 5092296, 5091199, 5091228, 5094641, 5095043, 5095171, 5095186, 5096922, 5097115, 5097345,
            5097482, 5097574, 5097876, 5097896, 5096225, 5100065, 5104452, 5106116, 5106209, 5063280, 5118007, 5118198,
            5118301, 5118398, 5127025, 5127458, 5163021, 5163207, 5165637, 5175391, 5175473, 5184538, 5185510, 5183180,
            5188581, 5190139, 5201660, 5201833, 5201857, 5096225, 5100065, 5104452, 5106116, 5106209, 5063280, 5118007,
            5118198, 5118301, 5118398, 5127458, 5183180, 5188581, 5190139, 5201660, 5201833, 5201857
        ]
        base = set(int(x) for x in (self.get_excluded_chats() or []))
        base.update(int(x) for x in extra_excluded)
        excluded_chats = sorted(base)

        # 2) Фильтры
        # Позиции: только для людей; бот (129) всегда допускается
        pst_filter = ""
        if getattr(self, "pst_positions", None):
            pst_filter = "  AND (amphtd.user_id = 129 OR su.position_id = ANY(%(pst_positions)s::int[]))"

        excluded_filter = ""
        if excluded_chats:
            excluded_filter = "  AND NOT (amphtd.chat_id = ANY(COALESCE(%(excluded_chats)s::int[], '{}')))"

        # 3) Шаблон SQL из файла
        sql_path = self.base_sql_path / "mp" / "get_detailed_with_bot.sql"
        sql_template = sql_path.read_text(encoding="utf-8")

        query = sql_template.format(
            date_field=self.date_field,
            pst_positions_filter=pst_filter,
            excluded_chats_filter=excluded_filter,
        )

        params = {
            "trunc": self.trunc,
            "date_format": self.date_format,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "project_ids": self.project_ids,
        }
        logger = None
        try:
            logger = get_dagster_logger()
        except Exception:
            logger = getattr(getattr(self, "context", None), "log", None)

        if getattr(self, "pst_positions", None):
            params["pst_positions"] = self.pst_positions
            if logger: logger.info(f"pst_positions: {self.pst_positions}")

        if excluded_chats:
            params["excluded_chats"] = excluded_chats
            if logger: logger.info(f"excluded_chats merged (len={len(excluded_chats)})")

        df = pd.read_sql_query(query, self.conn, params=params)
        return df.to_dict(orient="records")