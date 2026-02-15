# utils/resources/oracle_dwh.py
from dagster import ConfigurableResource
from typing import Any, Dict, Iterable, List, Optional
import oracledb

class OracleDWHResource(ConfigurableResource):
    """
    Конфигурируемый ресурс для подключения к Oracle DWH.
    Пример DSN: "172.27.129.151:1521/DWH"
    Все секреты — через ENV или Dagster secrets.
    """
    dsn: str
    user: str
    password: str
    encoding: str = "UTF-8"   # аналог AL32UTF8; oracledb использует "UTF-8"
    nencoding: str = "UTF-8"

    def _connect(self) -> oracledb.Connection:
        # THIN mode по умолчанию (без Instant Client).
        return oracledb.connect(
            user=self.user,
            password=self.password,
            dsn=self.dsn,
            encoding=self.encoding,
            nencoding=self.nencoding,
        )

    def fetch_all(self, sql: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Выполняет запрос и возвращает список dict-строк с UPPERCASE ключами колонок,
        как это обычно возвращает Oracle (cursor.description).
        """
        params = params or {}
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                cols = [d[0] for d in cur.description]  # названия колонок
                out: List[Dict[str, Any]] = []
                for row in cur:
                    out.append(dict(zip(cols, row)))
                return out
