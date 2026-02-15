# resources/dwh.py
from dagster import ConfigurableResource
from typing import Any, Dict, List, Optional
from pydantic import PrivateAttr
import oracledb

class OracleDWHResource(ConfigurableResource):
    """
    Ресурс Oracle с пулом сессий.
    Совместим с вашими ENV:
      DB_HOST_DWH, DB_PORT_DWH, DB_DATABASE_DWH (service_name),
      DB_USERNAME_DWH, DB_PASSWORD_DWH, DB_CHARSET_DWH (AL32UTF8)
    """
    host: str
    port: int
    service_name: str
    user: str
    password: str
    pool_min: int = 1
    pool_max: int = 4
    pool_increment: int = 1

    _pool: Optional["oracledb.ConnectionPool"] = PrivateAttr(default=None)

    def setup_for_execution(self, _context) -> None:
        dsn = f"{self.host}:{self.port}/{self.service_name}"

        # (опционально) Инициализация сессии: TZ на Бишкек
        def _session_init(conn, _tag):
            with conn.cursor() as cur:
                cur.execute("ALTER SESSION SET TIME_ZONE = '+06:00'")

        self._pool = oracledb.create_pool(
            user=self.user,
            password=self.password,
            dsn=dsn,
            min=self.pool_min,
            max=self.pool_max,
            increment=self.pool_increment,
            homogeneous=True,
            session_callback=_session_init,
            # ❌ Больше НЕ задаём: encoding / nencoding / threaded
        )

    def _conn(self) -> oracledb.Connection:
        if not self._pool:
            self.setup_for_execution(None)
        return self._pool.acquire()

    def fetch_all(self, sql: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        params = params or {}
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, row)) for row in cur]