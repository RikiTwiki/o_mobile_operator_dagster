from __future__ import annotations

import json
import os
import logging
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import requests


# ===== Вспомогательные функции =================================================

def _day_to_str(day: Union[str, date, datetime]) -> str:
    """Нормализует дату к формату 'DD.MM.YYYY' (как в примере curl)."""
    if isinstance(day, datetime):
        return day.strftime("%d.%m.%Y")
    if isinstance(day, date):
        return day.strftime("%d.%m.%Y")
    return str(day)


def _tags_to_str(tags: Union[str, List[str]]) -> str:
    """Если список — сериализует в JSON-строку; если уже строка — возвращает как есть."""
    return json.dumps(tags, ensure_ascii=False) if isinstance(tags, list) else str(tags)


# ===== Конфигурация ============================================================

@dataclass
class ReportsApiConfig:
    """
    Конфиг для загрузчика отчётов.

    Параметры по умолчанию подхватываются из переменных окружения:
    - REPORTS_API_BASE (fallback: https://cc1.o.kg)
    - REPORTS_API_TOKEN (может быть пустым)
    """
    base_url: str = os.getenv("REPORTS_API_BASE", "https://cc1.o.kg")
    token: Optional[str] = os.getenv("REPORTS_API_TOKEN")
    timeout_connect: int = 10
    timeout_read: int = 120

    @property
    def base_url_clean(self) -> str:
        return self.base_url.rstrip("/")


# ===== Класс загрузчика =======================================================

class ReportInstanceUploader:
    """
    Класс для загрузки PDF-отчёта в API:
      POST {base_url}/api/report-instances/upload/{report_id}
      multipart/form-data: file, title, summary, day, tags
    """

    def __init__(
        self,
        config: Optional[ReportsApiConfig] = None,
        logger: Optional[logging.Logger] = None,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.config = config or ReportsApiConfig()
        self.log = logger or logging.getLogger(self.__class__.__name__)
        self.session = session or requests.Session()

    def upload(
        self,
        file_path: Union[str, Path],
        report_id: int,
        title: str,
        summary: str,
        day: Union[str, date, datetime],
        tags: Union[str, List[str]],
    ) -> Dict[str, Any]:
        """
        Загружает PDF-файл как инстанс отчёта. Возвращает JSON-ответ API как dict.
        Если API вернул не-JSON, вернётся {'status_code': int, 'text': str}.

        Исключения:
          - FileNotFoundError, если файла нет
          - requests.HTTPError при статусах 4xx/5xx (с подробным логом тела)
          - любые другие исключения прокидываются наружу
        """
        cfg = self.config
        url = f"{cfg.base_url_clean}/api/report-instances/upload/{report_id}"

        file_path = Path(file_path)
        if not file_path.is_file():
            raise FileNotFoundError(f"PDF file not found: {file_path}")

        day_str = _day_to_str(day)
        tags_str = _tags_to_str(tags)

        headers = {"Accept": "application/json"}
        if cfg.token:
            headers["Authorization"] = f"Bearer {cfg.token}"

        self.log.info("Uploading PDF to %s", url)
        self.log.debug("title=%r, summary=%r, day=%s, tags=%s", title, summary, day_str, tags_str)

        try:
            with file_path.open("rb") as f:
                files = {"file": (file_path.name, f, "application/pdf")}
                data = {
                    "title": title,
                    "summary": summary,
                    "day": day_str,
                    "tags": tags_str,
                }
                resp = self.session.post(
                    url,
                    headers=headers,
                    data=data,
                    files=files,
                    timeout=(cfg.timeout_connect, cfg.timeout_read),
                )

            # Бросит HTTPError при неуспешном статусе
            try:
                resp.raise_for_status()
            except requests.HTTPError as http_err:
                body = getattr(http_err.response, "text", "")
                code = getattr(http_err.response, "status_code", "??")
                self.log.error("Upload failed: HTTP %s; body=%s", code, body)
                raise

            # Пытаемся распарсить JSON-ответ
            try:
                payload = resp.json()
            except Exception:
                payload = {"status_code": resp.status_code, "text": resp.text}

            self.log.info("Upload OK (status=%s)", resp.status_code)
            return payload

        except Exception as e:
            # Любая непредвиденная ошибка — с трассировкой
            self.log.exception("Upload failed with unexpected error: %s", e)
            raise