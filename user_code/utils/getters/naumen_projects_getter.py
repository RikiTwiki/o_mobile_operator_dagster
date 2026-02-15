import datetime
from typing import List, Dict, Callable, Optional, Iterable

from dagster import get_dagster_logger
from sqlalchemy.orm import Session
from sqlalchemy import select
from sqlalchemy.inspection import inspect as sa_inspect

from utils.models.naumen_project import NaumenProject, get_bpm_engine


class NaumenProjectsGetter:
    """
    Быстрый геттер проектов Naumen из bpm.naumen_projects.
    Загружает снимок один раз; можно refresh_from_db() для обновления.
    Фильтры обновляют working-set self.project_id и синхронно пересобирают SelectedProjects/SelectedProjectsInt.
    """

    def __init__(self):
        self._engine = get_bpm_engine()
        self.ProjectsInDB: List[Dict] = []
        self.project_id: List[Dict] = []
        self.SelectedProjects: List[str] = []
        self.SelectedProjectsInt: List[int] = []
        self.refresh_from_db()

    # ---------- DB snapshot ----------
    def refresh_from_db(self) -> None:
        """Перечитать все проекты из БД и сбросить фильтры."""
        def as_dict(model):
            return {c.key: getattr(model, c.key) for c in sa_inspect(model).mapper.column_attrs}

        with Session(self._engine) as session:
            models = session.execute(select(NaumenProject)).scalars().all()
            self.ProjectsInDB = [as_dict(m) for m in models]

        self.reload_projects()

    def reload_projects(self) -> None:
        """Сбросить фильтры к исходному срезу."""
        self.project_id = list(self.ProjectsInDB)
        self._set_select()

    # ---------- Filters ----------
    def only_replicable(self) -> List[Dict]:
        """Оставить только проекты replicate == True (первая проверка в PHP)."""
        return self._filter(lambda item: bool(item.get('replicate', True)))

    def exclude_archived(self) -> List[Dict]:
        """Исключить archived == True (вторая независимая проверка в PHP)."""
        return self._filter(lambda item: not bool(item.get('archived', False)))

    def filter_projects_by_group(self, group: str, type_: str = 'Operator') -> List[Dict]:
        return self._filter(lambda item: item.get('group') == group and item.get('type') == type_)

    def filter_projects_by_group_array(self, groups, type_: str = 'Operator') -> List[Dict]:
        groups_set = set(groups)
        return self._filter(lambda item: item.get('group') in groups_set and item.get('type') == type_)

    def filter_projects_by_lang(self, language: str) -> List[Dict]:
        return self._filter(lambda item: item.get('language') == language)

    # ---- Source filters with alias normalization ----
    def _normalize_source_alias(self, alias: str) -> Optional[str]:
        """
        Приводим алиасы к сырому значению в БД:
        return 'Naumen' | 'Naumen3' | None
        """
        a = (alias or '').strip().lower()
        if a in ('naumen', 'naumen_1', 'naumen1', 'n1', 'source_naumen_1'):
            return 'Naumen'
        if a in ('naumen3', 'naumen_3', 'n3', 'source_naumen_3'):
            return 'Naumen3'
        return None

    def filter_by_source(self, source_alias: str) -> List[Dict]:
        raw = self._normalize_source_alias(source_alias)
        if raw is None:
            # если не распознали алиас — ничего не меняем
            return self.project_id
        return self._filter(lambda item: item.get('source') == raw)

    def filter_by_sources(self, aliases: Iterable[str]) -> List[Dict]:
        raws = {self._normalize_source_alias(a) for a in aliases}
        raws.discard(None)
        if not raws:
            return self.project_id
        return self._filter(lambda item: item.get('source') in raws)

    # ---------- Internals ----------
    def _set_select(self) -> None:
        """Пересобрать SelectedProjects (str) и SelectedProjectsInt (int) из текущего working-set."""
        sel_str: List[str] = []
        sel_int: List[int] = []

        for item in self.project_id:
            pid = item.get('project_id')
            if pid is None:
                continue
            # нормализуем к строке
            s = str(pid).strip()
            if not s:
                continue
            sel_str.append(s)
            # безопасный int-каст (для SQL IN (...))
            try:
                sel_int.append(int(s))
            except (TypeError, ValueError):
                # не числовой project_id — пропускаем в int-версии
                pass

        self.SelectedProjects = sel_str
        self.SelectedProjectsInt = sel_int

    def _filter(self, callback: Callable[[Dict], bool]) -> List[Dict]:
        self.project_id = [item for item in self.project_id if callback(item)]
        self._set_select()
        return self.project_id

    # ---------- Date helpers (как и было) ----------
    def date(self, date_str: Optional[str] = None) -> str:
        if date_str is None:
            date_str = datetime.datetime.now().strftime("%Y-%m-%d") + " 00:00:00"
        dt = datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        return dt.strftime("%Y-%m-%d 00:00:00")

    def date_add_one_day(self, date_str: Optional[str] = None) -> str:
        if date_str is None:
            date_str = datetime.datetime.now().strftime("%Y-%m-%d") + " 00:00:00"
        dt = datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(days=1)
        return dt.strftime("%Y-%m-%d 00:00:00")

    def increment_date(self, date_str: Optional[str] = None, add: str = " + 1 hour") -> str:
        if date_str is None:
            date_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        dt = datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        add = add.strip()
        if "hour" in add:
            parts = add.split()
            hours = int(parts[1]) if len(parts) > 1 and parts[1].lstrip("+-").isdigit() else 1
            dt += datetime.timedelta(hours=hours)
        elif "day" in add:
            parts = add.split()
            days = int(parts[1]) if len(parts) > 1 and parts[1].lstrip("+-").isdigit() else 1
            dt += datetime.timedelta(days=days)
        else:
            dt += datetime.timedelta(hours=1)
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    def check_date(self, date_str: str) -> bool:
        dt = datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        return dt.strftime("%H:%M:%S") != "23:00:00"