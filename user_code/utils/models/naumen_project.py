from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Boolean
from sqlalchemy import create_engine, event

import datetime

from typing import ClassVar

# Базовый класс для декларативного определения моделей
Base = declarative_base()


class NaumenProject(Base):
    __tablename__ = 'naumen_projects'  # Имя таблицы, можно изменить при необходимости

    __table_args__ = {'schema': 'bpm'}

    id = Column(Integer, primary_key=True)
    project_id = Column(String)  # Если требуется, например, для хранения project_id

    source = Column(String)

    replicate = Column(Boolean, default=True)
    archived = Column(Boolean, default=False)

    arpu = Column(String)

    title = Column(String)
    group = Column(String)
    type = Column(String)
    language = Column(String)
    replicate_start_at = Column(String)  # Можно использовать DateTime, если требуется

    target_time = Column(String)

    short_calls_time_out = Column(String)

    connection: ClassVar[str] = 'bpm' # type: ignore

    @classmethod
    def all(cls, session):
        """
        Возвращает все проекты из базы.
        """
        return session.query(cls).all()

    def to_dict(self):
        result = {}
        for column in self.__table__.columns:
            value = getattr(self, column.name)
            # Если значение является датой или datetime, преобразуем в строку ISO
            if isinstance(value, (datetime.date, datetime.datetime)):
                value = value.isoformat()
            result[column.name] = value
        return result


# Функция для создания engine для подключения к базе 'bpm'
def get_bpm_engine():
    # Замените строку подключения на вашу актуальную конфигурацию подключения к БД 'bpm'
    engine = create_engine("postgresql://dag_user:IUas12sh@172.27.129.157:5432/ccdwh")
    return engine