from sqlalchemy import Column, Integer, JSON
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class TimetableStatus(Base):
    """
    SQLAlchemy model для таблицы bpm.timetable_status.
    Поле props хранится в формате JSON и автоматически приводится к dict в Python.
    """
    __tablename__ = "timetable_status"
    __table_args__ = {"schema": "bpm"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    props = Column(JSON, nullable=True)