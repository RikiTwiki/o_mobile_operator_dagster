from sqlalchemy import Column, Integer, JSON, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

from utils.models.timetable_status import TimetableStatus

Base = declarative_base()

class TimetableSession(Base):
    __tablename__ = "timetable_sessions"
    __table_args__ = {"schema": "bpm"}

    # Поля таблицы
    id = Column(Integer, primary_key=True)
    timetable_status_id = Column(Integer, ForeignKey("bpm.timetable_statuses.id"), nullable=True)
    props = Column(JSON, nullable=True)

    # Отношение к TimetableStatus
    status = relationship(
        "TimetableStatus",
        back_populates="sessions",
        foreign_keys=[timetable_status_id]
    )

    @property
    def timetable_status_title(self) -> str | None:
        """
        Атрибут для получения названия статуса из связанной модели TimetableStatus.
        """
        return self.status.title if self.status else None