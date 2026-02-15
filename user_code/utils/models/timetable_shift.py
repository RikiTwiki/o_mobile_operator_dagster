from sqlalchemy import Column, Integer
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class TimetableShift(Base):
    __tablename__ = "timetable_shifts"
    __table_args__ = {"schema": "bpm"}

    id = Column(Integer, primary_key=True)
    # добавьте другие поля по необходимости