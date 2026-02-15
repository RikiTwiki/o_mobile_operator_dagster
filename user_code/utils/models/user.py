# user_code/models/bpm/user.py

from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

from utils.models.staff_unit import StaffUnit

Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    __table_args__ = {"schema": "bpm"}

    # Поля таблицы users
    id = Column(Integer, primary_key=True)
    login = Column(String, nullable=False)
    # добавьте другие поля по необходимости

    # Отношение к StaffUnit
    staff_units = relationship(
        "StaffUnit",
        back_populates="user",
        primaryjoin="User.id==StaffUnit.user_id",
    )