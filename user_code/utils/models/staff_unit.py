from __future__ import annotations
from typing import Optional, List
from sqlalchemy import Column, Integer, ForeignKey, DateTime
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
import psycopg2

Base = declarative_base()

class StaffUnit(Base):
    __tablename__ = "staff_units"
    __table_args__ = {"schema": "bpm"}

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("bpm.users.id"), nullable=False)
    position_id = Column(Integer, ForeignKey("bpm.positions.id"), nullable=False)
    accepted_at = Column(DateTime, nullable=True)
    dismissed_at = Column(DateTime, nullable=True)
    # ... другие колонки по необходимости ...

    # Связь с моделью User
    user = relationship("User", back_populates="staff_units")

    @property
    def login(self) -> Optional[str]:
        """Атрибут login через связанный User"""
        return self.user.login if self.user else None

    @classmethod
    def get_id(
        cls,
        conn: psycopg2.extensions.connection,
        val: str,
        column: str = "login"
    ) -> Optional[int]:
        """
        1) Выбирает все user.id из bpm.users по полю column = val
        2) Среди них находит самый свежий staff_units.id (ORDER BY accepted_at DESC)
        """
        with conn.cursor() as cur:
            # Шаг 1: получить список user_id по логину
            cur.execute(
                f"SELECT id FROM bpm.users WHERE {column} = %s;",
                (val,)
            )
            user_ids = [row[0] for row in cur.fetchall()]

            if not user_ids:
                return None

            # Шаг 2: найти самый свежий staff_unit
            cur.execute(
                """
                SELECT id
                FROM bpm.staff_units
                WHERE user_id = ANY(%s)
                ORDER BY accepted_at DESC
                LIMIT 1;
                """,
                (user_ids,)
            )
            row = cur.fetchone()

        return row[0] if row else None