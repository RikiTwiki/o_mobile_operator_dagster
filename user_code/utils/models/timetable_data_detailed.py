from __future__ import annotations

from datetime import datetime, date, time, timedelta
from typing import List, Optional, Dict, Any

from sqlalchemy import (
    Column,
    Integer,
    Float,
    Date,
    DateTime,
    ForeignKey,
    func,
    select,
    and_,
    text,
    Time,
)
from sqlalchemy.orm import relationship, Session, aliased
from sqlalchemy.ext.declarative import declarative_base

from utils.models.timetable_session import TimetableSession
from utils.models.timetable_shift import TimetableShift
from utils.models.staff_unit import StaffUnit

from sqlalchemy import literal_column, and_, or_, text, func

import calendar

import psycopg2
from psycopg2.extensions import connection as PsycopgConnection

# NOTE: Department, Position, TimetableStatus, TimetableDisciplineData —
# импортируйте, когда добавите эти модели.

Base = declarative_base()


class TimetableDataDetailed(Base):
    """SQLAlchemy‑эквивалент модели **TimetableDataDetailed**.

    * Содержит базовые поля и все сервис‑методы, которые активно использует
      исходная PHP‑логика.
    * Там, где необходимы сложные фильтры / агрегации, оставлены точечные
      комментарии. При наличии соответствующих моделей можно расширить
      реализацию.
    """

    __tablename__ = "timetable_data_detailed"
    __table_args__ = {"schema": "bpm"}

    # ------------------------------------------------------------------
    # Columns
    # ------------------------------------------------------------------
    id = Column(Integer, primary_key=True, autoincrement=False)

    timetable_view_id = Column(Integer, nullable=False)

    timetable_session_id = Column(Integer, ForeignKey("bpm.timetable_sessions.id"), nullable=False)
    timetable_shift_id = Column(Integer, ForeignKey("bpm.timetable_shifts.id"), nullable=False)
    staff_unit_id = Column(Integer, ForeignKey("bpm.staff_units.id"), nullable=False)

    date = Column(Date, nullable=False)
    month = Column(Date, nullable=False)

    session_start = Column(Time, nullable=False)
    session_end = Column(Time, nullable=False)

    coffee_time = Column(Integer, nullable=True)

    total_hours = Column(Float, nullable=False)
    day_hours = Column(Float, nullable=False)
    night_hours = Column(Float, nullable=False)

    # ------------------------------------------------------------------
    # Relationships
    # ------------------------------------------------------------------
    session = relationship(
        "TimetableSession",
        back_populates="detailed_data",
        lazy="joined",
        viewonly=True,
    )
    shift = relationship("TimetableShift", back_populates="detailed_data")
    unit = relationship(
        "StaffUnit",
        back_populates="detailed_data",
        lazy="joined",
        viewonly=True,
    )

    # ------------------------------------------------------------------
    # Computed properties (analogs of $appends)
    # ------------------------------------------------------------------
    @property
    def title(self) -> Optional[str]:
        return getattr(self.session, "title", None)

    @property
    def coffee_time(self) -> Optional[int]:
        # если в TimetableShift есть атрибут .coffee
        return getattr(self.shift, "coffee", None)

    @property
    def infection_discipline(self) -> bool:
        return bool(self.session and self.session.props.get("infection_discipline"))

    @property
    def productive_time(self) -> bool:
        return bool(self.session and self.session.props.get("productive_time"))

    @property
    def staff_unit_login(self) -> Optional[str]:
        return getattr(self.unit.user if self.unit else None, "login", None)

    # ------------------------------------------------------------------
    # Utility setters
    # ------------------------------------------------------------------
    def set_start_and_end(self, day: date, start: time, end: time) -> None:
        """Полный аналог PHP setStartAndEnd."""
        start_dt = datetime.combine(day, start)
        end_dt = datetime.combine(day + timedelta(days=1), end) if start > end else datetime.combine(day, end)
        self.session_start = start_dt
        self.session_end = end_dt

    # ------------------------------------------------------------------
    # Static / class methods — переписаны ключевые выборки
    # ------------------------------------------------------------------

    # ----  Шифты для TimetableView  ------------------------------------
    @staticmethod
    def get_shifts_by_view_id(conn: PsycopgConnection, view_id: int) -> List[str]:
        """
        Возвращает список времен начала смен для данного view_id,
        используя raw SQL и psycopg2.Connection.
        """
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT to_char(session_start, 'HH24:MI:SS') AS shift_time
                FROM bpm.timetable_data_detailed
                WHERE timetable_view_id = %s
                ORDER BY shift_time;
                """,
                (view_id,),
            )
            rows = cur.fetchall()
        return [r[0] for r in rows]

    # ----  Время первого выхода сотрудника в смену ---------------------
    @classmethod
    def get_staff_unit_start_time(
        cls, session: Session, staff_unit_id: int, d: date | None = None
    ) -> Optional[datetime]:
        if d is None:
            d = date.today()
        q = (
            session.query(cls.session_start)
            .filter(cls.staff_unit_id == staff_unit_id, cls.date == d)
            .order_by(cls.session_start)
            .limit(1)
        )
        res = q.scalar()
        return res.strftime("%H:%M:%S") if res else None

    # ----  Сколько «плановых» дней у сотрудника в месяце ----------------
    @classmethod
    def staff_unit_planned_days(
        cls,
        session: Session,
        staff_unit_id: int,
        month_: date | None = None,
        excluded_days: List[date] | None = None,
    ) -> int:
        if month_ is None:
            month_ = date.today().replace(day=1)
        q = (
            session.query(cls.date)
            .join(TimetableSession, TimetableSession.id == cls.timetable_session_id)
            .filter(
                cls.staff_unit_id == staff_unit_id,
                cls.month == month_,
                text("timetable_sessions.props ->> 'productive_time' = 'true'")
            )
            .distinct()
        )
        return q.count()

    # ----  Записи (session_start / session_end) для сотрудника по дням --
    @classmethod
    def staff_unit_planned_days_data(
        cls,
        session: Session,
        staff_unit_id: int,
        month_: date | None = None,
        excluded_days: List[date] | None = None,
    ) -> List[Dict[str, Any]]:
        if month_ is None:
            month_ = date.today().replace(day=1)
        q = (
            session.query(
                cls.timetable_session_id,
                cls.date,
                cls.session_start,
                cls.session_end,
            )
            .join(TimetableSession, TimetableSession.id == cls.timetable_session_id)
            .filter(
                cls.staff_unit_id == staff_unit_id,
                cls.month == month_,
                cls.timetable_session_id.in_([1, 3]),
                text("timetable_sessions.props ->> 'productive_time' = 'true'"),
            )
        )
        if excluded_days:
            q = q.filter(~cls.date.in_(excluded_days))
        return [dict(row._mapping) for row in q]

    # ----  Старт/финиш первой и последней смены сотрудника  -------------
    @classmethod
    def staff_unit_planned_start_end_times(
        cls,
        session: Session,
        staff_unit_id: int,
        month_: date | None = None,
        excluded_days: List[date] | None = None,
    ) -> Dict[str, datetime]:
        if month_ is None:
            month_ = date.today().replace(day=1)

        base_q = (
            session.query(cls)
            .join(TimetableSession, TimetableSession.id == cls.timetable_session_id)
            .filter(
                cls.staff_unit_id == staff_unit_id,
                cls.month == month_,
                text("timetable_sessions.props ->> 'productive_time' = 'true'"),
            )
        )
        if excluded_days:
            base_q = base_q.filter(~cls.date.in_(excluded_days))

        first_row = (
            base_q.order_by(cls.date, cls.session_start).limit(1).first()
        )
        last_row = (
            base_q.order_by(cls.date.desc(), cls.session_end.desc()).limit(1).first()
        )
        year, mon = month_.year, month_.month
        last_day = date(year, mon, calendar.monthrange(year, mon)[1])
        return {
            "first_day_session_start": first_row.session_start if first_row else month_,
            "last_day_session_end": last_row.session_end if last_row else last_day,
        }

    @classmethod
    def staff_unit_planned_days_with_sick_leave(
            cls,
            session: Session,
            staff_unit_id: int,
            month_: date | None = None
    ) -> Dict[str, int]:
        if month_ is None:
            month_ = date.today().replace(day=1)
        # плановые
        planned = (
            session.query(cls.date)
            .filter(cls.staff_unit_id == staff_unit_id, cls.month == month_)
            .join(TimetableSession, TimetableSession.id == cls.timetable_session_id)
            .filter(text("timetable_sessions.props ->> 'productive_time' = 'true'"))
            .distinct()
            .all()
        )
        # отсутствия (ID 6 и 8)
        absent = (
            session.query(cls.date)
            .filter(
                cls.staff_unit_id == staff_unit_id,
                cls.month == month_,
                cls.timetable_session_id.in_([6, 8])
            )
            .distinct()
            .all()
        )
        planned_days = {r[0] for r in planned}
        absent_days = {r[0] for r in absent}
        return {
            "plannedDays": len(planned_days),
            "absentDays": len(absent_days),
        }

    # ----  Сумма плановых секунд сотрудника за месяц --------------------
    @classmethod
    def staff_unit_planned_time(
        cls,
        session: Session,
        staff_unit_id: int,
        month_: date | None = None,
        excluded_days: List[date] | None = None,
    ) -> int:
        if month_ is None:
            month_ = date.today().replace(day=1)
        q = session.query(cls.day_hours, cls.night_hours).filter(
            cls.staff_unit_id == staff_unit_id,
            cls.month == month_,
        )
        if excluded_days:
            q = q.filter(~cls.date.in_(excluded_days))
        total_sec = 0
        for dh, nh in q:
            total_sec += int((dh + nh) * 3600)
        return total_sec

    @classmethod
    def position_planned_time(
            cls,
            session: Session,
            position_ids: List[int],
            start_date: date,
            end_date: date
    ) -> int:
        """
        Возвращает суммарное количество запланированных секунд (day_hours+night_hours)*3600
        для всех сотрудников на заданных позициях в пределах месяца start_date…end_date,
        учитывая только продуктивное время.
        """
        # Условия для left join с таблицей staff_units
        join_condition = and_(
            cls.staff_unit_id == StaffUnit.id,
            StaffUnit.accepted_at <= end_date,
            or_(
                StaffUnit.dismissed_at > start_date,
                StaffUnit.dismissed_at.is_(None)
            )
        )

        # Суммируем в БД только те записи, где productive_time = true
        total_seconds = session.query(
            func.coalesce(func.sum((cls.day_hours + cls.night_hours) * 3600), 0)
        ).join(
            StaffUnit, join_condition  # LEFT JOIN эквивалент
        ).join(
            TimetableSession,
            TimetableSession.id == cls.timetable_session_id
        ).filter(
            StaffUnit.position_id.in_(position_ids),
            cls.month == start_date,
            text("timetable_sessions.props ->> 'productive_time' = 'true'")
        ).scalar()

        return int(total_seconds)

    @classmethod
    def position_planned_time_day(
            cls,
            session: Session,
            position_ids: List[int],
            start_date: date,
            end_date: date
    ) -> int:
        """
        Возвращает суммарное количество запланированных секунд (day_hours+night_hours)*3600
        для всех сотрудников на заданных позициях за конкретную дату start_date,
        учитывая только продуктивное время.
        """
        # Условия для LEFT JOIN с таблицей staff_units
        join_condition = and_(
            cls.staff_unit_id == StaffUnit.id,
            StaffUnit.accepted_at <= end_date,
            or_(
                StaffUnit.dismissed_at > start_date,
                StaffUnit.dismissed_at.is_(None)
            )
        )

        total_seconds = session.query(
            func.coalesce(func.sum((cls.day_hours + cls.night_hours) * 3600), 0)
        ).outerjoin(
            StaffUnit, join_condition
        ).join(
            TimetableSession, TimetableSession.id == cls.timetable_session_id
        ).filter(
            StaffUnit.position_id.in_(position_ids),
            cls.date == start_date,
            text("timetable_sessions.props ->> 'productive_time' = 'true'")
        ).scalar()

        return int(total_seconds)

    @classmethod
    def daily_position_planned_time(
            cls,
            session: Session,
            position_ids: List[int],
            start_date: date,
            end_date: date
    ) -> int:
        """
        Возвращает суммарное количество запланированных секунд (day_hours+night_hours)*3600
        для всех сотрудников на заданных позициях за конкретную дату start_date,
        учитывая только продуктивное время.
        """
        # Условие для LEFT JOIN с таблицей staff_units
        join_condition = and_(
            cls.staff_unit_id == StaffUnit.id,
            StaffUnit.accepted_at <= end_date,
            or_(
                StaffUnit.dismissed_at > start_date,
                StaffUnit.dismissed_at.is_(None)
            )
        )

        total_seconds = session.query(
            func.coalesce(func.sum((cls.day_hours + cls.night_hours) * 3600), 0)
        ).outerjoin(
            StaffUnit, join_condition
        ).join(
            TimetableSession, TimetableSession.id == cls.timetable_session_id
        ).filter(
            StaffUnit.position_id.in_(position_ids),
            cls.date == start_date,
            text("timetable_sessions.props ->> 'productive_time' = 'true'")
        ).scalar()

        return int(total_seconds)

    # ----  Кофе‑брейки сотрудника за день --------------------------------
    @classmethod
    def staff_unit_daily_planned_coffee(
        cls, session: Session, staff_unit_id: int, d: date | None = None
    ) -> int:
        if d is None:
            d = date.today()
        q = session.query(cls).filter(
            cls.staff_unit_id == staff_unit_id, cls.date == d
        )
        return sum(int(row.coffee_time or 0) for row in q if row.productive_time)

    # ----  Больничные ----------------------------------------------------
    @classmethod
    def staff_unit_sick_leave(
        cls,
        session: Session,
        staff_unit_id: int,
        month_: date | None = None,
        excluded_days: List[date] | None = None,
    ) -> Dict[str, Any]:
        if month_ is None:
            month_ = date.today().replace(day=1)
        sick_leave_session_id = 6  # as in PHP constant
        q = session.query(cls.total_hours, cls.date).filter(
            cls.staff_unit_id == staff_unit_id,
            cls.month == month_,
            cls.timetable_session_id == sick_leave_session_id,
        )
        if excluded_days:
            q = q.filter(~cls.date.in_(excluded_days))
        rows = q.all()
        total_sec = sum(int(r.total_hours * 3600) for r in rows)
        unique_days = {r.date for r in rows}
        return {"time": total_sec, "days": len(unique_days)}

    # ------------------------------------------------------------------
    # Internal helper
    # ------------------------------------------------------------------
    @classmethod
    def _is_productive(cls, session: Session, staff_unit_id: int, d: date) -> bool:
        row = (
            session.query(cls)
            .join(TimetableSession, TimetableSession.id == cls.timetable_session_id)
            .filter(
                cls.staff_unit_id == staff_unit_id,
                cls.date == d,
                text("timetable_sessions.props ->> 'productive_time' = 'true'"),
            )
            .first()
        )
        return bool(row)
