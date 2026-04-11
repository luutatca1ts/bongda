"""Database models for football analytics."""

from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime, Boolean
from sqlalchemy.orm import declarative_base, sessionmaker
from src.config import DATABASE_URL

Base = declarative_base()


class Match(Base):
    __tablename__ = "matches"

    id = Column(Integer, primary_key=True)
    match_id = Column(Integer, unique=True, index=True)  # Football-Data ID
    competition = Column(String)
    competition_code = Column(String)
    home_team = Column(String)
    home_team_id = Column(Integer)
    away_team = Column(String)
    away_team_id = Column(Integer)
    utc_date = Column(DateTime)
    matchday = Column(Integer, nullable=True)
    home_goals = Column(Integer, nullable=True)
    away_goals = Column(Integer, nullable=True)
    status = Column(String, default="SCHEDULED")  # SCHEDULED, FINISHED
    created_at = Column(DateTime, default=datetime.utcnow)


class Prediction(Base):
    __tablename__ = "predictions"

    id = Column(Integer, primary_key=True)
    match_id = Column(Integer, index=True)
    market = Column(String)  # h2h, totals, spreads
    outcome = Column(String)  # Home, Draw, Away, Over 2.5, Under 2.5, etc.
    model_probability = Column(Float)
    best_odds = Column(Float)
    best_bookmaker = Column(String)
    expected_value = Column(Float)
    confidence = Column(String)  # HIGH, MEDIUM, LOW
    is_value_bet = Column(Boolean, default=False)
    result = Column(String, nullable=True)  # WIN, LOSE, PUSH, None (pending)
    notified = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)


class Bookmaker(Base):
    __tablename__ = "bookmakers"

    id = Column(Integer, primary_key=True)
    key = Column(String, unique=True)
    name = Column(String)
    url = Column(String, nullable=True)
    is_default = Column(Boolean, default=False)
    added_at = Column(DateTime, default=datetime.utcnow)


class DailyReport(Base):
    __tablename__ = "daily_reports"

    id = Column(Integer, primary_key=True)
    date = Column(String, unique=True)
    total_picks = Column(Integer, default=0)
    correct = Column(Integer, default=0)
    wrong = Column(Integer, default=0)
    pending = Column(Integer, default=0)
    high_correct = Column(Integer, default=0)
    high_total = Column(Integer, default=0)
    medium_correct = Column(Integer, default=0)
    medium_total = Column(Integer, default=0)
    low_correct = Column(Integer, default=0)
    low_total = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)


# Engine & Session
engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(bind=engine)


def init_db():
    Base.metadata.create_all(engine)


def get_session():
    return SessionLocal()
