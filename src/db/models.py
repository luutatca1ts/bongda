"""Database models for football analytics."""

from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime, Boolean, Index
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
    # Phase B2.1: canonical (API-Football) name + id per team for each side.
    # Populated by migrate_team_mapping.py from artifacts/team_mapping.json.
    # Nullable — a row may not have a mapping entry yet, and the pipeline
    # must gracefully fall back to raw team name in that case.
    home_canonical = Column(String, nullable=True)
    away_canonical = Column(String, nullable=True)
    home_api_id = Column(Integer, nullable=True)
    away_api_id = Column(Integer, nullable=True)
    home_league_id = Column(Integer, nullable=True)
    away_league_id = Column(Integer, nullable=True)


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
    # CLV — closing odds captured ~trước kickoff để đo chất lượng bet dài hạn
    closing_odds = Column(Float, nullable=True)
    closing_captured_at = Column(DateTime, nullable=True)
    clv = Column(Float, nullable=True)  # %
    # Injury + weather adjustments factored into this prediction's λ
    injury_impact_home = Column(Float, default=0.0)
    injury_impact_away = Column(Float, default=0.0)
    weather_adjust = Column(Float, default=0.0)
    weather_description = Column(String, nullable=True)
    # xG estimates surfaced to the user (λ after all adjustments, nullable
    # because PoissonModel path doesn't always populate).
    home_xg_estimate = Column(Float, nullable=True)
    away_xg_estimate = Column(Float, nullable=True)
    # Special-match context (derby, cup final, knockout, 6-pointer) — JSON text.
    # Written when USE_MATCH_CONTEXT != "off". Presence is independent of
    # whether the model actually used it (log_only mode saves without adjust).
    match_context = Column(String, nullable=True)


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


class LiveMatchState(Base):
    """Snapshot state real-time của 1 trận đang live (score, minute, xG, red cards)."""
    __tablename__ = "live_match_states"

    id = Column(Integer, primary_key=True)
    match_id = Column(Integer, index=True)
    fixture_id = Column(Integer, nullable=True)  # API-Football fixture_id
    minute = Column(Integer)
    home_score = Column(Integer, default=0)
    away_score = Column(Integer, default=0)
    home_red_cards = Column(Integer, default=0)
    away_red_cards = Column(Integer, default=0)
    home_xg = Column(Float, default=0.0)
    away_xg = Column(Float, default=0.0)
    home_shots_on_target = Column(Integer, default=0)
    away_shots_on_target = Column(Integer, default=0)
    captured_at = Column(DateTime, default=datetime.utcnow, index=True)

    __table_args__ = (
        Index("ix_live_state_match_time", "match_id", "captured_at"),
    )


class LivePrediction(Base):
    """Value bet detected từ LivePoissonModel trong thời gian trận đang live."""
    __tablename__ = "live_predictions"

    id = Column(Integer, primary_key=True)
    match_id = Column(Integer, index=True)
    minute = Column(Integer)
    market = Column(String)  # h2h, totals, next_goal
    outcome = Column(String)
    model_probability = Column(Float)
    live_odds = Column(Float)
    best_bookmaker = Column(String)
    expected_value = Column(Float)
    confidence = Column(String)
    is_value_bet = Column(Boolean, default=False)
    alerted = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)


class OddsHistory(Base):
    """Snapshot từng lần odds thay đổi — dùng cho Line Movement / Steam / CLV."""
    __tablename__ = "odds_history"

    id = Column(Integer, primary_key=True)
    match_id = Column(Integer, index=True)
    bookmaker_key = Column(String, index=True)
    bookmaker_name = Column(String)
    market = Column(String, index=True)  # h2h, totals, spreads, corners_totals, ...
    outcome = Column(String)
    point = Column(Float, nullable=True)
    odds = Column(Float)
    captured_at = Column(DateTime, index=True, default=datetime.utcnow)

    __table_args__ = (
        Index("ix_odds_history_match_market_outcome_time",
              "match_id", "market", "outcome", "captured_at"),
        Index("ix_odds_history_bookmaker_time", "bookmaker_key", "captured_at"),
    )


# Engine & Session
engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(bind=engine)


def init_db():
    Base.metadata.create_all(engine)


def get_session():
    return SessionLocal()
