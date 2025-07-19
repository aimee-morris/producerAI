"""
SQLAlchemy models for Producer AI.

Run:
    python models/models.py
to initialize the database defined in environment variable PRODUCER_DB_URL
or fallback to 'sqlite:///producer_ai.db'.
"""

from __future__ import annotations
import os
from datetime import datetime
from typing import Optional, Dict, Any, List

from sqlalchemy import (
    create_engine, Column, Integer, String, Date, DateTime, Float, ForeignKey,
    JSON, UniqueConstraint, Index, Text
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker, Session as SASession

# -------------------------------------------------------------------
# Base & Mixins
# -------------------------------------------------------------------

Base = declarative_base()

class TimestampMixin:
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

# -------------------------------------------------------------------
# Core Domain Tables
# -------------------------------------------------------------------

class Speaker(TimestampMixin, Base):
    __tablename__ = 'speakers'

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False, index=True)
    primary_domain = Column(String, index=True)         # e.g., 'science', 'politics', 'wellness'
    bio_snippet = Column(Text)
    similarity_core = Column(Float)                     # similarity to roster centroid
    diversity_score = Column(Float)                     # inverse cluster density
    status = Column(String, default='candidate', index=True)  # candidate | watch | pitched | booked | drop
    strategic_themes = Column(String)                   # comma-separated or use a join table later

    # Relationships
    books = relationship("Book", back_populates="speaker", cascade="all, delete-orphan")
    social_metrics = relationship("SocialMetric", back_populates="speaker", cascade="all, delete-orphan")
    press_metrics = relationship("PressMetric", back_populates="speaker", cascade="all, delete-orphan")
    trend_metrics = relationship("TrendMetric", back_populates="speaker", cascade="all, delete-orphan")
    scores = relationship("Score", back_populates="speaker", cascade="all, delete-orphan")
    idea_cards = relationship("IdeaCard", back_populates="speaker", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<Speaker id={self.id} name={self.name!r} status={self.status}>"

class Book(TimestampMixin, Base):
    __tablename__ = 'books'

    id = Column(Integer, primary_key=True)
    speaker_id = Column(Integer, ForeignKey('speakers.id'), nullable=False, index=True)
    title = Column(String, nullable=False, index=True)
    publisher = Column(String)
    release_date = Column(Date, index=True)
    announce_date = Column(Date)
    isbn = Column(String, index=True)
    notes = Column(Text)

    speaker = relationship("Speaker", back_populates="books")

    __table_args__ = (
        UniqueConstraint('speaker_id', 'title', name='uq_book_speaker_title'),
    )

class SocialMetric(TimestampMixin, Base):
    __tablename__ = 'social_metrics'

    id = Column(Integer, primary_key=True)
    speaker_id = Column(Integer, ForeignKey('speakers.id'), nullable=False, index=True)
    platform = Column(String, nullable=False, index=True)  # twitter, instagram, youtube, etc.
    followers = Column(Integer)
    collected_at = Column(DateTime, default=datetime.utcnow, index=True)
    engagement_rate = Column(Float)  # optional future metric

    speaker = relationship("Speaker", back_populates="social_metrics")

    __table_args__ = (
        Index('ix_social_speaker_platform_collected', 'speaker_id', 'platform', 'collected_at'),
    )

class PressMetric(TimestampMixin, Base):
    __tablename__ = 'press_metrics'

    id = Column(Integer, primary_key=True)
    speaker_id = Column(Integer, ForeignKey('speakers.id'), nullable=False, index=True)
    period_start = Column(Date, nullable=False)
    period_end = Column(Date, nullable=False)
    article_count = Column(Integer, default=0)
    sentiment_avg = Column(Float)            # -1..1
    momentum_index = Column(Float)           # article_count_this_period / prev_period (capped)
    source_span = Column(Integer)            # distinct sources
    fetched_at = Column(DateTime, default=datetime.utcnow, index=True)

    speaker = relationship("Speaker", back_populates="press_metrics")

    __table_args__ = (
        UniqueConstraint('speaker_id', 'period_start', 'period_end', name='uq_press_period'),
        Index('ix_press_speaker_period', 'speaker_id', 'period_start', 'period_end'),
    )

class TrendMetric(TimestampMixin, Base):
    __tablename__ = 'trend_metrics'

    id = Column(Integer, primary_key=True)
    speaker_id = Column(Integer, ForeignKey('speakers.id'), index=True)
    keyword = Column(String, index=True, nullable=False)
    country = Column(String, default='GB', index=True)
    interest_last7 = Column(Float)
    interest_prev7 = Column(Float)
    trend_ratio = Column(Float)
    interest_90pctl = Column(Float)
    momentum_score = Column(Float)   # 0..1 composite
    fetched_at = Column(DateTime, default=datetime.utcnow, index=True)

    speaker = relationship("Speaker", back_populates="trend_metrics")

    __table_args__ = (
        Index('ix_trend_keyword_time', 'keyword', 'fetched_at'),
    )

class CompetitorEvent(TimestampMixin, Base):
    __tablename__ = 'competitor_events'

    id = Column(Integer, primary_key=True)
    competitor_name = Column(String, index=True)
    title = Column(String, index=True)
    speaker_name = Column(String, index=True)   # may not match internal speaker yet
    date = Column(Date, index=True)
    url = Column(String)
    first_seen = Column(DateTime, default=datetime.utcnow)
    last_seen = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index('ix_competitor_date', 'competitor_name', 'date'),
    )

class Score(TimestampMixin, Base):
    __tablename__ = 'scores'

    id = Column(Integer, primary_key=True)
    speaker_id = Column(Integer, ForeignKey('speakers.id'), nullable=False, index=True)
    run_timestamp = Column(DateTime, default=datetime.utcnow, index=True)
    composite_score = Column(Float, index=True)
    rank = Column(Integer)
    detail_json = Column(JSON)  # {dimension: value}

    speaker = relationship("Speaker", back_populates="scores")

    __table_args__ = (
        Index('ix_score_speaker_time', 'speaker_id', 'run_timestamp'),
    )

class IdeaCard(TimestampMixin, Base):
    __tablename__ = 'idea_cards'

    id = Column(Integer, primary_key=True)
    speaker_id = Column(Integer, ForeignKey('speakers.id'), nullable=False, index=True)
    generated_at = Column(DateTime, default=datetime.utcnow, index=True)
    prompt_version = Column(String)
    content_md = Column(Text)

    speaker = relationship("Speaker", back_populates="idea_cards")

    __table_args__ = (
        Index('ix_idea_speaker_time', 'speaker_id', 'generated_at'),
    )

# -------------------------------------------------------------------
# Database Initialization & Session
# -------------------------------------------------------------------

def get_database_url() -> str:
    return os.getenv('PRODUCER_DB_URL', 'sqlite:///producer_ai.db')

_engine = None
_SessionFactory = None

def init_db(db_url: Optional[str] = None):
    """Initialize engine & create tables. Returns Session factory."""
    global _engine, _SessionFactory
    if db_url is None:
        db_url = get_database_url()
    if _engine is None:
        _engine = create_engine(db_url, echo=False, future=True)
        Base.metadata.create_all(_engine)
        _SessionFactory = sessionmaker(bind=_engine)
    return _SessionFactory

def get_session() -> SASession:
    if _SessionFactory is None:
        init_db()
    return _SessionFactory()

# -------------------------------------------------------------------
# Utility / Helper Functions
# -------------------------------------------------------------------

def upsert_speaker(session: SASession, name: str, **kwargs) -> Speaker:
    """Create or update a speaker by name."""
    sp = session.query(Speaker).filter(Speaker.name == name).first()
    if sp:
        for k, v in kwargs.items():
            setattr(sp, k, v)
    else:
        sp = Speaker(name=name, **kwargs)
        session.add(sp)
    return sp

def latest_score(session: SASession, speaker_id: int) -> Optional[Score]:
    return session.query(Score).filter(Score.speaker_id == speaker_id)        .order_by(Score.run_timestamp.desc()).first()

def latest_trend(session: SASession, speaker_id: int) -> Optional[TrendMetric]:
    return session.query(TrendMetric).filter(TrendMetric.speaker_id == speaker_id)        .order_by(TrendMetric.fetched_at.desc()).first()

def latest_press_period(session: SASession, speaker_id: int) -> Optional[PressMetric]:
    return session.query(PressMetric).filter(PressMetric.speaker_id == speaker_id)        .order_by(PressMetric.period_end.desc()).first()

def aggregate_followers(session: SASession, speaker_id: int) -> int:
    """Return the max latest follower counts per platform summed."""
    platforms = {}
    for sm in session.query(SocialMetric).filter(SocialMetric.speaker_id == speaker_id).all():
        key = (sm.platform,)
        existing = platforms.get(key)
        if existing is None or sm.collected_at > existing.collected_at:
            platforms[key] = sm
    return sum(sm.followers or 0 for sm in platforms.values())

# -------------------------------------------------------------------
# CLI Execution
# -------------------------------------------------------------------

if __name__ == '__main__':
    SessionFactory = init_db()
    session = SessionFactory()
    # Example: seed a speaker
    demo = upsert_speaker(session, "Sample Speaker", primary_domain="science", bio_snippet="Example bio.")
    session.commit()
    print("Database initialized & sample speaker ensured.")
