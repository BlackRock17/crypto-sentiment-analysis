from datetime import datetime
from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, Text, Enum, Boolean, Index, ARRAY
from sqlalchemy.orm import declarative_base, relationship
import enum

Base = declarative_base()

class SentimentEnum(enum.Enum):
    POSITIVE = "positive"
    NEGATIVE = "negative"
    NEUTRAL = "neutral"


class Tweet(Base):
    __tablename__ = "tweets"

    id = Column(Integer, primary_key=True)
    tweet_id = Column(String(50), unique=True, nullable=False, index=True)
    text = Column(Text, nullable=False)
    created_at = Column(DateTime, nullable=False)
    collected_at = Column(DateTime, default=datetime.utcnow)


class MarketSentiment(Base):
    __tablename__ = "market_sentiment"

    id = Column(Integer, primary_key=True)
    tweet_id = Column(String(50), nullable=False, index=True)
    sentiment = Column(Enum(SentimentEnum), nullable=False)
    confidence_score = Column(Float, nullable=False)
    analyzed_at = Column(DateTime, default=datetime.utcnow)


class Token(Base):
    __tablename__ = "tokens"

    id = Column(Integer, primary_key=True)
    symbol = Column(String(20), nullable=False, index=True, unique=True)
    name = Column(String(100), nullable=True)
    blockchain_network = Column(String(50), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    sentiments = relationship("TokenSentiment", back_populates="token")


class TokenSentiment(Base):
    __tablename__ = "token_sentiment"

    id = Column(Integer, primary_key=True)
    token_id = Column(Integer, ForeignKey("tokens.id"), nullable=False, index=True)
    tweet_id = Column(String(50), nullable=False, index=True)
    sentiment = Column(Enum(SentimentEnum), nullable=False)
    confidence_score = Column(Float, nullable=False)
    analyzed_at = Column(DateTime, default=datetime.utcnow)

    token = relationship("Token", back_populates="sentiments")


class Network(Base):
    __tablename__ = "networks"

    id = Column(Integer, primary_key=True)
    name = Column(String(50), nullable=False, unique=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    sentiments = relationship("NetworkSentiment", back_populates="network")


class NetworkSentiment(Base):
    __tablename__ = "network_sentiment"

    id = Column(Integer, primary_key=True)
    network_id = Column(Integer, ForeignKey("networks.id"), nullable=False, index=True)
    tweet_id = Column(String(50), nullable=False, index=True)
    sentiment = Column(Enum(SentimentEnum), nullable=False)
    confidence_score = Column(Float, nullable=False)
    analyzed_at = Column(DateTime, default=datetime.utcnow)

    network = relationship("Network", back_populates="sentiments")


class Influencer(Base):
    __tablename__ = "influencers"

    id = Column(Integer, primary_key=True)
    username = Column(String(50), nullable=False, unique=True, index=True)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)