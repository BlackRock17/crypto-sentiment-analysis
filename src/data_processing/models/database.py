from datetime import datetime
from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, Text, Enum, Boolean, Index
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
    tweet_id = Column(String(50), unique=True, nullable=False)
    text = Column(Text, nullable=False)
    created_at = Column(DateTime, nullable=False)
    author_id = Column(String(50), nullable=False)
    author_username = Column(String(100))
    retweet_count = Column(Integer, default=0)
    like_count = Column(Integer, default=0)
    collected_at = Column(DateTime, default=datetime.utcnow)

    # Relation to analysis
    sentiment_analysis = relationship("SentimentAnalysis", back_populates="tweet", uselist=False)


class SentimentAnalysis(Base):
    __tablename__ = "sentiment_analysis"

    id = Column(Integer, primary_key=True)
    tweet_id = Column(Integer, ForeignKey("tweets.id"), unique=True, nullable=False)
    sentiment = Column(Enum(SentimentEnum), nullable=False)
    confidence_score = Column(Float, nullable=False)
    analyzed_at = Column(DateTime, default=datetime.utcnow)

    # Tweet Relation
    tweet = relationship("Tweet", back_populates="sentiment_analysis")


class BlockchainToken(Base):
    __tablename__ = "blockchain_tokens"

    id = Column(Integer, primary_key=True)
    token_address = Column(String(44), nullable=False)  # May vary in length for different blockchains
    symbol = Column(String(20), nullable=False)
    name = Column(String(100))
    blockchain_network = Column(String(50), nullable=True)  # Network name or NULL if unknown
    network_confidence = Column(Float, default=0.0)  # Confidence in network definition (0-1)
    manually_verified = Column(Boolean, default=False)  # Flag whether it is manually verified
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    needs_review = Column(Boolean, default=False)  # Flag if it needs review

    # Indices
    __table_args__ = (
        # Unique index by address and network, allowing NULL in blockchain_network
        Index('ix_blockchain_tokens_address_network', 'token_address', 'blockchain_network', unique=True),
        # Index for faster searching by symbol and network
        Index('ix_blockchain_tokens_symbol_network', 'symbol', 'blockchain_network')
    )

    # Relations
    mentions = relationship("TokenMention", back_populates="token", cascade="all, delete-orphan")


class TokenMention(Base):
    __tablename__ = "token_mentions"

    id = Column(Integer, primary_key=True)
    tweet_id = Column(Integer, ForeignKey("tweets.id"), nullable=False)
    token_id = Column(Integer, ForeignKey("solana_tokens.id"), nullable=False)
    mentioned_at = Column(DateTime, default=datetime.utcnow)

    # Relation
    token = relationship("BlockchainToken", back_populates="mentions")
