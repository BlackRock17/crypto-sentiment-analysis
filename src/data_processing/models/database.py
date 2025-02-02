from datetime import datetime
from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, Text, Enum
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