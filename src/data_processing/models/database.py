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
    blockchain_network_id = Column(Integer, ForeignKey("blockchain_networks.id"),
                                   nullable=True)  # Network ID or NULL if unknown
    network_confidence = Column(Float, default=0.0)  # Confidence in network determination (0-1)
    manually_verified = Column(Boolean, default=False)  # Whether manually verified
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    needs_review = Column(Boolean, default=False)  # Whether needs review

    # References the blockchain_network field for backward compatibility
    blockchain_network = Column(String(50), nullable=True)

    # Indexes
    __table_args__ = (
        # Unique index by address and network, allowing NULL in blockchain_network_id
        Index('ix_blockchain_tokens_address_network', 'token_address', 'blockchain_network_id', unique=True),
        # Index for faster lookup by symbol and network
        Index('ix_blockchain_tokens_symbol_network', 'symbol', 'blockchain_network_id')
    )

    # Relationships
    mentions = relationship("TokenMention", back_populates="token", cascade="all, delete-orphan")
    network = relationship("BlockchainNetwork", back_populates="tokens")


class TokenMention(Base):
    __tablename__ = "token_mentions"

    id = Column(Integer, primary_key=True)
    tweet_id = Column(Integer, ForeignKey("tweets.id"), nullable=False)
    token_id = Column(Integer, ForeignKey("solana_tokens.id"), nullable=False)
    mentioned_at = Column(DateTime, default=datetime.utcnow)

    # Relation
    token = relationship("BlockchainToken", back_populates="mentions")


class BlockchainNetwork(Base):
    __tablename__ = "blockchain_networks"

    id = Column(Integer, primary_key=True)
    name = Column(String(50), unique=True, nullable=False)  # Internal identifier (lowercase)
    display_name = Column(String(100))  # User-friendly name for display
    description = Column(Text)
    icon_url = Column(String(255))  # URL to network icon
    is_active = Column(Boolean, default=True)  # Whether the network is active
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Lists of related terms for network identification
    hashtags = Column(ARRAY(String))  # Associated hashtags
    keywords = Column(ARRAY(String))  # Associated keywords

    # Network metadata
    launch_date = Column(DateTime, nullable=True)  # When the network was launched
    website_url = Column(String(255), nullable=True)  # Official website
    explorer_url = Column(String(255), nullable=True)  # Block explorer URL

    # Relationships
    tokens = relationship("BlockchainToken", back_populates="network")

    def __repr__(self):
        return f"<BlockchainNetwork(name='{self.name}', display_name='{self.display_name}')>"
